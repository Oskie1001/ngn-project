"""
VoIP monitoring and call-control agent.

This service combines four responsibilities:

1. FastAPI control interface
   - POST /call starts an outgoing SIP call.
   - POST /hangup terminates all current calls.
   - GET /status returns the agent's current internal state.
   - GET / returns basic service information.

2. Persistent PJSUA process management
   - Starts one long-running pjsua process.
   - Sends interactive commands to pjsua through stdin.
   - Reads pjsua output continuously from stdout.
   - Detects SIP call-state changes from pjsua log messages.

3. RTP quality-statistics collection
   - Periodically sends the pjsua "dq" command.
   - Parses RX/TX packet loss, RX/TX jitter, and average RTT.
   - Keeps the most recently measured values after a call ends.

4. Prometheus metric export
   - Runs a Prometheus HTTP exporter on TCP port 9200.
   - Publishes call state and RTP quality measurements.
   - Uses the "agent" label so multiple VMs can be distinguished.

Concurrency model
-----------------
The FastAPI request handlers, pjsua output-monitor thread, periodic statistics
thread, Prometheus exporter, and watchdog can access shared state concurrently.
Locks and a condition variable are therefore used to prevent command overlap,
protect shared state, and coordinate collection of pjsua command output.
"""

from fastapi import FastAPI
from prometheus_client import start_http_server, Gauge
import subprocess
import threading
import time
import os
import re


app = FastAPI()


# ============================================================
# CONFIG
# ============================================================

AGENT_NAME = os.getenv("AGENT_NAME", "vm1")

CONFIG_FILE = os.getenv(
    "CONFIG_FILE",
    "/home/admin/agent/6001.conf"
)

ASTERISK_IP = os.getenv(
    "ASTERISK_IP",
    "192.168.40.10"
)

METRIC_EXPORT_INTERVAL_SECONDS = int(
    os.getenv(
        "METRIC_EXPORT_INTERVAL_SECONDS",
        "2"
    )
)

PJSUA_COMMAND_TIMEOUT_SECONDS = float(
    os.getenv(
        "PJSUA_COMMAND_TIMEOUT_SECONDS",
        "30"
    )
)

WATCHDOG_TIMEOUT_SECONDS = int(
    os.getenv(
        "WATCHDOG_TIMEOUT_SECONDS",
        "120"
    )
)


# ============================================================
# PROMETHEUS METRICS
# ============================================================

call_active_metric = Gauge(
    "voip_agent_call_active",
    "Whether a call is active",
    ["agent"]
)

call_state_metric = Gauge(
    "voip_agent_call_state",
    "Current call state as a one-hot value",
    ["agent", "state"]
)

jitter_metric = Gauge(
    "voip_agent_rtp_jitter_ms",
    "Measured RTP jitter",
    ["agent"]
)

loss_metric = Gauge(
    "voip_agent_rtp_loss_percent",
    "Measured RTP packet loss",
    ["agent"]
)

rx_loss_metric = Gauge(
    "voip_rx_packet_loss",
    "RTP RX packet loss",
    ["agent"]
)

tx_loss_metric = Gauge(
    "voip_tx_packet_loss",
    "RTP TX packet loss",
    ["agent"]
)

rx_jitter_metric = Gauge(
    "voip_rx_jitter_ms",
    "RTP RX jitter ms",
    ["agent"]
)

tx_jitter_metric = Gauge(
    "voip_tx_jitter_ms",
    "RTP TX jitter ms",
    ["agent"]
)

rtt_metric = Gauge(
    "voip_agent_rtt_ms",
    "Measured RTP RTT",
    ["agent"]
)


# ============================================================
# GLOBAL STATE
# ============================================================

agent_state = {
    "call_active": 0,
    "call_state": "IDLE",
    "jitter_ms": 0.0,
    "packet_loss_percent": 0.0,
    "rx_jitter_ms": 0.0,
    "tx_jitter_ms": 0.0,
    "rx_packet_loss_percent": 0.0,
    "tx_packet_loss_percent": 0.0,
    "rtt_ms": 0.0,
    "target": ""
}

CALL_STATES = [
    "IDLE",
    "CALLING",
    "RINGING",
    "CONFIRMED"
]

DQ_ALLOWED_STATES = [
    "CALLING",
    "RINGING",
    "CONFIRMED"
]

# Re-entrant lock protecting all reads and writes to ``agent_state``.
# RLock is used because a function that already owns the lock may call another
# helper that also needs the same lock without deadlocking itself.
lock = threading.RLock()

# Ensures that only one thread sends an interactive pjsua command at a time.
# This is especially important because placing a call requires two related
# writes: first "m", then the SIP URI.
pjsua_command_lock = threading.Lock()

# Coordinates the output-monitor thread with a thread waiting for a complete
# command response. The monitor appends characters and notifies the waiter.
pjsua_output_condition = threading.Condition()

# Temporary text buffer used only while a command is waiting for the next
# pjsua prompt. It is cleared before and after each synchronous command.
pjsua_output_buffer = ""

# Tells the monitor thread whether output should currently be copied into the
# synchronous command-response buffer.
pjsua_waiting_for_prompt = False

# Holds the subprocess.Popen object after pjsua has started.
persistent_pjsua = None

# Unix timestamp of the latest confirmed call event or successful RTP-stat
# update. The watchdog uses it to detect stale active-call state.
last_confirmed_time = 0


# ============================================================
# METRIC UPDATE
# ============================================================

def update_metrics():

    """
    Copy the current values from ``agent_state`` into Prometheus gauges.

    The shared state is read while holding ``lock`` so that all exported
    measurements represent one consistent snapshot. The call-state metric is
    represented as one-hot values: exactly one state label should normally be
    1, while the remaining state labels are 0.
    """

    with lock:

        current_call_state = agent_state["call_state"]

        call_active_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["call_active"])

        for call_state in CALL_STATES:

            call_state_metric.labels(
                agent=AGENT_NAME,
                state=call_state
            ).set(
                1 if call_state == current_call_state else 0
            )

        jitter_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["jitter_ms"])

        loss_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["packet_loss_percent"])

        rx_loss_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["rx_packet_loss_percent"])

        tx_loss_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["tx_packet_loss_percent"])

        rx_jitter_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["rx_jitter_ms"])

        tx_jitter_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["tx_jitter_ms"])

        rtt_metric.labels(
            agent=AGENT_NAME
        ).set(agent_state["rtt_ms"])


# ============================================================
# PARSE RTP STATS FROM PJSUA DQ OUTPUT
# ============================================================

def parse_rtp_stats(output):

    """
    Extract RTP statistics from the text returned by pjsua's ``dq`` command.

    Parameters
    ----------
    output:
        Complete pjsua diagnostic text captured before the next ``>>>`` prompt.

    Returns
    -------
    bool
        ``True`` when at least one supported metric was successfully parsed;
        otherwise ``False``.

    Parsing policy
    --------------
    - Overall packet loss follows RX loss because RX loss describes the media
      quality received by this endpoint.
    - RX and TX jitter use the average value from pjsua's
      ``min avg max last dev`` sequence.
    - RTT exports the average value rather than minimum, maximum, or last RTT.
    """

    updated = False

    print("\n===== RAW DQ OUTPUT FOR PARSER =====")
    print(output)
    print("====================================\n")

    rx_loss_match = re.search(
        r"RX[\s\S]*?pkt\s+loss\s*=\s*\d+\s*\(\s*([\d.]+)\s*%\s*\)",
        output,
        re.IGNORECASE
    )

    tx_loss_match = re.search(
        r"TX[\s\S]*?pkt\s+loss\s*=\s*\d+\s*\(\s*([\d.]+)\s*%\s*\)",
        output,
        re.IGNORECASE
    )

    # --------------------------------------------------------
    # RTT parser
    #
    # pjsua dq format:
    #
    # RTT msec : min avg max last dev
    #
    # Example:
    # RTT msec : 0.930 50.187 125.409 51.223 64.825
    #
    # This exports the AVG value as voip_agent_rtt_ms.
    # --------------------------------------------------------

    rtt_match = re.search(
        r"RTT\s+msec\s*:\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)",
        output,
        re.IGNORECASE
    )

    # --------------------------------------------------------
    # Jitter parser
    #
    # pjsua dq format:
    #
    # jitter : min avg max last dev
    #
    # This regex skips min and captures avg.
    # --------------------------------------------------------

    jitter_matches = re.findall(
        r"jitter\s*:\s*[\d.]+\s+([\d.]+)",
        output,
        re.IGNORECASE
    )

    with lock:

        if rx_loss_match:

            rx_loss = float(
                rx_loss_match.group(1)
            )

            print(f"PARSED RX LOSS: {rx_loss}%")

            agent_state["rx_packet_loss_percent"] = rx_loss
            agent_state["packet_loss_percent"] = rx_loss

            updated = True

        else:

            print("RX packet loss not matched")

        if tx_loss_match:

            tx_loss = float(
                tx_loss_match.group(1)
            )

            print(f"PARSED TX LOSS: {tx_loss}%")

            agent_state["tx_packet_loss_percent"] = tx_loss

            updated = True

        else:

            print("TX packet loss not matched")

        if rtt_match:

            rtt_min = float(
                rtt_match.group(1)
            )

            rtt_avg = float(
                rtt_match.group(2)
            )

            rtt_max = float(
                rtt_match.group(3)
            )

            rtt_last = float(
                rtt_match.group(4)
            )

            rtt_dev = float(
                rtt_match.group(5)
            )

            print(f"PARSED RTT MIN: {rtt_min} ms")
            print(f"PARSED RTT AVG: {rtt_avg} ms")
            print(f"PARSED RTT MAX: {rtt_max} ms")
            print(f"PARSED RTT LAST: {rtt_last} ms")
            print(f"PARSED RTT DEV: {rtt_dev} ms")

            # Export average RTT to Prometheus
            agent_state["rtt_ms"] = rtt_avg

            updated = True

        else:

            print("RTT not matched")

        if len(jitter_matches) >= 1:

            rx_jitter = float(
                jitter_matches[0]
            )

            print(f"PARSED RX JITTER: {rx_jitter} ms")

            agent_state["rx_jitter_ms"] = rx_jitter
            agent_state["jitter_ms"] = rx_jitter

            updated = True

        else:

            print("RX jitter not matched")

        if len(jitter_matches) >= 2:

            tx_jitter = float(
                jitter_matches[1]
            )

            print(f"PARSED TX JITTER: {tx_jitter} ms")

            agent_state["tx_jitter_ms"] = tx_jitter

            updated = True

        else:

            print("TX jitter not matched")

    if updated:

        update_metrics()

    return updated



# ============================================================
# PJSUA COMMAND
# ============================================================

def run_pjsua_command(command, wait_for_prompt=False):

    """
    Send one interactive command to the persistent pjsua process.

    When ``wait_for_prompt`` is false, the function only writes the command and
    returns immediately. When it is true, the monitor thread copies pjsua output
    into ``pjsua_output_buffer`` until the next ``>>>`` prompt appears.

    ``pjsua_command_lock`` serializes all commands so a background ``dq`` cannot
    be inserted between the ``m`` command and SIP URI used to place a call.
    """

    global pjsua_output_buffer
    global pjsua_waiting_for_prompt

    if persistent_pjsua is None:

        raise RuntimeError("PJSUA process is not running")

    if persistent_pjsua.stdin is None:

        raise RuntimeError("PJSUA stdin is not available")

    with pjsua_command_lock:

        if wait_for_prompt:

            with pjsua_output_condition:

                pjsua_output_buffer = ""
                pjsua_waiting_for_prompt = True

        persistent_pjsua.stdin.write(
            f"{command}\n"
        )

        persistent_pjsua.stdin.flush()

        if not wait_for_prompt:

            return ""

        deadline = time.time() + PJSUA_COMMAND_TIMEOUT_SECONDS

        with pjsua_output_condition:

            while ">>>" not in pjsua_output_buffer:

                remaining = deadline - time.time()

                if remaining <= 0:

                    pjsua_waiting_for_prompt = False

                    raise TimeoutError(
                        f"Timeout waiting for pjsua after '{command}'"
                    )

                pjsua_output_condition.wait(
                    remaining
                )

            output = pjsua_output_buffer.split(
                ">>>",
                1
            )[0]

            pjsua_output_buffer = ""
            pjsua_waiting_for_prompt = False

            return output


# ============================================================
# REQUEST RTP STATS
# ============================================================

def request_rtp_stats():

    """
    Request and process the current call's RTP diagnostic statistics.

    The function skips ``dq`` while the agent is idle, handles pjsua reporting
    that no current call exists, confirms an active call from diagnostic output,
    parses supported metrics, and refreshes the watchdog timestamp.
    """

    global last_confirmed_time

    with lock:

        current_active = agent_state["call_active"]
        current_state = agent_state["call_state"]

    print(
        f"request_rtp_stats() called: "
        f"call_active={current_active}, "
        f"call_state={current_state}"
    )

    if current_state not in DQ_ALLOWED_STATES:

        print("Skipping dq because no call attempt or active call")

        return

    try:

        output = run_pjsua_command(
            "dq",
            wait_for_prompt=True
        )

    except Exception as e:

        print(f"dq command failed: {e}")

        return

    if not output:

        print("No RTP stats captured from dq")

        return

    if "No current call" in output:

        print("PJSUA says no current call. Resetting call state.")

        reset_call_state()

        return

    print("\n===== DQ OUTPUT =====")
    print(output)
    print("=====================\n")

    if (
        "Call time:" in output
        and "RX" in output
        and "TX" in output
    ):

        with lock:

            agent_state["call_active"] = 1
            agent_state["call_state"] = "CONFIRMED"

        update_metrics()

        last_confirmed_time = time.time()

        print("CALL CONFIRMED BY DQ OUTPUT")

    if parse_rtp_stats(output):

        last_confirmed_time = time.time()

    else:

        print("No RTP stats matched in dq output")


# ============================================================
# PERIODIC METRIC EXPORT
# ============================================================

def periodic_metric_exporter():

    """
    Background loop that periodically refreshes RTP data and Prometheus gauges.

    Any exception is caught inside the loop so one temporary pjsua or parsing
    failure does not terminate the monitoring thread permanently.
    """

    while True:

        try:

            request_rtp_stats()

            update_metrics()

        except Exception as e:

            print(f"METRIC EXPORT ERROR: {e}")

        time.sleep(METRIC_EXPORT_INTERVAL_SECONDS)


# ============================================================
# RESET STATE
# ============================================================

def reset_call_state():

    """
    Return only the call-control fields to the idle state.

    RTP measurements are deliberately retained. This lets Grafana continue to
    display the quality of the most recently completed call instead of replacing
    all values with zero immediately after disconnection.
    """

    with lock:

        agent_state["call_state"] = "IDLE"
        agent_state["call_active"] = 0
        agent_state["target"] = ""

        # Do not reset RTP metrics here.
        # Keep last measured jitter/loss/RTT visible in Grafana.

    update_metrics()

    print("STATE RESET -> IDLE, RTP metrics kept")


# ============================================================
# START PROMETHEUS
# ============================================================

start_http_server(9200)

print("Prometheus exporter running on :9200")

update_metrics()


# ============================================================
# START PJSUA
# ============================================================

def start_pjsua():

    """
    Start the persistent pjsua subprocess and its output-monitor thread.

    stdin remains open for interactive commands. stderr is redirected to stdout
    so call-state events and errors can be processed through one output stream.
    Line buffering reduces delay while still allowing character-by-character
    prompt detection.
    """

    global persistent_pjsua

    cmd = [
        "/usr/local/bin/pjsua",
        "--config-file",
        CONFIG_FILE
    ]

    persistent_pjsua = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        universal_newlines=True,
        bufsize=1
    )

    print("Persistent PJSUA started")

    threading.Thread(
        target=monitor_pjsua,
        daemon=True
    ).start()


# ============================================================
# MONITOR PJSUA OUTPUT
# ============================================================

def monitor_pjsua():

    """
    Continuously read pjsua output and translate log events into agent state.

    Characters are read individually because the pjsua prompt may not end with
    a newline. The same characters are optionally copied into the command
    response buffer while another thread is waiting for a prompt.
    """

    global last_confirmed_time
    global pjsua_output_buffer
    global pjsua_waiting_for_prompt

    line_buffer = ""

    while True:

        char = persistent_pjsua.stdout.read(1)

        if not char:

            print("PJSUA output ended")

            break

        with pjsua_output_condition:

            if pjsua_waiting_for_prompt:

                pjsua_output_buffer += char

            pjsua_output_condition.notify_all()

        if char not in ("\n", "\r"):

            line_buffer += char

            continue

        line = line_buffer.strip()

        line_buffer = ""

        if not line:

            continue

        print(f"PJSUA: {line}")

        # ----------------------------------------------------
        # CALLING
        # ----------------------------------------------------

        if "CALLING" in line:

            with lock:

                agent_state["call_state"] = "CALLING"

        # ----------------------------------------------------
        # RINGING
        # ----------------------------------------------------

        elif "EARLY" in line:

            with lock:

                agent_state["call_state"] = "RINGING"

        # ----------------------------------------------------
        # INCOMING
        # ----------------------------------------------------

        elif "Incoming call" in line:

            with lock:

                agent_state["call_state"] = "RINGING"

        # ----------------------------------------------------
        # CONFIRMED / CONNECTED
        # ----------------------------------------------------

        elif (
            "CONFIRMED" in line
            or "state changed to CONFIRMED" in line
            or "Response msg 200/INVITE" in line
        ):

            with lock:

                agent_state["call_state"] = "CONFIRMED"
                agent_state["call_active"] = 1

            last_confirmed_time = time.time()

            print("CALL CONNECTED")

        # ----------------------------------------------------
        # DISCONNECTED
        # ----------------------------------------------------
        #
        # Do NOT check for "Call time:" here.
        # "Call time:" appears inside normal dq output.
        # ----------------------------------------------------

        elif "DISCONNECTED" in line:

            print("CALL DISCONNECTED")

            reset_call_state()

        update_metrics()


# ============================================================
# WATCHDOG
# ============================================================

def state_watchdog():

    """
    Detect a call that remains marked active without recent confirmation.

    If no successful ``dq`` parse or confirmed-state event refreshes
    ``last_confirmed_time`` before the configured timeout, the call state is
    forced back to IDLE. This prevents stale Grafana call-state data when a
    disconnect event is missed.
    """

    global last_confirmed_time

    while True:

        try:

            with lock:

                stale_call = (
                    agent_state["call_active"] == 1
                    and time.time() - last_confirmed_time > WATCHDOG_TIMEOUT_SECONDS
                )

            if stale_call:

                print("STALE CALL DETECTED")
                print("FORCING IDLE RESET")

                reset_call_state()

        except Exception as e:

            print(f"WATCHDOG ERROR: {e}")

        time.sleep(5)


# ============================================================
# API
# ============================================================

@app.get("/")
def root():

    """
    Return basic identity and runtime configuration for health checking.
    """

    return {
        "agent": AGENT_NAME,
        "status": "running",
        "metrics_port": 9200,
        "metric_export_interval_seconds": METRIC_EXPORT_INTERVAL_SECONDS,
        "watchdog_timeout_seconds": WATCHDOG_TIMEOUT_SECONDS
    }


@app.get("/status")
def status():

    """
    Return a thread-safe copy of the current internal state.
    """

    with lock:

        return dict(agent_state)


# ============================================================
# MAKE CALL
# ============================================================

@app.post("/call")
def make_call(target: str):

    """
    Start an outgoing call to a SIP extension through the Asterisk server.

    The endpoint rejects a new request when a call is already active or being
    placed. The pjsua ``m`` command and destination URI are written while one
    command lock is held, preventing the periodic ``dq`` thread from interrupting
    the two-step pjsua dialing interaction.
    """

    with lock:

        if agent_state["call_active"] == 1:

            return {
                "status": "busy",
                "message": "Agent already has an active call"
            }

        if agent_state["call_state"] == "CALLING":

            return {
                "status": "already calling"
            }

        agent_state["call_state"] = "CALLING"
        agent_state["target"] = target

    update_metrics()

    sip_uri = f"sip:{target}@{ASTERISK_IP}"

    print(f"START CALL -> {sip_uri}")

    try:

        # Send "m" and SIP URI atomically so dq cannot interrupt.

        with pjsua_command_lock:

            if persistent_pjsua is None:

                raise RuntimeError("PJSUA process is not running")

            if persistent_pjsua.stdin is None:

                raise RuntimeError("PJSUA stdin is not available")

            persistent_pjsua.stdin.write("m\n")
            persistent_pjsua.stdin.flush()

            time.sleep(0.5)

            persistent_pjsua.stdin.write(f"{sip_uri}\n")
            persistent_pjsua.stdin.flush()

        print("CALL COMMAND SENT")

        return {
            "status": "calling",
            "target": target
        }

    except Exception as e:

        reset_call_state()

        return {
            "status": "error",
            "message": str(e)
        }


# ============================================================
# HANGUP
# ============================================================

@app.post("/hangup")
def hangup():

    """
    Terminate all pjsua calls and immediately expose IDLE state.

    The ``ha`` command is used instead of ``h`` because it reliably hangs up all
    calls, which is safer when this endpoint is controlled automatically.
    """

    try:

        # Use "ha" instead of "h" to hang up all calls.
        # This is safer for automation.

        run_pjsua_command("ha")

        print("HANGUP ALL SENT")

        # Immediately reset state so Grafana sees idle state.
        # RTP quality metrics are kept.

        reset_call_state()

        return {
            "status": "terminated"
        }

    except Exception as e:

        return {
            "status": "error",
            "message": str(e)
        }


# ============================================================
# STARTUP
# ============================================================

start_pjsua()

threading.Thread(
    target=state_watchdog,
    daemon=True
).start()

threading.Thread(
    target=periodic_metric_exporter,
    daemon=True
).start()

print("VoIP agent ready")


# ============================================================
# RUN EXAMPLES
# ============================================================

# VM1:
# export AGENT_NAME=vm1
# export CONFIG_FILE=/home/admin/agent/6001.conf
# export METRIC_EXPORT_INTERVAL_SECONDS=5
# export WATCHDOG_TIMEOUT_SECONDS=120
# uvicorn agent:app --host 192.168.10.10 --port 5001

# VM2:
# export AGENT_NAME=vm2
# export CONFIG_FILE=/home/admin/agent/6002.conf
# export METRIC_EXPORT_INTERVAL_SECONDS=5
# export WATCHDOG_TIMEOUT_SECONDS=120
# uvicorn agent:app --host 192.168.20.10 --port 5001

# VM3:
# export AGENT_NAME=vm3
# export CONFIG_FILE=/home/admin/agent/6003.conf
# export METRIC_EXPORT_INTERVAL_SECONDS=5
# export WATCHDOG_TIMEOUT_SECONDS=120
# uvicorn agent:app --host 192.168.30.10 --port 5001