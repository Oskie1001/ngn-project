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

ASTERISK_IP = "192.168.40.10"

METRIC_EXPORT_INTERVAL_SECONDS = int(
    os.getenv(
        "METRIC_EXPORT_INTERVAL_SECONDS",
        "5"
    )
)

PJSUA_COMMAND_TIMEOUT_SECONDS = float(
    os.getenv(
        "PJSUA_COMMAND_TIMEOUT_SECONDS",
        "30"
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

lock = threading.RLock()

pjsua_command_lock = threading.Lock()

pjsua_output_condition = threading.Condition()

pjsua_output_buffer = ""

pjsua_waiting_for_prompt = False

pjsua_recent_lines = []

persistent_pjsua = None

last_confirmed_time = 0

# ============================================================
# METRIC UPDATE
# ============================================================

def update_metrics():

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
# PARSE RTP STATS
# ============================================================

def parse_rtp_stats(output):

    updated = False

    rx_matches = re.findall(
        r"RX.*?pkt loss=\d+\s+\(([\d\.]+)%\)",
        output,
        re.DOTALL
    )

    tx_matches = re.findall(
        r"TX.*?pkt loss=\d+\s+\(([\d\.]+)%\)",
        output,
        re.DOTALL
    )

    rtt_matches = re.findall(
        r"RTT msec\s*:\s*([\d\.]+)",
        output
    )

    jitter_matches = re.findall(
        r"jitter\s*:\s*[\d\.]+\s+([\d\.]+)",
        output
    )

    with lock:

        if rx_matches:

            rx_loss = float(
                rx_matches[-1]
            )

            agent_state["rx_packet_loss_percent"] = rx_loss
            agent_state["packet_loss_percent"] = rx_loss
            updated = True

        if tx_matches:

            agent_state["tx_packet_loss_percent"] = float(
                tx_matches[-1]
            )

            updated = True

        if rtt_matches:

            agent_state["rtt_ms"] = float(
                rtt_matches[-1]
            )

            updated = True

        if len(jitter_matches) >= 2:

            rx_jitter = float(
                jitter_matches[-2]
            )

            agent_state["rx_jitter_ms"] = rx_jitter
            agent_state["jitter_ms"] = rx_jitter
            updated = True

            agent_state["tx_jitter_ms"] = float(
                jitter_matches[-1]
            )

            updated = True

        elif len(jitter_matches) == 1:

            rx_jitter = float(
                jitter_matches[-1]
            )

            agent_state["rx_jitter_ms"] = rx_jitter
            agent_state["jitter_ms"] = rx_jitter
            updated = True

    if updated:

        update_metrics()

    return updated

# ============================================================
# TRACK PJSUA RTP SUMMARY
# ============================================================

def track_pjsua_output_line(line):

    global last_confirmed_time

    pjsua_recent_lines.append(line)

    if len(pjsua_recent_lines) > 30:

        del pjsua_recent_lines[0]

    if "RTT msec" not in line:

        return

    output = "\n".join(pjsua_recent_lines)

    if parse_rtp_stats(output):

        last_confirmed_time = time.time()

        print("RTP stats updated from PJSUA output")

# ============================================================
# PJSUA COMMAND
# ============================================================

def run_pjsua_command(command, wait_for_prompt=False):

    global pjsua_output_buffer
    global pjsua_waiting_for_prompt

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

    global last_confirmed_time

    with lock:

        if agent_state["call_active"] != 1:

            return

    output = run_pjsua_command(
        "dq",
        wait_for_prompt=True
    )

    if not output:

        print("No RTP stats captured from dq")

        return

    print("\n===== DQ OUTPUT =====")
    print(output)
    print("=====================\n")

    if parse_rtp_stats(output):

        last_confirmed_time = time.time()

    else:

        print("No RTP stats matched in dq output")

# ============================================================
# PERIODIC METRIC EXPORT
# ============================================================

def periodic_metric_exporter():

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

    global agent_state

    with lock:

        agent_state["call_state"] = "IDLE"

        agent_state["call_active"] = 0

        agent_state["target"] = ""

    update_metrics()

    print("STATE RESET -> IDLE")

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

        track_pjsua_output_line(line)

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
        # CONFIRMED
        # ----------------------------------------------------

        elif "CONFIRMED" in line:

            with lock:

                agent_state["call_state"] = "CONFIRMED"

                # IMPORTANT:
                # NEVER increment
                # ALWAYS binary

                agent_state["call_active"] = 1

            last_confirmed_time = time.time()

            print("CALL CONNECTED")

        # ----------------------------------------------------
        # DISCONNECTED
        # ----------------------------------------------------

        elif (
            "DISCONNECTED" in line
            or
            "Call time:" in line
            or
            "deinitializing media" in line
        ):

            print("CALL DISCONNECTED")

            reset_call_state()

        update_metrics()

# ============================================================
# WATCHDOG
# ============================================================

def state_watchdog():

    global last_confirmed_time

    while True:

        try:

            with lock:

                if (
                    agent_state["call_active"] == 1
                    and
                    time.time() - last_confirmed_time > 20
                ):

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

    return {
        "agent": AGENT_NAME,
        "status": "running"
    }

# ============================================================

@app.get("/status")
def status():

    with lock:
        return dict(agent_state)

# ============================================================
# MAKE CALL
# ============================================================

@app.post("/call")
def make_call(target: str):

    global persistent_pjsua

    with lock:

        if agent_state["call_active"] == 1:

            return {
                "status": "busy"
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

        run_pjsua_command("m")

        time.sleep(0.5)

        run_pjsua_command(sip_uri)

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

    global persistent_pjsua

    try:

        run_pjsua_command("h")

        print("HANGUP SENT")

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
# RUN
# ============================================================

# VM1:
# uvicorn agent:app --host 192.168.10.10 --port 5001

# VM2:
# uvicorn agent:app --host 192.168.20.10 --port 5001
