"""
Independent multi-agent VoIP call controller.

This script coordinates multiple VoIP agent services. Each agent runs in its
own worker thread and independently performs the following cycle:

1. Wait for a randomized initial start delay.
2. Select a randomized call duration.
3. Select a randomized idle duration.
4. Send an HTTP request to the agent's /call endpoint.
5. Keep the call active for the selected duration.
6. Send an HTTP request to the agent's /hangup endpoint.
7. Remain idle for the selected duration.
8. Repeat the cycle indefinitely.

The controller also exports Prometheus metrics on a configurable TCP port.
These metrics record command counts, failures, selected durations, timestamps,
controller-side call state, and distributions of call and idle durations.

Concurrency model
-----------------
One daemon thread is created for every configured agent. Because each agent has
its own worker thread, call start times, call durations, and idle durations are
independent rather than synchronized across all agents.

The main thread stays alive until the user presses Ctrl+C. During shutdown, the
controller sends one final hangup command to every configured agent.
"""

import time
import os
import random
import requests
import threading
from prometheus_client import Counter, Gauge, Histogram, start_http_server


# ============================================================
# CONFIG
# ============================================================

CONTROLLER_METRICS_PORT = int(
    os.getenv(
        "CONTROLLER_METRICS_PORT",
        "9300"
    )
)

# Discrete call lengths used by every worker. random.choice() selects one
# value for each new cycle, so durations are bounded and predictable.
CALL_DURATION_CHOICES_SECONDS = [
    90,
    100,
    110,
    120
]

# Discrete waiting periods after a call ends or fails. Each worker chooses
# independently, which helps prevent the agents from becoming synchronized.
IDLE_DURATION_CHOICES_SECONDS = [
    15,
    20,
    25,
    30
]

# Random startup offsets stagger the first call from each agent. Without this
# delay, all worker threads would normally start their first call almost at once.
INITIAL_START_DELAY_CHOICES_SECONDS = [
    0,
    5,
    10,
    15,
    20,
    25,
    30
]

HTTP_TIMEOUT_SECONDS = int(
    os.getenv(
        "HTTP_TIMEOUT_SECONDS",
        "10"
    )
)


# ============================================================
# AGENTS
# ============================================================

# Static agent inventory. Each dictionary defines:
# - name: Prometheus label and log identifier
# - api: base URL of the FastAPI agent
# - target: SIP extension dialed by that agent
agents = [
    {
        "name": "vm1",
        "api": "http://192.168.10.10:5001",
        "target": "7001"
    },
    {
        "name": "vm2",
        "api": "http://192.168.20.10:5001",
        "target": "7002"
    },
    {
        "name": "vm3",
        "api": "http://192.168.30.10:5001",
        "target": "7003"
    }
]


# ============================================================
# PROMETHEUS METRICS
# ============================================================

controller_call_command_total = Counter(
    "voip_controller_call_command_total",
    "Total call commands sent by the controller",
    ["agent", "target"]
)

controller_hangup_command_total = Counter(
    "voip_controller_hangup_command_total",
    "Total hangup commands sent by the controller",
    ["agent"]
)

controller_command_failed_total = Counter(
    "voip_controller_command_failed_total",
    "Total failed controller commands",
    ["agent", "command", "reason"]
)

controller_call_cycle_total = Counter(
    "voip_controller_call_cycle_total",
    "Total completed call cycles per agent",
    ["agent", "target"]
)

controller_agent_call_active = Gauge(
    "voip_controller_agent_call_active",
    "Whether the controller believes this agent currently has an active call",
    ["agent", "target"]
)

controller_selected_call_duration_seconds = Gauge(
    "voip_controller_selected_call_duration_seconds",
    "Random call duration selected for this agent",
    ["agent", "target"]
)

controller_selected_idle_duration_seconds = Gauge(
    "voip_controller_selected_idle_duration_seconds",
    "Random idle duration selected for this agent",
    ["agent", "target"]
)

controller_last_call_timestamp = Gauge(
    "voip_controller_last_call_timestamp_seconds",
    "Unix timestamp of the most recent call command",
    ["agent", "target"]
)

controller_last_hangup_timestamp = Gauge(
    "voip_controller_last_hangup_timestamp_seconds",
    "Unix timestamp of the most recent hangup command",
    ["agent", "target"]
)

controller_call_duration_seconds = Histogram(
    "voip_controller_call_duration_seconds",
    "Randomized call duration used by the controller",
    ["agent", "target"],
    buckets=[
        60,
        90,
        100,
        110,
        120,
        150
    ]
)

controller_idle_duration_seconds = Histogram(
    "voip_controller_idle_duration_seconds",
    "Randomized idle duration used by the controller",
    ["agent", "target"],
    buckets=[
        10,
        15,
        20,
        25,
        30,
        45,
        60
    ]
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def classify_api_response(response):

    """
    Convert an agent HTTP response into a normalized failure reason.

    Parameters
    ----------
    response:
        The ``requests.Response`` returned by an agent API call.

    Returns
    -------
    str | None
        A machine-friendly failure reason such as ``http_500``, ``busy``,
        ``already_calling``, or ``error``. ``None`` means the response is
        treated as successful.

    Notes
    -----
    HTTP status codes of 400 or higher are always treated as failures. For
    successful HTTP responses, the JSON body's ``status`` field is inspected
    because the agent may return HTTP 200 while still reporting that it is busy
    or unable to perform the requested operation.
    """

    if response.status_code >= 400:
        return f"http_{response.status_code}"

    try:
        body = response.json()
    except ValueError:
        return None

    status = body.get("status", "")

    if status in [
        "busy",
        "already calling",
        "error"
    ]:
        return status.replace(" ", "_")

    return None


def send_call_command(agent):

    """
    Ask one VoIP agent to start a call to its configured target.

    The function sends a POST request to ``/call`` with the SIP extension as a
    query parameter. Prometheus counters, timestamps, and the controller-side
    active-call gauge are updated according to the result.

    Returns
    -------
    bool
        ``True`` when the command is accepted as successful; otherwise
        ``False``.
    """

    name = agent["name"]
    api = agent["api"]
    target = agent["target"]

    try:

        print(f"[{name}] CALL -> {target}")

        response = requests.post(
            f"{api}/call",
            params={
                "target": target
            },
            timeout=HTTP_TIMEOUT_SECONDS
        )

        print(f"[{name}] CALL status: {response.status_code}")
        print(f"[{name}] CALL response: {response.text}")

        failure_reason = classify_api_response(response)

        if failure_reason:

            controller_command_failed_total.labels(
                agent=name,
                command="call",
                reason=failure_reason
            ).inc()

            controller_agent_call_active.labels(
                agent=name,
                target=target
            ).set(0)

            return False

        controller_call_command_total.labels(
            agent=name,
            target=target
        ).inc()

        controller_last_call_timestamp.labels(
            agent=name,
            target=target
        ).set(time.time())

        controller_agent_call_active.labels(
            agent=name,
            target=target
        ).set(1)

        return True

    except Exception as e:

        print(f"[{name}] CALL exception: {e}")

        controller_command_failed_total.labels(
            agent=name,
            command="call",
            reason="exception"
        ).inc()

        controller_agent_call_active.labels(
            agent=name,
            target=target
        ).set(0)

        return False



def send_hangup_command(agent):

    """
    Ask one VoIP agent to terminate its current call.

    A successful request increments the hangup counter, records the current Unix
    timestamp, and marks the controller-side call state as inactive.

    Returns
    -------
    bool
        ``True`` when the HTTP request succeeds with a status below 400;
        otherwise ``False``.
    """

    name = agent["name"]
    api = agent["api"]
    target = agent["target"]

    try:

        print(f"[{name}] HANGUP")

        response = requests.post(
            f"{api}/hangup",
            timeout=HTTP_TIMEOUT_SECONDS
        )

        print(f"[{name}] HANGUP status: {response.status_code}")
        print(f"[{name}] HANGUP response: {response.text}")

        if response.status_code >= 400:

            controller_command_failed_total.labels(
                agent=name,
                command="hangup",
                reason=f"http_{response.status_code}"
            ).inc()

            return False

        controller_hangup_command_total.labels(
            agent=name
        ).inc()

        controller_last_hangup_timestamp.labels(
            agent=name,
            target=target
        ).set(time.time())

        controller_agent_call_active.labels(
            agent=name,
            target=target
        ).set(0)

        return True

    except Exception as e:

        print(f"[{name}] HANGUP exception: {e}")

        controller_command_failed_total.labels(
            agent=name,
            command="hangup",
            reason="exception"
        ).inc()

        return False


def agent_worker(agent):

    """
    Run an endless, independent call cycle for one configured agent.

    Each worker chooses its own initial delay, call duration, and idle duration.
    Therefore, agents do not begin calls at the same time and do not share one
    global schedule.

    A completed cycle means:
    - the call command succeeded,
    - the configured call duration elapsed,
    - and the hangup command succeeded.

    Unexpected errors are caught so the worker thread remains alive. The worker
    attempts a cleanup hangup and retries after ten seconds.
    """

    name = agent["name"]
    target = agent["target"]

    initial_delay = random.choice(
        INITIAL_START_DELAY_CHOICES_SECONDS
    )

    print(
        f"[{name}] Initial random start delay: "
        f"{initial_delay}s"
    )

    time.sleep(initial_delay)

    while True:

        try:

            call_duration = random.choice(
                CALL_DURATION_CHOICES_SECONDS
            )

            idle_duration = random.choice(
                IDLE_DURATION_CHOICES_SECONDS
            )

            controller_selected_call_duration_seconds.labels(
                agent=name,
                target=target
            ).set(call_duration)

            controller_selected_idle_duration_seconds.labels(
                agent=name,
                target=target
            ).set(idle_duration)

            controller_call_duration_seconds.labels(
                agent=name,
                target=target
            ).observe(call_duration)

            controller_idle_duration_seconds.labels(
                agent=name,
                target=target
            ).observe(idle_duration)

            print("\n--------------------------------")
            print(f"[{name}] New independent call cycle")
            print(f"[{name}] Target: {target}")
            print(f"[{name}] Call duration: {call_duration}s")
            print(f"[{name}] Idle duration after hangup: {idle_duration}s")
            print("--------------------------------")

            call_started = send_call_command(agent)

            if call_started:

                print(
                    f"[{name}] Call started. "
                    f"Waiting {call_duration}s..."
                )

                time.sleep(call_duration)

                hangup_success = send_hangup_command(agent)

                if hangup_success:

                    controller_call_cycle_total.labels(
                        agent=name,
                        target=target
                    ).inc()

            else:

                print(
                    f"[{name}] Call failed or rejected. "
                    f"Skipping to idle."
                )

            print(
                f"[{name}] Idle for {idle_duration}s..."
            )

            time.sleep(idle_duration)

        except Exception as e:

            print(f"[{name}] WORKER ERROR: {e}")

            controller_command_failed_total.labels(
                agent=name,
                command="worker_loop",
                reason="exception"
            ).inc()

            try:
                send_hangup_command(agent)
            except Exception:
                pass

            print(f"[{name}] Retrying in 10s...")

            time.sleep(10)


# ============================================================
# STARTUP
# ============================================================

start_http_server(
    CONTROLLER_METRICS_PORT
)

print(
    f"Controller Prometheus exporter running on :{CONTROLLER_METRICS_PORT}"
)

print("Independent multi-agent controller started")

print(
    "Call duration choices:",
    CALL_DURATION_CHOICES_SECONDS
)

print(
    "Idle duration choices:",
    IDLE_DURATION_CHOICES_SECONDS
)

print(
    "Initial start delay choices:",
    INITIAL_START_DELAY_CHOICES_SECONDS
)

print("\nControlled agents:")

for agent in agents:

    print(
        f"- {agent['name']} | "
        f"API={agent['api']} | "
        f"target={agent['target']}"
    )

    controller_agent_call_active.labels(
        agent=agent["name"],
        target=agent["target"]
    ).set(0)

    controller_selected_call_duration_seconds.labels(
        agent=agent["name"],
        target=agent["target"]
    ).set(0)

    controller_selected_idle_duration_seconds.labels(
        agent=agent["name"],
        target=agent["target"]
    ).set(0)


# ============================================================
# START ONE THREAD PER AGENT
# ============================================================

# Keep references to created threads for visibility and future extension.
# The threads are daemon threads, so they do not independently block shutdown.
threads = []

for agent in agents:

    thread = threading.Thread(
        target=agent_worker,
        args=(agent,),
        daemon=True
    )

    thread.start()

    threads.append(thread)


# ============================================================
# KEEP MAIN THREAD ALIVE
# ============================================================

try:

    while True:
        time.sleep(1)

except KeyboardInterrupt:

    print("\nController stopped by user.")
    print("Sending final hangup to all agents...")

    for agent in agents:

        send_hangup_command(agent)

        controller_agent_call_active.labels(
            agent=agent["name"],
            target=agent["target"]
        ).set(0)