from fastapi import FastAPI
from prometheus_client import Gauge, start_http_server
import os
import subprocess
import threading
import time

app = FastAPI()


# ============================================================
# CONFIG
# ============================================================

def read_config():

    return {
        "agent_name": os.getenv("AGENT_NAME", "vm1"),
        "config_file": os.getenv(
            "CONFIG_FILE",
            "/home/admin/agent/6001.conf"
        ),
        "asterisk_ip": os.getenv(
            "ASTERISK_IP",
            "192.168.40.10"
        ),
        "pjsua_bin": os.getenv(
            "PJSUA_BIN",
            "/usr/local/bin/pjsua"
        ),
        "metrics_port": int(
            os.getenv(
                "METRICS_PORT",
                "9200"
            )
        ),
        "command_delay_seconds": float(
            os.getenv(
                "PJSUA_COMMAND_DELAY_SECONDS",
                "0.5"
            )
        )
    }


config = read_config()


# ============================================================
# STATE AND METRICS
# ============================================================

state_lock = threading.RLock()
pjsua_lock = threading.Lock()
pjsua_process = None
call_active = False
current_target = ""

call_active_metric = Gauge(
    "voip_agent_call_active",
    "Whether this agent is active in a call session",
    ["agent"]
)


def set_call_active(active, target=None):

    global call_active
    global current_target

    with state_lock:

        call_active = active
        if active and target is not None:

            current_target = target

        elif not active:

            current_target = ""

        call_active_metric.labels(
            agent=config["agent_name"]
        ).set(1 if call_active else 0)


def get_status():

    with state_lock:

        return {
            "agent": config["agent_name"],
            "status": "active" if call_active else "idle",
            "call_active": call_active,
            "target": current_target
        }


# ============================================================
# PJSUA
# ============================================================

def is_pjsua_running():

    return (
        pjsua_process is not None
        and
        pjsua_process.poll() is None
        and
        pjsua_process.stdin is not None
        and
        not pjsua_process.stdin.closed
    )


def start_pjsua():

    global pjsua_process

    cmd = [
        config["pjsua_bin"],
        "--config-file",
        config["config_file"]
    ]

    pjsua_process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        universal_newlines=True,
        bufsize=1
    )

    threading.Thread(
        target=monitor_pjsua,
        args=(pjsua_process,),
        daemon=True
    ).start()

    print("Persistent PJSUA started")


def restart_pjsua(reason):

    global pjsua_process

    print(f"Restarting PJSUA: {reason}")

    old_process = pjsua_process
    pjsua_process = None

    if old_process is not None and old_process.poll() is None:

        old_process.terminate()

    set_call_active(False)
    start_pjsua()


def send_pjsua_command(command):

    with pjsua_lock:

        if not is_pjsua_running():

            restart_pjsua("process is not running")

        try:

            pjsua_process.stdin.write(f"{command}\n")
            pjsua_process.stdin.flush()

        except OSError:

            restart_pjsua("failed to write command")
            pjsua_process.stdin.write(f"{command}\n")
            pjsua_process.stdin.flush()


def monitor_pjsua(process):

    for line in process.stdout:

        line = line.strip()

        if not line:

            continue

        print(f"PJSUA: {line}")

        line_lower = line.lower()

        if (
            "calling" in line_lower
            or
            "incoming call" in line_lower
            or
            "confirmed" in line_lower
        ):

            set_call_active(True, current_target)

        elif (
            "disconnected" in line_lower
            or
            "call time:" in line_lower
            or
            "deinitializing media" in line_lower
            or
            "no current call" in line_lower
        ):

            set_call_active(False)


# ============================================================
# BASIC CALL FUNCTIONS
# ============================================================

def make_pjsua_call(target):

    sip_uri = f"sip:{target}@{config['asterisk_ip']}"

    set_call_active(True, target)

    print(f"START CALL -> {sip_uri}")

    send_pjsua_command("m")
    time.sleep(config["command_delay_seconds"])
    send_pjsua_command(sip_uri)

    print("CALL COMMAND SENT")


def hangup_pjsua_call():

    send_pjsua_command("h")
    set_call_active(False)

    print("HANGUP SENT")


# ============================================================
# RECEIVING API
# ============================================================

@app.get("/")
def root():

    return {
        "agent": config["agent_name"],
        "status": "running"
    }


@app.get("/status")
def status():

    return get_status()


@app.post("/call")
def call(target: str):

    with state_lock:

        if call_active:

            return {
                "status": "busy"
            }

    try:

        make_pjsua_call(target)

        return {
            "status": "calling",
            "target": target
        }

    except Exception as e:

        set_call_active(False)

        return {
            "status": "error",
            "message": str(e)
        }


@app.post("/hangup")
def hangup():

    try:

        hangup_pjsua_call()

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

start_http_server(config["metrics_port"])
print(f"Prometheus exporter running on :{config['metrics_port']}")

set_call_active(False)
start_pjsua()

print("VoIP agent ready")


# VM1:
# uvicorn agent:app --host 192.168.10.10 --port 5001

# VM2:
# uvicorn agent:app --host 192.168.20.10 --port 5001
