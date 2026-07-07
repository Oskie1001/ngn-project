# Advanced Observability Frameworks for VoIP Networks

This project is a small next-generation networking testbed for generating VoIP traffic, applying controlled network impairments, and exporting measurements for observability. It combines SIP call agents, a multi-agent call controller, Linux traffic-control impairment tooling, and Prometheus metrics so call quality can be tested under repeatable delay, jitter, and packet-loss conditions.

## Project Structure

- `Scripts/Agent.py` - FastAPI service that runs on each VoIP VM. It starts and manages a persistent `pjsua` process, exposes call-control endpoints, parses RTP statistics, and exports per-agent Prometheus metrics on port `9200`.
- `Scripts/Agent_Controller.py` - Central controller that runs independent worker threads for each configured agent. It starts and stops calls through the agents' HTTP APIs, randomizes call and idle durations, and exports controller metrics on port `9300` by default.
- `Scripts/Impairment_Controller.py` - NiceGUI web app for applying Linux `tc netem` impairments to VLAN-tagged traffic. It controls VLANs `110`, `120`, and `130`, stores impairment history in SQLite, and exports impairment metrics on port `9100` by default.
- `Configs/Sample_config` - Example `pjsua` configuration for SIP registration, local binding, auto-answer, and audio playback.
- `Docs/Network Topology.jpg` - Network topology diagram for the testbed.

## Testbed Overview

The system is designed around three VoIP agent VMs and one impairment/controller environment:

- `vm1` agent API: `http://192.168.10.10:5001`, target extension `7001`
- `vm2` agent API: `http://192.168.20.10:5001`, target extension `7002`
- `vm3` agent API: `http://192.168.30.10:5001`, target extension `7003`
- Asterisk/SIP server default IP: `192.168.40.10`
- Impairment UI host: `192.168.60.10:8080`
- VLAN impairment classes: `110`, `120`, and `130`

Each agent controls a local `pjsua` client. The controller calls each agent's `/call` and `/hangup` endpoints in a randomized loop, while the impairment controller applies per-VLAN network conditions to the traffic path. Prometheus-compatible metrics expose both configured impairment state and observed VoIP quality.

## Main Workflow

1. Start one `Agent.py` instance on each VoIP VM with the appropriate SIP config and bind address.
2. Start `Impairment_Controller.py` on the machine that can control the VLAN-tagged interface.
3. Use the NiceGUI interface to apply delay, jitter, or packet loss per VLAN.
4. Start `Agent_Controller.py` to generate repeated SIP calls across all configured agents.
5. Scrape Prometheus metrics from the agents, controller, and impairment controller to compare configured impairments against observed RTP quality.

## Agent API

Each `Agent.py` instance exposes:

- `GET /` - basic service information
- `GET /status` - current call and RTP state
- `POST /call?target=<extension>` - start an outgoing SIP call
- `POST /hangup` - terminate current calls

## Metrics

The project exports Prometheus metrics from three places:

- Agent metrics: port `9200`; call state, active call status, RX/TX packet loss, RX/TX jitter, and RTT.
- Controller metrics: port `9300` by default; command totals, failures, call cycles, selected durations, and controller-side state.
- Impairment metrics: port `9100` by default; configured delay, jitter, loss, update timestamps, and export timestamps per VLAN.

## Configuration

Important environment variables:

- `AGENT_NAME` - label/name for an agent, such as `vm1`.
- `CONFIG_FILE` - path to the `pjsua` config used by an agent.
- `ASTERISK_IP` - SIP server address, defaulting to `192.168.40.10`.
- `METRIC_EXPORT_INTERVAL_SECONDS` - agent metric refresh interval.
- `WATCHDOG_TIMEOUT_SECONDS` - timeout for resetting stale active-call state.
- `CONTROLLER_METRICS_PORT` - Prometheus port for `Agent_Controller.py`, defaulting to `9300`.
- `HTTP_TIMEOUT_SECONDS` - controller HTTP timeout for agent requests.
- `IMPAIRMENT_METRIC_EXPORT_PORT` - Prometheus port for `Impairment_Controller.py`, defaulting to `9100`.
- `IMPAIRMENT_METRIC_EXPORT_INTERVAL_SECONDS` - impairment metric refresh interval.

The impairment controller currently applies `tc` rules to interface `enp0s9`. Update `NETEM_INTERFACE` in `Scripts/Impairment_Controller.py` if the impairment host uses a different interface.

## Running

Install the Python dependencies used by the scripts:

```bash
pip install fastapi uvicorn prometheus-client requests nicegui
```

Run an agent on each VM, for example:

```bash
cd Scripts
export AGENT_NAME=vm1
export CONFIG_FILE=/home/admin/agent/6001.conf
export METRIC_EXPORT_INTERVAL_SECONDS=5
export WATCHDOG_TIMEOUT_SECONDS=120
uvicorn Agent:app --host 192.168.10.10 --port 5001
```

Run the impairment controller on the impairment host:

```bash
cd /path/to/ngn-project
sudo python3 Scripts/Impairment_Controller.py
```

Run the multi-agent call controller:

```bash
python3 Scripts/Agent_Controller.py
```

## Notes

- `Impairment_Controller.py` requires privileges to run Linux `tc` commands.
- The impairment controller writes local history to `impairment_history.db`.
- `pjsua` must be installed and reachable on each agent VM.
- The static agent inventory and VLAN map are defined directly in the Python scripts.
