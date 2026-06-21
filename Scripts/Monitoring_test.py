import pexpect
import re
import time

from prometheus_client import (
    start_http_server,
    Gauge
)

# =====================================
# PROMETHEUS METRICS
# =====================================

rx_loss_metric = Gauge(
    'voip_rx_packet_loss',
    'RTP RX packet loss'
)

tx_loss_metric = Gauge(
    'voip_tx_packet_loss',
    'RTP TX packet loss'
)

rx_jitter_metric = Gauge(
    'voip_rx_jitter_ms',
    'RTP RX jitter ms'
)

tx_jitter_metric = Gauge(
    'voip_tx_jitter_ms',
    'RTP TX jitter ms'
)

rtt_metric = Gauge(
    'voip_rtt_ms',
    'RTP RTT ms'
)

# =====================================
# START PROMETHEUS EXPORTER
# =====================================

start_http_server(8001)

print("Prometheus exporter running")
print("Metrics: http://localhost:8000/metrics")

# =====================================
# START PJSUA
# =====================================

child = pexpect.spawn(
    "pjsua --config-file=6001.conf",
    encoding="utf-8",
    timeout=30
)

# Optional:
# show live pjsua logs
child.logfile = open(
    "/tmp/pjsua_pexpect.log",
    "w"
)

print("Starting pjsua...")

# Wait until pjsua prompt appears
child.expect(">>>")

print("PJSUA ready")

# =====================================
# MAIN MONITORING LOOP
# =====================================

while True:

    try:

        # =================================
        # SEND dq COMMAND
        # =================================

        child.sendline("dq")

        # Wait until pjsua finishes
        # and returns to prompt
        child.expect(">>>")

        # Fresh dq output
        output = child.before

        print("\n===== DQ OUTPUT =====")
        print(output)
        print("=====================\n")

        # =================================
        # RX PACKET LOSS
        # =================================

        rx_match = re.search(
            r'RX.*?pkt loss=\d+\s+\(([\d\.]+)%\)',
            output,
            re.DOTALL
        )

        if rx_match:

            rx_loss = float(
                rx_match.group(1)
            )

            rx_loss_metric.set(
                rx_loss
            )

            print(
                f"RX LOSS: {rx_loss}"
            )

        # =================================
        # TX PACKET LOSS
        # =================================

        tx_match = re.search(
            r'TX.*?pkt loss=\d+\s+\(([\d\.]+)%\)',
            output,
            re.DOTALL
        )

        if tx_match:

            tx_loss = float(
                tx_match.group(1)
            )

            tx_loss_metric.set(
                tx_loss
            )

            print(
                f"TX LOSS: {tx_loss}"
            )

        # =================================
        # RTT
        # =================================

        rtt_match = re.search(
            r'RTT msec\s*:\s*([\d\.]+)',
            output
        )

        if rtt_match:

            rtt = float(
                rtt_match.group(1)
            )

            rtt_metric.set(
                rtt
            )

            print(
                f"RTT: {rtt}"
            )

        # =================================
        # JITTER
        # =================================

        jitter_matches = re.findall(
            r'jitter\s*:\s*[\d\.]+\s+([\d\.]+)',
            output
        )

        if len(jitter_matches) >= 1:

            rx_jitter = float(
                jitter_matches[0]
            )

            rx_jitter_metric.set(
                rx_jitter
            )

            print(
                f"RX JITTER: {rx_jitter}"
            )

        if len(jitter_matches) >= 2:

            tx_jitter = float(
                jitter_matches[1]
            )

            tx_jitter_metric.set(
                tx_jitter
            )

            print(
                f"TX JITTER: {tx_jitter}"
            )

        # =================================
        # WAIT BEFORE NEXT dq
        # =================================

        time.sleep(5)

    except pexpect.TIMEOUT:

        print("Timeout waiting for pjsua output")

    except Exception as e:

        print(
            f"Monitoring error: {e}"
        )

        time.sleep(5)