import random
import time
import os
import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ============================================================
# METRICS
# ============================================================

CONTROLLER_METRICS_PORT = int(
    os.getenv(
        "CONTROLLER_METRICS_PORT",
        "9300"
    )
)

CALL_LABELS = [
    "src_agent",
    "dst_agent",
    "src_number",
    "dst_number",
    "pair"
]

ROUTE_LABELS = [
    "src_agent",
    "dst_agent",
    "pair"
]

controller_call_active = Gauge(
    "voip_controller_call_active",
    "Whether the controller currently has an active call session",
    CALL_LABELS
)

controller_current_call_start_timestamp = Gauge(
    "voip_controller_current_call_start_timestamp_seconds",
    "Unix timestamp for the current active call start",
    CALL_LABELS
)

controller_call_started_total = Counter(
    "voip_controller_call_started_total",
    "Total calls started by the controller",
    ROUTE_LABELS
)

controller_call_completed_total = Counter(
    "voip_controller_call_completed_total",
    "Total calls completed by the controller",
    ROUTE_LABELS
)

controller_call_failed_total = Counter(
    "voip_controller_call_failed_total",
    "Total call failures detected by the controller",
    ROUTE_LABELS + ["reason"]
)

controller_call_duration_seconds = Histogram(
    "voip_controller_call_duration_seconds",
    "Planned call duration selected by the controller",
    ROUTE_LABELS,
    buckets=[
        10,
        20,
        30,
        45,
        60,
        90,
        120
    ]
)

controller_idle_duration_seconds = Histogram(
    "voip_controller_idle_duration_seconds",
    "Idle duration selected by the controller between calls",
    buckets=[
        5,
        10,
        15,
        20,
        30,
        60
    ]
)

controller_last_call_timestamp = Gauge(
    "voip_controller_last_call_timestamp_seconds",
    "Unix timestamp for the most recent call start",
    ROUTE_LABELS
)

controller_selected_pair_total = Counter(
    "voip_controller_selected_pair_total",
    "Total times each traffic pair was selected before direction randomization",
    ["pair"]
)


def route_pair(src_id, dst_id):

    return f"{src_id}-{dst_id}"


def metric_labels(src, dst, pair_label):

    return {
        "src_agent": src["name"],
        "dst_agent": dst["name"],
        "src_number": src["number"],
        "dst_number": dst["number"],
        "pair": pair_label
    }


def route_metric_labels(src, dst, pair_label):

    return {
        "src_agent": src["name"],
        "dst_agent": dst["name"],
        "pair": pair_label
    }


def set_call_active(labels, start_time):

    controller_call_active.labels(**labels).set(1)
    controller_current_call_start_timestamp.labels(**labels).set(start_time)


def clear_call_active(labels):

    controller_call_active.labels(**labels).set(0)
    controller_current_call_start_timestamp.labels(**labels).set(0)


def classify_call_response(response):

    if response.status_code >= 400:

        return "api_error"

    try:

        response_body = response.json()

    except ValueError:

        return None

    status = response_body.get("status")

    if status in (
        "busy",
        "already calling",
        "error"
    ):

        return status.replace(" ", "_")

    return None


start_http_server(CONTROLLER_METRICS_PORT)

print(
    f"Controller Prometheus exporter running on :{CONTROLLER_METRICS_PORT}"
)

# ============================================================
# AGENTS
# ============================================================

agents = {
    "A": {
        "name": "Agent-A",
        "number": "6001",
        "api": "http://192.168.10.10:5001"
    },

    "B": {
        "name": "Agent-B",
        "number": "6002",
        "api": "http://192.168.20.10:5001"
    }
}

# ============================================================
# TRAFFIC PATTERNS
# ============================================================

pairs = [
    ("A", "B")
]

weights = [
    1.0
]

# ============================================================
# MAIN LOOP
# ============================================================

print("Central controller started")

while True:

    labels = None
    route_labels = None
    call_was_active = False

    try:

        # ------------------------------------
        # Choose pair
        # ------------------------------------

        selected_pair = random.choices(
            pairs,
            weights=weights,
            k=1
        )[0]

        selected_pair_label = route_pair(
            selected_pair[0],
            selected_pair[1]
        )

        controller_selected_pair_total.labels(
            pair=selected_pair_label
        ).inc()

        pair = list(selected_pair)

        # ------------------------------------
        # Randomize call initiator
        # ------------------------------------

        random.shuffle(pair)

        src_id = pair[0]
        dst_id = pair[1]

        src = agents[src_id]
        dst = agents[dst_id]

        pair_label = route_pair(
            src_id,
            dst_id
        )

        labels = metric_labels(
            src,
            dst,
            pair_label
        )

        route_labels = route_metric_labels(
            src,
            dst,
            pair_label
        )

        # ------------------------------------
        # Random duration
        # ------------------------------------

        duration = random.randint(
            20,
            60
        )

        # ------------------------------------
        # Random idle interval
        # ------------------------------------

        idle_time = random.randint(
            5,
            20
        )

        controller_call_duration_seconds.labels(
            **route_labels
        ).observe(duration)

        controller_idle_duration_seconds.observe(
            idle_time
        )

        print("\n================================")
        print("CALL SESSION")
        print(
            f"{src['name']} ({src['number']}) "
            f"--> "
            f"{dst['name']} ({dst['number']})"
        )
        print(f"Duration: {duration}s")
        print(f"Next wait interval: {idle_time}s")
        print("================================")

        # ------------------------------------
        # Trigger call
        # ------------------------------------

        response = requests.post(
            f"{src['api']}/call",
            params={
                "target": dst["number"]
            },
            timeout=10
        )

        print(
            "API status:",
            response.status_code
        )

        print(
            "Response:",
            response.text
        )

        failure_reason = classify_call_response(
            response
        )

        if failure_reason:

            controller_call_failed_total.labels(
                **route_labels,
                reason=failure_reason
            ).inc()

        else:

            call_start_time = time.time()

            controller_call_started_total.labels(
                **route_labels
            ).inc()

            controller_last_call_timestamp.labels(
                **route_labels
            ).set(call_start_time)

            set_call_active(
                labels,
                call_start_time
            )

            call_was_active = True

        # ------------------------------------
        # Wait for call duration
        # ------------------------------------

        print(
            f"Waiting {duration}s..."
        )

        time.sleep(duration)

        # ------------------------------------
        # Hangup
        # ------------------------------------

        try:

            hangup_response = requests.post(
                f"{src['api']}/hangup",
                timeout=10
            )

            print(
                "Hangup status:",
                hangup_response.status_code
            )

            if call_was_active:

                if hangup_response.status_code >= 400:

                    controller_call_failed_total.labels(
                        **route_labels,
                        reason="hangup_api_error"
                    ).inc()

                else:

                    controller_call_completed_total.labels(
                        **route_labels
                    ).inc()

        except Exception as e:

            print(
                "Hangup failed:",
                e
            )

            if call_was_active:

                controller_call_failed_total.labels(
                    **route_labels,
                    reason="hangup_exception"
                ).inc()

        finally:

            if call_was_active:

                clear_call_active(
                    labels
                )

        # ------------------------------------
        # Idle period
        # ------------------------------------

        print(
            f"Idle for {idle_time}s..."
        )

        time.sleep(idle_time)

    except KeyboardInterrupt:

        print(
            "\nController stopped"
        )

        break

    except Exception as e:

        if route_labels:

            controller_call_failed_total.labels(
                **route_labels,
                reason="exception"
            ).inc()

        if call_was_active and labels:

            clear_call_active(
                labels
            )

        print(
            "\nERROR:",
            e
        )

        print(
            "Retrying in 10s..."
        )

        time.sleep(10)
