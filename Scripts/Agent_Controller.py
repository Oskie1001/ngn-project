import random
import time
import requests

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
    },

    "C": {
        "name": "Agent-C",
        "number": "6003",
        "api": "http://192.168.30.10:5001"
    }
}

# ============================================================
# TRAFFIC PATTERNS
# ============================================================

pairs = [
    ("A", "B"),
    ("B", "C"),
    ("C", "A")
]

weights = [
    0.60,
    0.25,
    0.15
]

# ============================================================
# MAIN LOOP
# ============================================================

print("Central controller started")

while True:

    try:

        # ------------------------------------
        # Choose pair
        # ------------------------------------

        pair = random.choices(
            pairs,
            weights=weights,
            k=1
        )[0]

        pair = list(pair)

        # ------------------------------------
        # Randomize call initiator
        # ------------------------------------

        random.shuffle(pair)

        src_id = pair[0]
        dst_id = pair[1]

        src = agents[src_id]
        dst = agents[dst_id]

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

        except Exception as e:

            print(
                "Hangup failed:",
                e
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

        print(
            "\nERROR:",
            e
        )

        print(
            "Retrying in 10s..."
        )

        time.sleep(10)