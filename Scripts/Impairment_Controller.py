"""
VoIP network impairment controller with NiceGUI and Prometheus support.

This application configures Linux traffic control (tc) and netem on one network
interface so that different VLANs can receive independent network impairments.

Main responsibilities
---------------------
1. Build and initialize the tc hierarchy.
   - A root priority qdisc divides traffic into separate bands.
   - One netem qdisc is attached to each configured VLAN band.
   - Flower filters classify tagged Ethernet frames by VLAN ID.

2. Provide a browser-based NiceGUI interface.
   - Separate tabs are shown for VLAN 110, 120, and 130.
   - Users can configure delay, jitter, and packet loss.
   - Pressing Apply updates the corresponding netem qdisc.

3. Export Prometheus metrics.
   - Current configured delay, jitter, and loss are exported per VLAN.
   - Update timestamps are exported for troubleshooting.
   - Metrics are refreshed periodically in a background thread.

4. Store an impairment history.
   - Every applied configuration is written to a local SQLite database.
   - The database records timestamp, VLAN, delay, jitter, and packet loss.

Traffic classification design
-----------------------------
The controller does not classify packets by RTP port, destination IP address,
or SIP extension. It classifies all IEEE 802.1Q-tagged traffic according to the
VLAN ID and directs that traffic to the associated netem queue.

Concurrency
-----------
The NiceGUI event loop and the background metric-export thread can access the
shared VLAN state at the same time. A re-entrant lock protects the shared
dictionary and ensures consistent metric snapshots.
"""

from nicegui import ui
from prometheus_client import Gauge, start_http_server
import os
import subprocess
import sqlite3
import threading
import time
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

# Physical or virtual interface on which VLAN-tagged traffic is observed.
# All tc qdiscs and filters created by this program are attached here.
NETEM_INTERFACE = 'enp0s9'

METRIC_EXPORT_PORT = int(
    os.getenv(
        'IMPAIRMENT_METRIC_EXPORT_PORT',
        '9100'
    )
)

METRIC_EXPORT_INTERVAL_SECONDS = float(
    os.getenv(
        'IMPAIRMENT_METRIC_EXPORT_INTERVAL_SECONDS',
        '2'
    )
)

# ============================================================
# DATABASE
# ============================================================

# One SQLite connection is shared by the application. NiceGUI callbacks may
# run outside the thread that created the connection, so check_same_thread is
# disabled. Access is simple in this application, but a dedicated database lock
# would be advisable if many concurrent writes were introduced.
conn = sqlite3.connect(
    'impairment_history.db',
    check_same_thread=False
)

cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS impairment_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    vlan TEXT,
    delay INTEGER,
    jitter INTEGER,
    loss INTEGER
)
''')

conn.commit()

# ============================================================
# VLAN CONFIG
# ============================================================

# Maps each VLAN to its location in the tc hierarchy.
#
# classid:
#   Parent priority band to which the VLAN's netem qdisc is attached.
#
# handle:
#   Unique identifier assigned to that VLAN's netem qdisc.
#
# prio:
#   Filter evaluation priority; lower numbers are evaluated first.
VLAN_MAP = {

    '110': {
        'classid': '1:1',
        'handle': '10:',
        'prio': 1,
    },

    '120': {
        'classid': '1:2',
        'handle': '20:',
        'prio': 2,
    },

    '130': {
        'classid': '1:3',
        'handle': '30:',
        'prio': 3,
    }
}

# ============================================================
# VLAN STATE
# ============================================================

# Mutable controller-side state for every VLAN.
# ``last_updated`` stores a Unix timestamp and remains zero until the first
# configuration is applied through the UI.
VLANS = {

    '110': {
        'delay': 0,
        'jitter': 0,
        'loss': 0,
        'last_updated': 0,
    },

    '120': {
        'delay': 0,
        'jitter': 0,
        'loss': 0,
        'last_updated': 0,
    },

    '130': {
        'delay': 0,
        'jitter': 0,
        'loss': 0,
        'last_updated': 0,
    }
}

# ============================================================
# PROMETHEUS METRICS
# ============================================================

metric_delay = None
metric_jitter = None
metric_loss = None
metric_last_updated = None
metric_last_exported = None

# Protects the shared VLANS dictionary while UI callbacks and the periodic
# exporter access it concurrently. RLock permits safe nested acquisition if the
# code is extended with helper functions that also use the same lock.
vlan_lock = threading.RLock()

# ============================================================
# RUN COMMAND
# ============================================================

def run(cmd):

    """
    Execute one shell command and print its output.

    Parameters
    ----------
    cmd:
        Complete shell command to execute.

    Notes
    -----
    ``shell=True`` is used because commands are assembled as shell strings and
    may include shell redirection such as ``2>/dev/null``. Standard output and
    standard error are captured and printed for troubleshooting.

    The function currently does not raise an exception when ``tc`` returns a
    non-zero exit code. Therefore, callers must inspect the printed output when
    verifying whether a traffic-control operation succeeded.
    """

    print(f'\n[CMD] {cmd}\n')

    result = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

# ============================================================
# METRICS SETUP
# ============================================================

def setup_metrics():

    """
    Create all Prometheus Gauge objects used by the controller.

    The metric objects are assigned to global variables because other functions
    update them after initialization. Once the gauges are created, the current
    in-memory impairment state is exported immediately.
    """

    global metric_delay
    global metric_jitter
    global metric_loss
    global metric_last_updated
    global metric_last_exported

    metric_delay = Gauge(
        'voip_impairment_delay_ms',
        'Configured network impairment delay in milliseconds',
        ['vlan']
    )

    metric_jitter = Gauge(
        'voip_impairment_jitter_ms',
        'Configured network impairment jitter in milliseconds',
        ['vlan']
    )

    metric_loss = Gauge(
        'voip_impairment_packet_loss_percent',
        'Configured network impairment packet loss percentage',
        ['vlan']
    )

    metric_last_updated = Gauge(
        'voip_impairment_last_update_timestamp_seconds',
        'Unix timestamp of the last configured impairment update',
        ['vlan']
    )

    metric_last_exported = Gauge(
        'voip_impairment_last_export_timestamp_seconds',
        'Unix timestamp of the last impairment metric export'
    )

    export_current_impairments()


# ============================================================
# METRIC UPDATE
# ============================================================

def export_current_impairments():

    """
    Copy the current VLAN impairment configuration into Prometheus gauges.

    A single timestamp is generated for the export operation. While holding
    ``vlan_lock``, the function publishes delay, jitter, packet loss, and the
    last configuration-update time for every VLAN.

    The exported values describe the controller's configured state. They do not
    directly measure actual packet delay or loss observed on the network.
    """

    now = time.time()

    with vlan_lock:

        for vlan, params in VLANS.items():

            metric_delay.labels(
                vlan=vlan
            ).set(params['delay'])

            metric_jitter.labels(
                vlan=vlan
            ).set(params['jitter'])

            metric_loss.labels(
                vlan=vlan
            ).set(params['loss'])

            metric_last_updated.labels(
                vlan=vlan
            ).set(params['last_updated'])

        metric_last_exported.set(now)


def periodic_metric_exporter():

    """
    Refresh all impairment metrics continuously in a background thread.

    Exceptions are caught inside the loop so that one temporary export problem
    does not permanently stop the metrics thread.
    """

    while True:

        try:

            export_current_impairments()

        except Exception as e:

            print(f'METRIC EXPORT ERROR: {e}')

        time.sleep(METRIC_EXPORT_INTERVAL_SECONDS)


# ============================================================
# SHOW DEBUG
# ============================================================

def show_debug():

    """
    Print detailed tc qdisc and filter statistics for the managed interface.

    The ``-s`` option shows packet counters, byte counters, drops, backlog, and
    other statistics. This is useful for verifying that VLAN filters match
    traffic and that packets are passing through the expected netem qdiscs.
    """

    print('\n================================================')
    print('QDISC STATUS')
    print('================================================\n')

    run(
        f'sudo tc -s qdisc show dev {NETEM_INTERFACE}'
    )

    print('\n================================================')
    print('FILTER STATUS')
    print('================================================\n')

    run(
        f'sudo tc -s filter show dev {NETEM_INTERFACE}'
    )

# ============================================================
# INITIALIZE TC
# ============================================================

def initialize_tc():

    """
    Rebuild the complete traffic-control hierarchy from a clean state.

    Initialization performs these steps:

    1. Delete any existing root qdisc on the configured interface.
    2. Create a four-band priority qdisc with handle ``1:``.
    3. Attach one netem qdisc to bands 1, 2, and 3.
    4. Add flower filters for VLANs 110, 120, and 130.
    5. Display qdisc and filter statistics for verification.

    The fourth priority band is intentionally left without a VLAN-specific
    netem child and acts as an unused/default band in the current design.
    """

    print('\n================================================')
    print('INITIALIZING TC')
    print('================================================\n')

    # --------------------------------------------------------
    # CLEAN OLD CONFIG
    # --------------------------------------------------------

    run(
        f'sudo tc qdisc del dev {NETEM_INTERFACE} root 2>/dev/null'
    )

    # --------------------------------------------------------
    # ROOT PRIO
    # --------------------------------------------------------

    run(
        f'sudo tc qdisc add dev {NETEM_INTERFACE} '
        f'root handle 1: prio bands 4'
    )

    # --------------------------------------------------------
    # VLAN110 NETEM
    # --------------------------------------------------------

    run(
        f'sudo tc qdisc add dev {NETEM_INTERFACE} '
        f'parent 1:1 handle 10: '
        f'netem delay 0ms loss 0%'
    )

    # --------------------------------------------------------
    # VLAN120 NETEM
    # --------------------------------------------------------

    run(
        f'sudo tc qdisc add dev {NETEM_INTERFACE} '
        f'parent 1:2 handle 20: '
        f'netem delay 0ms loss 0%'
    )

    # --------------------------------------------------------
    # VLAN130 NETEM
    # --------------------------------------------------------

    run(
        f'sudo tc qdisc add dev {NETEM_INTERFACE} '
        f'parent 1:3 handle 30: '
        f'netem delay 0ms loss 0%'
    )

    # ========================================================
    # FLOWER FILTERS
    # ========================================================

    # --------------------------------------------------------
    # VLAN110
    # --------------------------------------------------------

    run(
        f'sudo tc filter add dev {NETEM_INTERFACE} '
        f'protocol 802.1Q '
        f'parent 1:0 '
        f'prio 1 '
        f'flower vlan_id 110 '
        f'flowid 1:1'
    )

    # --------------------------------------------------------
    # VLAN120
    # --------------------------------------------------------

    run(
        f'sudo tc filter add dev {NETEM_INTERFACE} '
        f'protocol 802.1Q '
        f'parent 1:0 '
        f'prio 2 '
        f'flower vlan_id 120 '
        f'flowid 1:2'
    )

    # --------------------------------------------------------
    # VLAN130
    # --------------------------------------------------------

    run(
        f'sudo tc filter add dev {NETEM_INTERFACE} '
        f'protocol 802.1Q '
        f'parent 1:0 '
        f'prio 3 '
        f'flower vlan_id 130 '
        f'flowid 1:3'
    )

    print('\n================================================')
    print('TC INITIALIZED SUCCESSFULLY')
    print('================================================\n')

    show_debug()

# ============================================================
# APPLY IMPAIRMENT
# ============================================================

def apply_impairment(vlan):

    """
    Apply the current in-memory impairment values to one VLAN.

    Parameters
    ----------
    vlan:
        VLAN identifier as a string, for example ``"110"``.

    The function reads delay, jitter, and loss under ``vlan_lock``, maps the VLAN
    to its tc parent and handle, and runs ``tc qdisc replace``. ``replace`` is
    used so the command works whether the qdisc already exists or must be
    recreated.

    After applying the configuration, the function:
    - refreshes Prometheus metrics,
    - stores the configuration in SQLite,
    - prints tc debugging statistics,
    - and shows a NiceGUI success notification.
    """

    with vlan_lock:

        delay = VLANS[vlan]['delay']
        jitter = VLANS[vlan]['jitter']
        loss = VLANS[vlan]['loss']

    parent = VLAN_MAP[vlan]['classid']
    handle = VLAN_MAP[vlan]['handle']

    command = (
        f'sudo tc qdisc replace dev {NETEM_INTERFACE} '
        f'parent {parent} '
        f'handle {handle} '
        f'netem '
        f'delay {delay}ms {jitter}ms '
        f'loss {loss}%'
    )

    print('\n================================================')
    print(f'APPLYING IMPAIRMENT VLAN {vlan}')
    print(command)
    print('================================================\n')

    run(command)

    export_current_impairments()

    # --------------------------------------------------------
    # SAVE HISTORY
    # --------------------------------------------------------

    cursor.execute(
        '''
        INSERT INTO impairment_history
        (
            timestamp,
            vlan,
            delay,
            jitter,
            loss
        )
        VALUES (?, ?, ?, ?, ?)
        ''',
        (
            datetime.now().strftime(
                '%Y-%m-%d %H:%M:%S'
            ),
            vlan,
            delay,
            jitter,
            loss
        )
    )

    conn.commit()

    # --------------------------------------------------------
    # DEBUG
    # --------------------------------------------------------

    show_debug()

    # --------------------------------------------------------
    # UI NOTIFICATION
    # --------------------------------------------------------

    ui.notify(
        (
            f'VLAN {vlan} updated | '
            f'Delay={delay}ms | '
            f'Jitter={jitter}ms | '
            f'Loss={loss}%'
        ),
        color='green'
    )


# ============================================================
# VLAN PANEL
# ============================================================

def create_vlan_panel(vlan):

    """
    Build one NiceGUI control panel for a VLAN.

    The panel contains sliders for delay, jitter, and packet loss, plus an Apply
    button. Slider values are copied into the shared ``VLANS`` dictionary only
    when the button is pressed.

    The nested ``apply`` callback captures the VLAN argument and the three slider
    objects, allowing each tab to update only its own VLAN configuration.
    """

    with ui.column().classes(
        'w-full gap-8 p-4'
    ):

        # ----------------------------------------------------
        # DELAY
        # ----------------------------------------------------

        ui.label(
            'Delay (ms)'
        ).classes(
            'text-white text-xl font-bold'
        )

        delay_slider = ui.slider(
            min=0,
            max=1000,
            value=0
        ).props(
            'label color=green'
        ).classes(
            'w-full'
        )

        # ----------------------------------------------------
        # JITTER
        # ----------------------------------------------------

        ui.label(
            'Jitter (ms)'
        ).classes(
            'text-white text-xl font-bold'
        )

        jitter_slider = ui.slider(
            min=0,
            max=500,
            value=0
        ).props(
            'label color=green'
        ).classes(
            'w-full'
        )

        # ----------------------------------------------------
        # LOSS
        # ----------------------------------------------------

        ui.label(
            'Packet Loss (%)'
        ).classes(
            'text-white text-xl font-bold'
        )

        loss_slider = ui.slider(
            min=0,
            max=100,
            value=0
        ).props(
            'label color=green'
        ).classes(
            'w-full'
        )

        # ----------------------------------------------------
        # APPLY BUTTON
        # ----------------------------------------------------

        def apply():

            with vlan_lock:

                VLANS[vlan]['delay'] = delay_slider.value
                VLANS[vlan]['jitter'] = jitter_slider.value
                VLANS[vlan]['loss'] = loss_slider.value
                VLANS[vlan]['last_updated'] = time.time()

            apply_impairment(vlan)

        ui.button(
            f'Apply VLAN {vlan} Impairment',
            on_click=apply
        ).classes(
            'bg-[#27e06e] text-black font-bold mt-5'
        )

# ============================================================
# MAIN PAGE
# ============================================================

@ui.page('/')

def main_page():

    """
    Render the main NiceGUI page.

    The page uses a dark layout with one tab per VLAN. Each tab delegates its
    controls to ``create_vlan_panel`` so the UI logic is not duplicated.
    """

    ui.colors(
        primary='#27e06e'
    )

    with ui.column().classes(
        'w-full items-center bg-[#5a5a5a] min-h-screen p-5'
    ):

        # ====================================================
        # HEADER
        # ====================================================

        with ui.row().classes(
            'w-full bg-[#101214] p-6'
        ):

            ui.label(
                'VoIP Network Impairment Controller'
            ).classes(
                'text-5xl italic font-bold text-[#27e06e]'
            )

        # ====================================================
        # MAIN PANEL
        # ====================================================

        with ui.card().classes(
            'w-[1100px] bg-[#101214] mt-10 p-8'
        ):

            with ui.tabs().classes(
                'text-white'
            ) as tabs:

                vlan110 = ui.tab('VLAN 110')
                vlan120 = ui.tab('VLAN 120')
                vlan130 = ui.tab('VLAN 130')

            with ui.tab_panels(
                tabs,
                value=vlan110
            ).classes(
                'w-full bg-[#101214]'
            ):

                with ui.tab_panel(vlan110):
                    create_vlan_panel('110')

                with ui.tab_panel(vlan120):
                    create_vlan_panel('120')

                with ui.tab_panel(vlan130):
                    create_vlan_panel('130')


# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':

    setup_metrics()

    initialize_tc()

    start_http_server(METRIC_EXPORT_PORT)

    threading.Thread(
        target=periodic_metric_exporter,
        daemon=True
    ).start()

    print(
        '\n================================================\n'
        f'PROMETHEUS EXPORTER STARTED ON :{METRIC_EXPORT_PORT}\n'
        f'IMPAIRMENT METRICS REFRESH EVERY '
        f'{METRIC_EXPORT_INTERVAL_SECONDS}s\n'
        'WEB UI STARTED ON :8080\n'
        '================================================\n'
    )

    ui.run(
        host='192.168.60.10',
        port=8080,
        reload=False,
        title='VoIP Network Impairment Controller'
    )
