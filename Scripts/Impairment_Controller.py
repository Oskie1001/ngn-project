from nicegui import ui
from prometheus_client import Gauge, start_http_server
import subprocess
import sqlite3
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================

NETEM_INTERFACE = 'enp0s9'

# ============================================================
# DATABASE
# ============================================================

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

VLANS = {

    '110': {
        'delay': 0,
        'jitter': 0,
        'loss': 0,
    },

    '120': {
        'delay': 0,
        'jitter': 0,
        'loss': 0,
    },

    '130': {
        'delay': 0,
        'jitter': 0,
        'loss': 0,
    }
}

# ============================================================
# PROMETHEUS METRICS
# ============================================================

metric_delay = None
metric_jitter = None
metric_loss = None

# ============================================================
# RUN COMMAND
# ============================================================

def run(cmd):

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

    global metric_delay
    global metric_jitter
    global metric_loss

    metric_delay = Gauge(
        'voip_impairment_delay_ms',
        'Configured delay',
        ['vlan']
    )

    metric_jitter = Gauge(
        'voip_impairment_jitter_ms',
        'Configured jitter',
        ['vlan']
    )

    metric_loss = Gauge(
        'voip_impairment_packet_loss_percent',
        'Configured packet loss',
        ['vlan']
    )

# ============================================================
# SHOW DEBUG
# ============================================================

def show_debug():

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

    # --------------------------------------------------------
    # UPDATE PROMETHEUS
    # --------------------------------------------------------

    metric_delay.labels(vlan=vlan).set(delay)
    metric_jitter.labels(vlan=vlan).set(jitter)
    metric_loss.labels(vlan=vlan).set(loss)

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

            VLANS[vlan]['delay'] = delay_slider.value
            VLANS[vlan]['jitter'] = jitter_slider.value
            VLANS[vlan]['loss'] = loss_slider.value

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

    start_http_server(9100)

    print(
        '\n================================================\n'
        'PROMETHEUS EXPORTER STARTED ON :9100\n'
        'WEB UI STARTED ON :8080\n'
        '================================================\n'
    )

    ui.run(
        host='192.168.60.10',
        port=8080,
        reload=False,
        title='VoIP Network Impairment Controller'
    )