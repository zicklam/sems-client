from datetime import datetime
import jmespath
from dotwiz import DotWiz
from influxdb_client import Point, WritePrecision

METRICS = DotWiz({
    ## Power plant stats
    "d_pv_sum": "energeStatisticsCharts.sum",                   # Today total PV generation
    "d_pv_use": "energeStatisticsCharts.selfUseOfPv",           # Today PV consumption
    "d_sell": "energeStatisticsCharts.sell",                    # Today PV excess sell
    "d_buy": "energeStatisticsCharts.buy",                      # Today buy from Grid
    "d_use": "energeStatisticsCharts.consumptionOfLoad",        # Today total consumption

    ## Powerflow stats
    "p_pv": "powerflow.pv",             # Current PV generation
    "p_load": "powerflow.load",         # Current load
    "p_grid": "powerflow.grid",         # Current grid
    #"p_battery": "powerflow.bettery",  # Current battery load (yes, it's 'bettery' in the API)

    ## Invertor-specific stats
    # Any 'jmespath' expression is allowed. If you've got 2 or more invertors
    # you can select them e.g. using their serial number:
    #   "vdc1": "inverter[?sn==`58500MSU123X9876`]|[0].d.vpv1"
    # With a single inverter a simple "inverter[0].d.vpv1" will do.
    "vdc1": "inverter[0].d.vpv1",     # MPPT 1 voltage
    "vdc2": "inverter[0].d.vpv2",     # MPPT 2 voltage
    "vdc3": "inverter[0].d.vpv3",     # MPPT 3 voltage

    "idc1": "inverter[0].d.ipv1",     # MPPT 1 current
    "idc2": "inverter[0].d.ipv2",     # MPPT 2 current
    "idc3": "inverter[0].d.ipv3",     # MPPT 3 current

    "vac": "inverter[0].d.vac1",      # AC voltage
    "iac": "inverter[0].d.iac1",      # AC current
    "fac": "inverter[0].d.fac1",      # AC frequency
    "pac": "inverter[0].d.pac",       # AC power
})

def parse_data(sems_data):
    out_data = {}

    ## Parse the timestamp
    info_time = jmespath.search("info.time", sems_data)
    # The time in 'info.time' is apparently in our local timezone
    timestamp = datetime.strptime(info_time, "%m/%d/%Y %H:%M:%S").timestamp()

    ## Parse all required values
    for key in METRICS:
        value = METRICS[key]
        out_data[key] = jmespath.search(value, sems_data)

        # Powerflow is reported as a string, e.g. "3503(W)"
        if value.startswith("powerflow"):
            if not out_data[key]:
                # Sometimes the API returns None or empty string for powerflow values
                del out_data[key]
                continue

            if out_data[key].endswith("(W)"):
                out_data[key] = int(float(out_data[key][:-3]))

                # Filter out noise
                if abs(out_data[key]) < 10:
                    out_data[key] = 0
                    continue

                # The grid flow direction seems to be indicated by loadStatus field,
                # who knows what gridStatus is for then...
                # I don't have a PV battery, not sure how the powerflows are reported for it.
                # This is brain-dead (as is most of this API).
                if value == "powerflow.grid":
                    flow_direction = jmespath.search("powerflow.loadStatus", sems_data)
                    out_data[key] *= flow_direction

            if type(out_data[key]) != int:
                print(f"Type error: {info_time}: {key}: '{out_data[key]}'")
                del out_data[key]

    return int(timestamp), out_data

def create_point(measurement, timestamp, out_data):
    point = Point(measurement).time(timestamp, WritePrecision.S)
    for key in out_data:
        point.field(key, out_data[key])

    return point
