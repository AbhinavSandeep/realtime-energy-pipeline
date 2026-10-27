"""Generates grafana/dashboards/energy.json.

Usage: python grafana/make_dashboard.py grafana/dashboards/energy.json
"""
import json
import sys

DS = {"type": "grafana-postgresql-datasource", "uid": "timescaledb"}
METER = "meter_id IN (${meter:sqlstring})"


def target(sql, ref="A", fmt="time_series"):
    return {"refId": ref, "datasource": DS, "editorMode": "code", "format": fmt, "rawQuery": True, "rawSql": sql}


def ts_panel(pid, title, x, y, w, h, targets, unit, desc):
    return {
        "id": pid, "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 10, "showPoints": "never"}},
                        "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": targets,
    }


def stat_panel(pid, title, x, y, sql, unit="short"):
    return {
        "id": pid, "type": "stat", "title": title, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": 6, "h": 4},
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "fixed", "fixedColor": "blue"}}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "value"},
        "targets": [target(sql, fmt="table")],
    }


panels = [
    stat_panel(1, "Readings stored", 0, 0, "SELECT count(*) FROM energy.readings"),
    stat_panel(2, "5-min windows", 6, 0, "SELECT count(*) FROM energy.agg_5min"),
    stat_panel(3, "Invalid readings", 12, 0, "SELECT coalesce(sum(invalid_count),0) FROM energy.quality_5min"),
    stat_panel(4, "Seconds since last ingest", 18, 0,
               "SELECT extract(epoch FROM now() - max(ingested_at)) FROM energy.readings", "s"),
    ts_panel(5, "Active power: 5-min tumbling (avg, min, max)", 0, 4, 24, 9, [
        target(f"""SELECT window_start AS time, avg(avg_active_power) AS "avg kW",
  min(min_active_power) AS "min kW", max(max_active_power) AS "max kW"
FROM energy.agg_5min WHERE $__timeFilter(window_start) AND {METER}
GROUP BY 1 ORDER BY 1""")], "kwatt", "Tumbling 5-minute windows on event time."),
    ts_panel(6, "Trend: 15-min sliding window (slide 5 min)", 0, 13, 12, 8, [
        target(f"""SELECT window_start AS time, avg(avg_active_power) AS "avg kW (15 min)"
FROM energy.agg_15min WHERE $__timeFilter(window_start) AND {METER}
GROUP BY 1 ORDER BY 1""")], "kwatt", "Sliding windows smooth short peaks."),
    ts_panel(7, "Peak current: 15-min sliding max", 12, 13, 12, 8, [
        target(f"""SELECT window_start AS time, max(max_intensity) AS "max A"
FROM energy.agg_15min WHERE $__timeFilter(window_start) AND {METER}
GROUP BY 1 ORDER BY 1""")], "amp", "Highest current intensity per sliding window."),
    ts_panel(8, "Sub-meters per 5 min (Wh)", 0, 21, 12, 8, [
        target(f"""SELECT window_start AS time, sum(sum_sub_1_wh) AS "kitchen",
  sum(sum_sub_2_wh) AS "laundry", sum(sum_sub_3_wh) AS "water heater + AC"
FROM energy.agg_5min WHERE $__timeFilter(window_start) AND {METER}
GROUP BY 1 ORDER BY 1""")], "watth", "Energy per area of the house."),
    ts_panel(9, "Data quality per 5 min", 12, 21, 12, 8, [
        target(f"""SELECT window_start AS time, sum(valid_count) AS "valid", sum(invalid_count) AS "invalid"
FROM energy.quality_5min WHERE $__timeFilter(window_start) AND {METER}
GROUP BY 1 ORDER BY 1""")], "short", "Invalid readings are also written to topic energy.invalid."),
    ts_panel(10, "Daily energy (kWh)", 0, 29, 24, 8, [
        target(f"""SELECT window_start AS time, sum(energy_kwh) AS "total kWh", sum(sub_1_kwh) AS "kitchen kWh",
  sum(sub_2_kwh) AS "laundry kWh", sum(sub_3_kwh) AS "water heater + AC kWh"
FROM energy.agg_daily WHERE $__timeFilter(window_start) AND {METER}
GROUP BY 1 ORDER BY 1""")], "kwatth", "Tumbling 24-hour windows."),
]

dashboard = {
    "uid": "energy-stream",
    "title": "Smart-meter energy stream",
    "tags": ["energy", "streaming"],
    "timezone": "utc",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "editable": False,
    # The data set covers 2006-12-16 to 2010-11-26: the default range shows the first days.
    "time": {"from": "2006-12-16T17:00:00.000Z", "to": "2006-12-20T00:00:00.000Z"},
    "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m"]},
    "templating": {"list": [{
        "name": "meter", "label": "Meter", "type": "query", "datasource": DS,
        "query": "SELECT DISTINCT meter_id FROM energy.agg_5min ORDER BY 1",
        "definition": "SELECT DISTINCT meter_id FROM energy.agg_5min ORDER BY 1",
        "refresh": 2, "multi": True, "includeAll": True, "current": {"selected": True, "text": "All", "value": "$__all"},
    }]},
    "annotations": {"list": []},
    "panels": panels,
}

with open(sys.argv[1], "w", encoding="utf-8", newline="\n") as fh:
    json.dump(dashboard, fh, indent=2)
    fh.write("\n")
print("written", sys.argv[1])
