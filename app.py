"""GridGuard dashboard (Streamlit + Plotly).

Run with:  streamlit run app.py

The dashboard only READS pre-computed results written by ``run_pipeline.py`` (or the
Colab notebook). It never re-processes the raw dataset.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src import dashboard_data as dd
from src.config import SUMMARY_TABLE, load_config

# ----------------------------------------------------------------------------- theme
NAVY, PANEL, BORDER = "#0B1220", "#111B2E", "#1F2E4A"
BLUE, CYAN, TEXT, MUTED = "#2D7BFF", "#00D4FF", "#E6EDF7", "#8FA3BF"
RED, AMBER, GREEN = "#FF5C7A", "#FFB547", "#2ED8A3"
DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

st.set_page_config(page_title="GridGuard", page_icon="⚡", layout="wide")

st.markdown(
    f"""
    <style>
    .stApp {{ background: {NAVY}; color: {TEXT}; font-family: "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }}
    section[data-testid="stSidebar"] {{ background: {PANEL}; border-right: 1px solid {BORDER}; }}
    h1, h2, h3 {{ color: {TEXT}; letter-spacing: .2px; }}
    div[data-testid="stMetric"] {{
        background: {PANEL}; border: 1px solid {BORDER}; border-left: 4px solid {BLUE};
        padding: 14px 16px; border-radius: 10px;
    }}
    div[data-testid="stMetricLabel"] p {{ color: {MUTED}; font-size: .82rem; }}
    div[data-testid="stMetricValue"] {{ color: {CYAN}; }}
    .gg-hero {{ padding: 6px 0 14px 0; border-bottom: 1px solid {BORDER}; margin-bottom: 18px; }}
    .gg-hero h1 {{ margin: 0; font-size: 1.9rem; }}
    .gg-hero p {{ margin: 2px 0 0 0; color: {MUTED}; }}
    .gg-badge {{ display:inline-block; padding:2px 10px; border-radius:999px; font-size:.78rem;
                 border:1px solid {BORDER}; background:{NAVY}; color:{TEXT}; }}
    .gg-ok {{ border-color:{GREEN}; color:{GREEN}; }} .gg-warn {{ border-color:{AMBER}; color:{AMBER}; }}
    .gg-bad {{ border-color:{RED}; color:{RED}; }}
    </style>
    """,
    unsafe_allow_html=True,
)


def style(fig: go.Figure, height: int = 380, title: str | None = None) -> go.Figure:
    fig.update_layout(
        template="plotly_dark", height=height, title=title,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor=PANEL,
        font=dict(color=TEXT), margin=dict(l=10, r=10, t=50 if title else 20, b=10),
        colorway=[CYAN, BLUE, AMBER, RED, GREEN],
        legend=dict(orientation="h", y=1.08, x=0),
    )
    fig.update_xaxes(gridcolor=BORDER, zeroline=False)
    fig.update_yaxes(gridcolor=BORDER, zeroline=False)
    return fig


def show_chart(fig: go.Figure, key: str) -> None:
    try:
        st.plotly_chart(fig, width="stretch", key=key)
    except TypeError:  # older Streamlit versions
        st.plotly_chart(fig, use_container_width=True, key=key)


def show_table(df: pd.DataFrame, key: str) -> None:
    try:
        st.dataframe(df, width="stretch", hide_index=True, key=key)
    except TypeError:
        st.dataframe(df, use_container_width=True, hide_index=True)


def badge(text: str, kind: str = "ok") -> str:
    return f'<span class="gg-badge gg-{kind}">{text}</span>'


# ----------------------------------------------------------------------------- data loading
def _stamp(output_dir: str) -> float:
    """Changes whenever the pipeline writes a new summary, so the cache refreshes itself."""
    folder = Path(output_dir) / SUMMARY_TABLE
    if not folder.is_dir():
        return 0.0
    return max((f.stat().st_mtime for f in folder.iterdir() if f.is_file()), default=0.0)


@st.cache_data(ttl=30, show_spinner=False)
def cached_backend(requested: str, hdfs_enabled: bool, webhdfs_url: str, user: str) -> tuple:
    cfg = load_config().with_overrides(hdfs_enabled=hdfs_enabled, webhdfs_url=webhdfs_url, hdfs_user=user)
    return dd.resolve_backend(cfg, requested)


@st.cache_data(ttl=600, show_spinner="Loading pre-computed results...")
def cached_tables(backend: str, output_dir: str, webhdfs_url: str, hdfs_uri: str,
                  hdfs_base: str, user: str, stamp: float) -> dict:
    cfg = load_config().with_overrides(
        output_dir=Path(output_dir), webhdfs_url=webhdfs_url, hdfs_namenode_uri=hdfs_uri,
        hdfs_base_dir=hdfs_base, hdfs_user=user,
    )
    return dd.load_all(cfg, backend)


def fmt_int(value) -> str:
    try:
        return f"{int(float(value)):,}"
    except (TypeError, ValueError):
        return "n/a"


def date_range_picker(df: pd.DataFrame, key: str):
    lo, hi = df["date"].min().date(), df["date"].max().date()
    picked = st.date_input("Date range", value=(lo, hi), min_value=lo, max_value=hi, key=key)
    if not isinstance(picked, (tuple, list)) or len(picked) != 2:
        st.info("Select both a start and an end date.")
        st.stop()
    return pd.Timestamp(picked[0]), pd.Timestamp(picked[1])


def not_ready(tables: dict) -> None:
    st.warning("**Processing has not been completed yet.** No pre-computed results were found, "
               "so there is nothing to display.")
    st.markdown("Run the pipeline first, then reload this page:")
    st.code("python data_download.py     # real dataset (or use --sample below)\n"
            "python run_pipeline.py      # add --sample for the small synthetic test file", language="bash")
    for res in tables.values():
        if not res.ok and res.message:
            st.caption(f"• {res.name}: {res.message}")


# ----------------------------------------------------------------------------- pages
def page_overview(tables: dict, info: dict) -> None:
    hourly, daily = tables["hourly"].df, tables["daily"].df
    total = info.get("total_records")
    n_anom = int(float(info.get("anomaly_count") or 0))
    evaluated = int(float(info.get("evaluated_hours") or 0))
    pct = (n_anom / evaluated * 100) if evaluated else 0.0

    c = st.columns(5)
    c[0].metric("Records processed", fmt_int(total))
    c[1].metric("Average consumption", f"{hourly['observed_kw'].mean():.3f} kW")
    c[2].metric("Maximum (hourly mean)", f"{hourly['observed_kw'].max():.3f} kW")
    c[3].metric("Detected anomalies", fmt_int(n_anom))
    c[4].metric("Anomaly share", f"{pct:.2f} %")
    st.caption(
        f"Anomaly share = anomalies ÷ hours that could be scored ({fmt_int(evaluated)} of {fmt_int(info.get('hourly_rows'))} "
        f"hourly rows). Rule: |z| ≥ {info.get('anomaly_threshold')} against the previous "
        f"{fmt_int(info.get('baseline_days'))} days at the same hour. Data: {info.get('data_start')} → {info.get('data_end')}."
    )

    if daily.empty:
        st.info("The daily table is empty.")
        return
    d = daily.sort_values("date").copy()
    d["rolling_7d"] = d["avg_kw"].rolling(7, min_periods=1).mean()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d["date"], y=d["avg_kw"], name="Daily average (kW)", mode="lines",
                             line=dict(color=BLUE, width=1.5)))
    fig.add_trace(go.Scatter(x=d["date"], y=d["rolling_7d"], name="7-day average", mode="lines",
                             line=dict(color=CYAN, width=3)))
    flagged = d[d["anomaly_count"] > 0]
    if not flagged.empty:
        fig.add_trace(go.Scatter(x=flagged["date"], y=flagged["avg_kw"], mode="markers", name="Day with anomalies",
                                 marker=dict(color=RED, size=7),
                                 text=flagged["anomaly_count"].astype(int).astype(str) + " anomalous hour(s)"))
    fig.update_yaxes(title="kW")
    show_chart(style(fig, 420, "Daily consumption trend"), "ov_daily")


def page_analytics(tables: dict, info: dict) -> None:
    hourly = tables["hourly"].df
    left, right = st.columns([2, 1])
    with left:
        start, end = date_range_picker(hourly, "an_dates")
    with right:
        gran = st.radio("Granularity", ["Hourly", "Daily"], horizontal=True)
    mask = (hourly["date"] >= start) & (hourly["date"] <= end)
    h = hourly[mask]
    if h.empty:
        st.info("No data in the selected range.")
        return

    if gran == "Hourly":
        fig = px.line(h, x="timestamp", y="observed_kw", labels={"observed_kw": "kW", "timestamp": ""})
        title = "Hourly mean consumption"
    else:
        dly = tables["daily"].df
        dly = dly[(dly["date"] >= start) & (dly["date"] <= end)]
        fig = go.Figure(go.Scatter(x=dly["date"], y=dly["avg_kw"], mode="lines+markers", name="Daily average"))
        fig.add_trace(go.Scatter(x=dly["date"], y=dly["peak_hourly_kw"], mode="lines", name="Highest hour of the day"))
        fig.update_yaxes(title="kW")
        title = "Daily consumption"
    show_chart(style(fig, 380, title), "an_series")

    col1, col2 = st.columns(2)
    with col1:
        prof = h.groupby(["hour_of_day", "is_weekend"], as_index=False)["observed_kw"].mean()
        prof["Day type"] = prof["is_weekend"].map({True: "Weekend", False: "Weekday"})
        fig = px.line(prof, x="hour_of_day", y="observed_kw", color="Day type", markers=True,
                      labels={"hour_of_day": "Hour of day", "observed_kw": "Average kW"})
        show_chart(style(fig, 340, "Time-of-day profile"), "an_tod")
    with col2:
        wk = h.groupby("day_name", as_index=False)["observed_kw"].mean()
        wk["day_name"] = pd.Categorical(wk["day_name"], categories=DAY_ORDER, ordered=True)
        wk = wk.sort_values("day_name")
        fig = px.bar(wk, x="day_name", y="observed_kw", labels={"day_name": "", "observed_kw": "Average kW"})
        fig.update_traces(marker_color=BLUE)
        show_chart(style(fig, 340, "Weekly pattern"), "an_week")

    pivot = h.pivot_table(index="hour_of_day", columns="day_name", values="observed_kw", aggfunc="mean")
    pivot = pivot.reindex(columns=[d for d in DAY_ORDER if d in pivot.columns])
    if pivot.shape[1] >= 2:
        fig = px.imshow(pivot, aspect="auto", color_continuous_scale="Blues",
                        labels=dict(x="Day of week", y="Hour of day", color="kW"))
        show_chart(style(fig, 420, "Average consumption: hour × weekday"), "an_heat")
    st.caption("Weekday patterns need several weeks of data to be meaningful.")


def page_anomalies(tables: dict, info: dict) -> None:
    anomalies, hourly = tables["anomalies"].df, tables["hourly"].df
    threshold = float(info.get("anomaly_threshold") or 3.0)
    if anomalies.empty:
        st.success("The pipeline ran, but no hours met the anomaly rule with the current settings "
                   f"(|z| ≥ {threshold}). Try a lower threshold: `python run_pipeline.py --threshold 2.5`.")
        return

    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        direction = st.radio("Direction", ["All anomalies", "High", "Low"])
    with f2:
        sev_options = [s for s in ["Moderate", "Strong", "Extreme"] if s in set(anomalies["severity"])]
        severity = st.multiselect("Severity", sev_options, default=sev_options)
    with f3:
        start, end = date_range_picker(anomalies, "anom_dates")

    df = anomalies[(anomalies["date"] >= start) & (anomalies["date"] <= end) & anomalies["severity"].isin(severity)]
    if direction != "All anomalies":
        df = df[df["anomaly_direction"] == direction]
    sort_by = st.selectbox("Sort by", ["Anomaly score (|z|), highest first", "Time, oldest first"])
    df = (df.sort_values("abs_z_score", ascending=False) if sort_by.startswith("Anomaly")
          else df.sort_values("timestamp"))

    m = st.columns(3)
    m[0].metric("Anomalies shown", fmt_int(len(df)))
    m[1].metric("High", fmt_int((df["anomaly_direction"] == "High").sum()))
    m[2].metric("Low", fmt_int((df["anomaly_direction"] == "Low").sum()))
    if df.empty:
        st.info("No anomalies match the current filters.")
        return

    cols = ["timestamp", "observed_kw", "baseline_mean_kw", "baseline_std_kw", "z_score",
            "anomaly_direction", "severity", "history_count"]
    show_table(df[cols], "anom_table")
    st.download_button("Download filtered anomalies (CSV)", df.to_csv(index=False).encode("utf-8"),
                       file_name="gridguard_anomalies.csv", mime="text/csv")

    st.subheader("Investigate one anomaly")
    options = df.head(500).reset_index(drop=True)
    labels = [f"{r.timestamp:%Y-%m-%d %H:%M}  |  {r.anomaly_direction}  |  z = {r.z_score:.1f}"
              for r in options.itertuples()]
    chosen = st.selectbox("Anomaly", range(len(options)), format_func=lambda i: labels[i])
    row = options.iloc[chosen]
    st.info(row["explanation"])

    ts = row["timestamp"]
    window = hourly[(hourly["timestamp"] >= ts - pd.Timedelta(hours=36)) &
                    (hourly["timestamp"] <= ts + pd.Timedelta(hours=36))].sort_values("timestamp")
    upper = window["baseline_mean_kw"] + threshold * window["baseline_std_kw"]
    lower = window["baseline_mean_kw"] - threshold * window["baseline_std_kw"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=window["timestamp"], y=upper, mode="lines", line=dict(width=0),
                             showlegend=False, hoverinfo="skip"))
    fig.add_trace(go.Scatter(x=window["timestamp"], y=lower, mode="lines", line=dict(width=0),
                             fill="tonexty", fillcolor="rgba(45,123,255,0.18)", name=f"Baseline ± {threshold:g}σ"))
    fig.add_trace(go.Scatter(x=window["timestamp"], y=window["baseline_mean_kw"], mode="lines",
                             name="Historical mean (same hour, prior days)", line=dict(color=BLUE, dash="dash")))
    fig.add_trace(go.Scatter(x=window["timestamp"], y=window["observed_kw"], mode="lines+markers",
                             name="Observed", line=dict(color=CYAN)))
    fig.add_trace(go.Scatter(x=[ts], y=[row["observed_kw"]], mode="markers", name="Selected anomaly",
                             marker=dict(color=RED, size=14, symbol="star")))
    fig.update_yaxes(title="kW")
    show_chart(style(fig, 420, "Observed consumption vs. historical baseline (±36 h)"), "anom_detail")
    st.caption("A flag is a statistical deviation from this household's own recent pattern. "
               "It does not prove theft, equipment failure or any fault.")


def page_peaks(tables: dict, info: dict) -> None:
    hourly, daily = tables["hourly"].df, tables["daily"].df
    top, freq = tables["peak_top"].df, tables["peak_freq"].df

    if not top.empty:
        max_n = int(min(len(top), 50))
        n = st.slider("Number of peak periods to show", 5, max_n, min(10, max_n)) if max_n > 5 else max_n
        t = top.head(n)
        show_table(t[["timestamp", "observed_kw", "baseline_mean_kw", "diff_from_baseline_kw",
                      "diff_from_baseline_pct", "z_score"]], "peak_table")
        lab = t["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=lab, y=t["observed_kw"], name="Observed", marker_color=CYAN))
        fig.add_trace(go.Bar(x=lab, y=t["baseline_mean_kw"], name="Historical average (same hour)", marker_color=BLUE))
        fig.update_layout(barmode="group")
        fig.update_yaxes(title="kW")
        show_chart(style(fig, 380, "Top peak periods vs. their historical average"), "pk_bar")
    else:
        st.info("No peak table available.")

    c1, c2 = st.columns(2)
    with c1:
        if not freq.empty:
            fig = px.bar(freq.sort_values("hour_of_day"), x="hour_of_day", y="days_as_daily_peak",
                         labels={"hour_of_day": "Hour of day", "days_as_daily_peak": "Days this hour was the daily peak"})
            fig.update_traces(marker_color=AMBER)
            show_chart(style(fig, 340, "Recurring peak hours"), "pk_freq")
    with c2:
        vals = hourly["observed_kw"].dropna()
        fig = px.histogram(vals, nbins=60, labels={"value": "Hourly mean kW"})
        fig.update_traces(marker_color=BLUE)
        fig.update_layout(showlegend=False)
        if len(vals):
            p95 = float(vals.quantile(0.95))
            fig.add_vline(x=p95, line_dash="dash", line_color=RED, annotation_text=f"95th pct = {p95:.2f} kW")
        show_chart(style(fig, 340, "Consumption distribution (hourly)"), "pk_hist")

    ok = hourly[hourly["baseline_status"] == "ok"]
    if not ok.empty:
        cmp_ = ok.groupby("hour_of_day", as_index=False)[["observed_kw", "baseline_mean_kw"]].mean()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=cmp_["hour_of_day"], y=cmp_["observed_kw"], name="Observed (average)",
                                 mode="lines+markers", line=dict(color=CYAN)))
        fig.add_trace(go.Scatter(x=cmp_["hour_of_day"], y=cmp_["baseline_mean_kw"], name="Historical baseline (average)",
                                 mode="lines+markers", line=dict(color=BLUE, dash="dash")))
        fig.update_xaxes(title="Hour of day")
        fig.update_yaxes(title="kW")
        show_chart(style(fig, 340, "Observed vs. historical average by hour"), "pk_cmp")

    if not daily.empty:
        st.subheader("Highest-demand days")
        d = daily.sort_values("peak_hourly_kw", ascending=False).head(10)
        show_table(d[["date", "day_name", "peak_hourly_kw", "avg_kw", "energy_kwh", "hours_with_data"]], "pk_days")


def page_storage(tables: dict, info: dict, cfg, requested: str, backend: str, backend_msg: str) -> None:
    st.subheader("Storage backend")
    c = st.columns(3)
    c[0].metric("Selected source", requested.capitalize())
    c[1].metric("Dashboard is reading from", "HDFS (WebHDFS)" if backend == "hdfs" else "Local files")
    c[2].metric("Pipeline wrote to", {"hdfs": "HDFS", "local": "Local filesystem"}.get(str(info.get("storage_mode")), "unknown"))
    st.caption(backend_msg)

    if info:
        if info.get("storage_mode") == "hdfs" and backend != "hdfs":
            st.warning("The pipeline stored its results in HDFS, but the dashboard is currently reading the **local export** "
                       "of that run (HDFS/WebHDFS is not reachable from here or was not selected).")
        elif info.get("storage_mode") == "local":
            st.info("Standalone mode: the pipeline used the local filesystem. No HDFS was involved in this run.")

    hdfs_ok = [r for r in tables.values() if r.source == "hdfs" and r.ok]
    st.subheader("HDFS paths")
    if backend == "hdfs" and hdfs_ok:
        st.success("Connected to HDFS - tables below were read from these paths.")
        show_table(pd.DataFrame({"table": [r.name for r in hdfs_ok], "HDFS path": [r.location for r in hdfs_ok]}), "st_hdfs")
    else:
        st.info("HDFS is not connected, so no HDFS paths are shown. "
                "To use it: start the cluster (`hadoop/README.md`), set `GRIDGUARD_HDFS_ENABLED=true`, "
                "run `python run_pipeline.py --hdfs`, then choose Auto or HDFS in the sidebar.")

    st.subheader("Tables and schema")
    rows = [{"table": k, "status": r.status, "rows": len(r.df), "columns": len(r.df.columns),
             "source": r.source, "location": r.location or r.message} for k, r in tables.items()]
    show_table(pd.DataFrame(rows), "st_tables")
    for key, res in tables.items():
        if res.ok:
            with st.expander(f"Schema: {key}"):
                show_table(pd.DataFrame({"column": res.df.columns, "dtype": res.df.dtypes.astype(str).values}), f"st_schema_{key}")

    st.subheader("Last pipeline run")
    if info:
        show_table(pd.DataFrame({"setting": list(info.keys()), "value": [str(v) for v in info.values()]}), "st_info")
    else:
        st.warning("No pipeline summary found - processing has not been completed yet.")


# ----------------------------------------------------------------------------- main
def main() -> None:
    cfg = load_config()
    with st.sidebar:
        st.markdown("## ⚡ GridGuard")
        st.caption("PySpark · Hadoop HDFS · Streamlit")
        page = st.radio("Navigate", ["Overview", "Consumption Analytics", "Anomaly Investigation",
                                     "Peak Demand", "Data and Storage"])
        requested = st.selectbox("Data source", ["Auto", "Local files", "HDFS (WebHDFS)"],
                                 help="Auto uses HDFS only if it is enabled and reachable.")
        if st.button("Reload results"):
            st.cache_data.clear()

    req_key = {"Auto": "auto", "Local files": "local", "HDFS (WebHDFS)": "hdfs"}[requested]
    backend, backend_msg = cached_backend(req_key, cfg.hdfs_enabled, cfg.webhdfs_url, cfg.hdfs_user)
    tables = cached_tables(backend, str(cfg.output_dir), cfg.webhdfs_url, cfg.hdfs_namenode_uri,
                           cfg.hdfs_base_dir, cfg.hdfs_user, _stamp(str(cfg.output_dir)))
    info = dd.summary_dict(tables)
    ready = bool(info) and tables["hourly"].ok

    with st.sidebar:
        st.markdown("---")
        st.markdown("**Processing status**")
        if ready:
            st.markdown(badge("completed", "ok"), unsafe_allow_html=True)
            st.caption(f"Last run: {info.get('processed_at')}")
        else:
            st.markdown(badge("not completed", "warn"), unsafe_allow_html=True)
        st.markdown("**Storage status**")
        if backend == "hdfs":
            st.markdown(badge("reading from HDFS", "ok"), unsafe_allow_html=True)
        elif info.get("storage_mode") == "hdfs":
            st.markdown(badge("local export of HDFS run", "warn"), unsafe_allow_html=True)
        else:
            st.markdown(badge("local files (standalone)", "warn" if not ready else "ok"), unsafe_allow_html=True)

    st.markdown(
        '<div class="gg-hero"><h1>⚡ GridGuard</h1>'
        "<p>PySpark-based electricity consumption anomaly detection and visualization</p></div>",
        unsafe_allow_html=True,
    )

    if page == "Data and Storage":
        page_storage(tables, info, cfg, req_key, backend, backend_msg)
    elif not ready:
        not_ready(tables)
    elif page == "Overview":
        page_overview(tables, info)
    elif page == "Consumption Analytics":
        page_analytics(tables, info)
    elif page == "Anomaly Investigation":
        page_anomalies(tables, info)
    else:
        page_peaks(tables, info)


main()
