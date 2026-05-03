import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime

# ── Config ───────────────────────────────────────────────
DB_URL = "postgresql://energy_user:energy_pass@localhost:5432/energy_db"
engine = create_engine(DB_URL)

st.set_page_config(
    page_title="Energy Grid Dashboard",
    page_icon="⚡",
    layout="wide"
)

# ── Auto Refresh ─────────────────────────────────────────
REFRESH_INTERVAL = 10  # seconds
st.markdown(
    f"""
    <meta http-equiv="refresh" content="{REFRESH_INTERVAL}">
    """,
    unsafe_allow_html=True
)

# ── Data Loaders ─────────────────────────────────────────
@st.cache_data(ttl=10)
def load_latest_readings():
    return pd.read_sql("""
        SELECT DISTINCT ON (zone)
            zone, timestamp, energy_consumed_kwh,
            apparent_power_kva, demand_priority, voltage
        FROM public.energy_readings
        ORDER BY zone, timestamp DESC
    """, engine)

@st.cache_data(ttl=10)
def load_readings_history():
    return pd.read_sql("""
        SELECT zone, timestamp, energy_consumed_kwh, demand_priority
        FROM public.energy_readings
        ORDER BY timestamp DESC
        LIMIT 500
    """, engine)

@st.cache_data(ttl=10)
def load_allocations():
    return pd.read_sql("""
        SELECT DISTINCT ON (zone)
            zone, demand_kwh, allocated_kwh,
            allocation_pct, priority_score, status
        FROM public.energy_allocations
        ORDER BY zone, run_time DESC
    """, engine)

@st.cache_data(ttl=10)
def load_transport_flows():
    return pd.read_sql("""
        SELECT DISTINCT ON (source_name, zone)
            source_name, zone, flow_kwh,
            transmission_loss_pct, effective_kwh
        FROM public.transport_flows
        ORDER BY source_name, zone, run_time DESC
    """, engine)

# ── Header ───────────────────────────────────────────────
st.title("⚡ Real-Time Energy Grid Dashboard")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · Auto-refreshes every {REFRESH_INTERVAL}s")

# ── KPI Row ──────────────────────────────────────────────
readings    = load_latest_readings()
allocations = load_allocations()
flows       = load_transport_flows()

col1, col2, col3, col4, col5 = st.columns(5)

total_demand    = allocations["demand_kwh"].sum()
total_allocated = allocations["allocated_kwh"].sum()
total_effective = flows["effective_kwh"].sum()
total_loss      = flows["flow_kwh"].sum() - total_effective
avg_voltage     = readings["voltage"].mean()

col1.metric("🔋 Total Demand",    f"{total_demand:.1f} kWh")
col2.metric("✅ Total Allocated", f"{total_allocated:.1f} kWh")
col3.metric("⚡ Effective Power", f"{total_effective:.1f} kWh")
col4.metric("📉 Transmission Loss", f"{total_loss:.2f} kWh")
col5.metric("🔌 Avg Voltage",     f"{avg_voltage:.1f} V")

st.divider()

# ── Row 1: Live Readings + Priority ──────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("📡 Live Zone Readings")
    fig = px.bar(
        readings.sort_values("energy_consumed_kwh", ascending=False),
        x="zone",
        y="energy_consumed_kwh",
        color="demand_priority",
        color_discrete_map={
            "high":   "#ef4444",
            "medium": "#f97316",
            "low":    "#22c55e"
        },
        labels={"energy_consumed_kwh": "Energy (kWh)", "zone": "Zone"},
        title="Current Energy Consumption by Zone"
    )
    fig.update_layout(showlegend=True, height=350)
    st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("🎯 Stage 1 — Fair Allocation (LP)")
    if not allocations.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            name="Demand",
            x=allocations["zone"],
            y=allocations["demand_kwh"],
            marker_color="#94a3b8"
        ))
        fig2.add_trace(go.Bar(
            name="Allocated",
            x=allocations["zone"],
            y=allocations["allocated_kwh"],
            marker_color="#3b82f6"
        ))
        fig2.update_layout(
            barmode="group",
            height=350,
            title="Demand vs Allocated per Zone",
            yaxis_title="kWh"
        )
        st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ── Row 2: Transport Flows + History ─────────────────────
col_c, col_d = st.columns(2)

with col_c:
    st.subheader("🔀 Stage 2 — Transport Routing")
    if not flows.empty:
        fig3 = px.sunburst(
            flows,
            path=["source_name", "zone"],
            values="flow_kwh",
            color="transmission_loss_pct",
            color_continuous_scale="RdYlGn_r",
            title="Power Flow: Source → Zone (color = loss %)"
        )
        fig3.update_layout(height=400)
        st.plotly_chart(fig3, use_container_width=True)

with col_d:
    st.subheader("📈 Energy Consumption History")
    history = load_readings_history()
    if not history.empty:
        fig4 = px.line(
            history.sort_values("timestamp"),
            x="timestamp",
            y="energy_consumed_kwh",
            color="zone",
            title="Energy Consumption Over Time",
            labels={
                "energy_consumed_kwh": "kWh",
                "timestamp": "Time"
            }
        )
        fig4.update_layout(height=400)
        st.plotly_chart(fig4, use_container_width=True)

st.divider()

# ── Row 3: Allocation Table + Flow Table ─────────────────
col_e, col_f = st.columns(2)

with col_e:
    st.subheader("📋 Allocation Decisions")
    if not allocations.empty:
        display = allocations[[
            "zone", "demand_kwh", "allocated_kwh",
            "allocation_pct", "priority_score"
        ]].copy()
        display.columns = [
            "Zone", "Demand (kWh)", "Allocated (kWh)",
            "Allocation %", "Priority"
        ]
        st.dataframe(display, use_container_width=True, hide_index=True)

with col_f:
    st.subheader("🚦 Transport Flows")
    if not flows.empty:
        display2 = flows[[
            "source_name", "zone", "flow_kwh",
            "transmission_loss_pct", "effective_kwh"
        ]].copy()
        display2.columns = [
            "Source", "Zone", "Flow (kWh)",
            "Loss %", "Effective (kWh)"
        ]
        st.dataframe(display2, use_container_width=True, hide_index=True)
