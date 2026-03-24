from __future__ import annotations

import pandas as pd
import streamlit as st

from enterprise_data_trust_poc.config import WORKBOOK_PATH, export_paths
from enterprise_data_trust_poc.db import (
    app_engine,
    export_summary_to_desktop,
    get_adoption,
    get_before_state,
    get_dq_results,
    get_feed_monitor,
    get_incidents,
    get_issue_log,
    get_kpi_registry,
    load_workbook_to_postgres,
    run_certified_pipeline,
    test_connection,
)

st.set_page_config(page_title="Enterprise Data Platform", layout="wide")

if "engine" not in st.session_state:
    st.session_state.engine = None

if "summary_df" not in st.session_state:
    st.session_state.summary_df = None


def connect_app_engine():

    engine = app_engine()

    test_connection(engine)

    tables = pd.read_sql(
        "SELECT tablename FROM pg_tables WHERE schemaname='public'",
        engine
    )

    if len(tables) == 0:
        load_workbook_to_postgres(engine)

    return engine


def certification_badge(status):

    mapping = {
        "Certified": "✅ Certified",
        "Pending": "⚠️ Pending",
        "Pending Review": "⚠️ Pending Review",
        "Not Trusted": "❌ Not Trusted",
    }

    return mapping.get(status, status)


st.title("Enterprise Data Platform Transformation")
st.caption("Director Enterprise Data, Analytics & Integration capability demonstration")

with st.sidebar:

    st.subheader("Application")

    st.success("Railway auto-connect enabled")

    st.write("Sample workbook")

    st.code(str(WORKBOOK_PATH))

    paths = export_paths()

    st.write("Export paths")

    st.code(f'Excel: {paths["xlsx"]}\nCSV: {paths["csv"]}')

    if st.button("Connect app", use_container_width=True):

        try:

            st.session_state.engine = connect_app_engine()

            st.success("Connected to application database.")

        except Exception as exc:

            st.error(str(exc))


    if st.button("Reload static sample data", use_container_width=True):

        try:

            engine = connect_app_engine()

            loaded = load_workbook_to_postgres(engine)

            st.session_state.engine = engine

            st.success("Reloaded workbook tables")

            st.json(loaded)

        except Exception as exc:

            st.error(str(exc))


if st.session_state.engine is None:

    try:

        st.session_state.engine = connect_app_engine()

    except Exception as exc:

        st.error(f"Database connection failed: {exc}")

        st.stop()


engine = st.session_state.engine


before_df = get_before_state(engine)

registry_df = get_kpi_registry(engine)

feed_df = get_feed_monitor(engine)

incidents_df = get_incidents(engine)

dq_df = get_dq_results(engine)

issue_df = get_issue_log(engine)

adoption_df = get_adoption(engine)


tabs = st.tabs([

"Problem",
"Solution",
"Integration",
"Data Quality",
"Executive Analytics",
"Business Value",
"Executive Strategy View"

])


#########################
# PROBLEM
#########################

with tabs[0]:

    st.subheader("Enterprise Problem")

    st.markdown("""
Analytics adoption cannot scale because ownership, definitions and platform execution are fragmented.
""")

    c1,c2,c3 = st.columns(3)

    if not before_df.empty:

        spread_value=float(before_df["spread"].iloc[0])

        c1.metric("Revenue variance",f"${spread_value:,.0f}")

    c2.metric("Competing definitions","3")

    c3.metric("Trusted enterprise KPI","0")


    st.dataframe(pd.read_sql("SELECT * FROM kpi_submissions",engine),
    use_container_width=True)


#########################
# SOLUTION
#########################

with tabs[1]:

    st.subheader("Enterprise KPI Industrialization")

    trusted=int((registry_df["certification_status"]=="Certified").sum())

    pending=int((registry_df["certification_status"]=="Pending").sum())

    g1,g2=st.columns(2)

    g1.metric("Certified KPIs",trusted)

    g2.metric("Pending KPIs",pending)

    st.dataframe(registry_df,use_container_width=True)


#########################
# INTEGRATION
#########################

with tabs[2]:

    st.subheader("Integration Platform")

    st.metric(
    "Open incidents",
    int((incidents_df["status"]=="Open").sum())
    )

    st.dataframe(feed_df,use_container_width=True)

    st.dataframe(incidents_df,use_container_width=True)


#########################
# DATA QUALITY
#########################

with tabs[3]:

    st.subheader("Trust Engineering")

    st.metric(
    "Average Quality Score",
    float(dq_df["quality_score"].mean())
    )

    st.dataframe(dq_df)

    st.dataframe(issue_df)


#########################
# EXECUTIVE ANALYTICS
#########################

with tabs[4]:

    st.subheader("Decision Analytics")

    if st.button("Run trusted pipeline"):

        summary_df=run_certified_pipeline(engine)

        st.session_state.summary_df=summary_df


    if st.session_state.summary_df is not None:

        summary_df=st.session_state.summary_df

        total_actual=float(summary_df["actual_revenue"].sum())

        total_plan=float(summary_df["plan_revenue"].fillna(0).sum())

        variance=float(summary_df["variance"].sum())

        k1,k2,k3=st.columns(3)

        k1.metric("Actual",f"${total_actual:,.0f}")

        k2.metric("Plan",f"${total_plan:,.0f}")

        k3.metric("Variance",f"${variance:,.0f}")

        st.dataframe(summary_df)


#########################
# VALUE
#########################

with tabs[5]:

    st.subheader("Business Impact")

    latest=adoption_df.sort_values("month_start").iloc[-1]

    a1,a2,a3,a4=st.columns(4)

    a1.metric("Users",int(latest["active_users"]))

    a2.metric("Trusted KPIs",int(latest["trusted_kpis"]))

    a3.metric("Manual reports removed",int(latest["manual_reports_eliminated"]))

    a4.metric("Hours saved weekly",int(latest["hours_saved_per_week"]))


#########################
# EXECUTIVE STRATEGY VIEW (NEW)
#########################

with tabs[6]:

    st.header("Executive Strategy View")

    st.subheader("Enterprise Data Platform Transformation")

    st.markdown("---")

    st.subheader("Problem")

    st.markdown("""
Enterprise analytics cannot scale because:

• Data ownership fragmented  
• KPI definitions inconsistent  
• Integration reliability uneven  
• Platform accountability unclear  

Result:

Executives cannot rely on analytics for decisions.
""")

    st.markdown("---")

    st.subheader("Solution")

    st.markdown("""
Enterprise Data Platform Alignment:

• KPI industrialization
• Ownership model
• Integration monitoring
• Automated quality controls
• Certified analytics layer
""")

    st.markdown("---")

    st.subheader("Business Value")

    st.markdown("""
Cost reduction  
Faster decision cycles  
Higher analytics adoption  
Lower reporting risk  
Enterprise KPI trust
""")

    st.markdown("---")

    st.subheader("Execution Roadmap")

    st.markdown("""
Discovery → KPI conflicts identified

Pilot → Revenue KPI standardized

Expand → Additional KPIs certified

Institutionalize → Enterprise operating model
""")

    st.success("Enterprise data treated as a platform capability, not reporting output.")