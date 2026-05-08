"""
Snowflake Streamlit (Snowsight) dashboard for order revenue KPIs.
Runs inside Snowflake with an active Snowpark session.
"""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

DW = "FOOD_PLATFORM_DW"
MART = f"{DW}.ANALYTICS"


def _session():
    return get_active_session()


def load_yearly_kpis() -> pd.DataFrame:
    q = f"""
        SELECT
            year,
            total_revenue,
            total_orders,
            avg_revenue_per_order,
            avg_revenue_per_item,
            max_order_value
        FROM {MART}.VW_YEARLY_REVENUE_KPIS
        ORDER BY year
    """
    rows = _session().sql(q).collect()
    return pd.DataFrame(
        rows,
        columns=[
            "YEAR",
            "TOTAL_REVENUE",
            "TOTAL_ORDERS",
            "AVG_REVENUE_PER_ORDER",
            "AVG_REVENUE_PER_ITEM",
            "MAX_ORDER_VALUE",
        ],
    )


def load_monthly_revenue(year: int) -> pd.DataFrame:
    q = f"""
        SELECT month::NUMBER(2) AS month, total_revenue::NUMBER(18, 2) AS total_revenue
        FROM {MART}.VW_MONTHLY_REVENUE_KPIS
        WHERE year = {year}
        ORDER BY month
    """
    rows = _session().sql(q).collect()
    return pd.DataFrame(rows, columns=["Month", "Total Monthly Revenue"])


def load_months_for_restaurant_view(year: int) -> pd.DataFrame:
    q = f"""
        SELECT DISTINCT month
        FROM {MART}.VW_MONTHLY_REVENUE_BY_RESTAURANT
        WHERE year = {year}
        ORDER BY month
    """
    rows = _session().sql(q).collect()
    return pd.DataFrame(rows, columns=["MONTH"])


def load_top_restaurants(year: int, month: int) -> pd.DataFrame:
    q = f"""
        SELECT
            restaurant_name,
            total_revenue,
            total_orders,
            avg_revenue_per_order,
            avg_revenue_per_item,
            max_order_value
        FROM {MART}.VW_MONTHLY_REVENUE_BY_RESTAURANT
        WHERE year = {year} AND month = {month}
        ORDER BY total_revenue DESC
        LIMIT 10
    """
    rows = _session().sql(q).collect()
    return pd.DataFrame(
        rows,
        columns=[
            "Restaurant Name",
            "Total Revenue (INR)",
            "Total Orders",
            "Avg Revenue per Order (INR)",
            "Avg Revenue per Item (INR)",
            "Max Line Value (INR)",
        ],
    )


def format_inr(value: float) -> str:
    return f"INR {value:,.1f}"


def stripe_style(row: pd.Series) -> list[str]:
    color = "#e8f4f8" if row.name % 2 == 0 else "#ffffff"
    return [f"background-color: {color}"] * len(row)


def main() -> None:
    st.set_page_config(page_title="Food platform revenue", layout="wide")
    st.title("Food platform — revenue cockpit")

    df = load_yearly_kpis()
    if df.empty:
        st.warning("No rows in yearly KPI view yet. Run the SQL pipeline and refresh.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Lifetime revenue (INR)", format_inr(float(df["TOTAL_REVENUE"].sum())))
    with c2:
        st.metric("Lifetime orders", f"{int(df['TOTAL_ORDERS'].sum()):,}")
    with c3:
        st.metric("Peak line value (INR)", f"{float(df['MAX_ORDER_VALUE'].max()):,.0f}")

    st.divider()

    years = sorted(df["YEAR"].unique().tolist())
    default_year = max(years)
    selected_year = st.selectbox("Calendar year", years, index=years.index(default_year))

    slice_y = df[df["YEAR"] == selected_year]
    if slice_y.empty:
        st.info("No KPI row for that year.")
        return

    total_rev = float(slice_y["TOTAL_REVENUE"].iloc[0])
    total_ord = int(slice_y["TOTAL_ORDERS"].iloc[0])
    avg_po = float(slice_y["AVG_REVENUE_PER_ORDER"].iloc[0])
    avg_li = float(slice_y["AVG_REVENUE_PER_ITEM"].iloc[0])
    max_lv = float(slice_y["MAX_ORDER_VALUE"].iloc[0])

    prev = df[df["YEAR"] == selected_year - 1]
    if not prev.empty:
        d_rev = total_rev - float(prev["TOTAL_REVENUE"].iloc[0])
        d_ord = total_ord - int(prev["TOTAL_ORDERS"].iloc[0])
        d_apo = avg_po - float(prev["AVG_REVENUE_PER_ORDER"].iloc[0])
        d_ali = avg_li - float(prev["AVG_REVENUE_PER_ITEM"].iloc[0])
        d_max = max_lv - float(prev["MAX_ORDER_VALUE"].iloc[0])
    else:
        d_rev = d_ord = d_apo = d_ali = d_max = None

    a1, a2, a3 = st.columns(3)
    with a1:
        st.metric("Revenue", format_inr(total_rev), delta=f"{d_rev:,.0f}" if d_rev is not None else None)
        st.metric("Orders", f"{total_ord:,}", delta=f"{d_ord:,}" if d_ord is not None else None)
    with a2:
        st.metric("Avg / order", format_inr(avg_po), delta=f"{d_apo:,.0f}" if d_apo is not None else None)
        st.metric("Avg / line", format_inr(avg_li), delta=f"{d_ali:,.0f}" if d_ali is not None else None)
    with a3:
        st.metric("Max line", format_inr(max_lv), delta=f"{d_max:,.0f}" if d_max is not None else None)

    st.divider()

    month_df = load_monthly_revenue(int(selected_year))
    label_map = {
        1: "Jan",
        2: "Feb",
        3: "Mar",
        4: "Apr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Aug",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }
    month_df["Month"] = month_df["Month"].map(label_map)
    month_df["Month"] = pd.Categorical(
        month_df["Month"],
        categories=list(label_map.values()),
        ordered=True,
    )
    month_df = month_df.sort_values("Month")

    st.subheader(f"{selected_year} — monthly revenue")
    bar = (
        alt.Chart(month_df)
        .mark_bar(color="#0d9488")
        .encode(
            x=alt.X("Month", sort=list(label_map.values())),
            y=alt.Y("Total Monthly Revenue", title="Revenue (INR)"),
        )
        .properties(width=760, height=360)
    )
    st.altair_chart(bar, use_container_width=True)

    line = (
        alt.Chart(month_df)
        .mark_line(color="#115e59", point=True)
        .encode(
            x=alt.X("Month", sort=list(label_map.values())),
            y=alt.Y("Total Monthly Revenue", title="Revenue (INR)"),
        )
        .properties(width=760, height=320)
    )
    st.altair_chart(line, use_container_width=True)

    months_df = load_months_for_restaurant_view(int(selected_year))
    if months_df.empty:
        st.caption("No restaurant-level rows for that year.")
        return

    months = sorted(months_df["MONTH"].astype(int).unique().tolist())
    pick_m = st.selectbox("Month (restaurant leaderboard)", months, index=len(months) - 1)

    st.subheader(f"Top restaurants — {pick_m:02d} / {selected_year}")
    top = load_top_restaurants(int(selected_year), int(pick_m))
    if top.empty:
        st.warning("No rows for that month.")
        return
    st.dataframe(top.style.apply(stripe_style, axis=1), hide_index=True)


main()
