import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text

def show(engine, schema):
    st.title("Measurement Dashboard")

    # --- Load all measurements ---
    query = text(f"""
        SELECT person_id, measurement_concept_id, measurement_date, value_as_number,
               measurement_source_value
        FROM {schema}.measurement
        WHERE value_as_number IS NOT NULL
    """)
    df = pd.read_sql(query, engine)

    if df.empty:
        st.warning("No measurement data available.")
        return


    # --- 1. Table: number of subjects per number of measurements ---
    total_measurements = len(df)
    st.markdown(f"**Total number of measurements: {total_measurements:,}**")

    st.subheader("Table: Number of Subjects per Number of Measurements")
    subject_counts = df.groupby("person_id").size().reset_index(name="num_measurements")

    # Compute distribution: for each number of measurements, how many subjects have that many
    distribution_df = subject_counts["num_measurements"].value_counts().reset_index()
    distribution_df.columns = ["num_measurements", "num_subjects"]
    distribution_df = distribution_df.sort_values("num_measurements").reset_index(drop=True)

    st.dataframe(distribution_df, use_container_width=True)

    # --- 1. Histogram: number of measurements per subject ---
    st.subheader("Distribution: Number of Measurements per Subject")
    subject_counts = df.groupby("person_id").size().reset_index(name="num_measurements")
    fig1 = px.histogram(subject_counts, x="num_measurements", nbins=50,
                        title="Histogram of Measurements per Subject")
    st.plotly_chart(fig1, use_container_width=True)

    # --- 2. Histogram: frequency of measurement_concept_id per subject ---
    st.subheader("Distribution: Frequency of Each Measurement per Subject")
    freq_counts = df.groupby(["person_id", "measurement_concept_id"]).size().reset_index(name="frequency")
    fig2 = px.histogram(freq_counts, x="frequency", nbins=50,
                        title="Histogram: How often each measurement appears per subject")
    st.plotly_chart(fig2, use_container_width=True)

    # --- 3. Top 20 measurements with highest variation ---
    st.subheader("Top 20 Measurements with Highest Variation")
    variance_df = df.groupby("measurement_concept_id")["value_as_number"].var().reset_index(name="variance")
    variance_df = variance_df.sort_values("variance", ascending=False).head(20)
    st.dataframe(variance_df)

    # --- 4. Time series: manual person_source_value + measurement_source_value input ---
    st.subheader("Time-Series Viewer (using source values)")

    person_source_input = st.text_input("Enter Person Source Value:", "")
    measurement_source_input = st.text_input("Enter Measurement Source Value:", "")

    if person_source_input and measurement_source_input:
        query_ts = text(f"""
            SELECT m.measurement_date, m.value_as_number
            FROM {schema}.measurement m
            JOIN {schema}.person p
              ON m.person_id = p.person_id
            WHERE p.person_source_value = :person_source
              AND m.measurement_source_value = :measurement_source
              AND m.value_as_number IS NOT NULL
            ORDER BY m.measurement_date
        """)
        df_ts = pd.read_sql(query_ts, engine,
                            params={"person_source": person_source_input,
                                    "measurement_source": measurement_source_input})

        if df_ts.empty:
            st.warning("No data found for this person_source_value and measurement_source_value.")
        else:
            if len(df_ts) == 1:
                fig_ts = px.scatter(df_ts, x="measurement_date", y="value_as_number",
                                    title=f"{person_source_input} - {measurement_source_input}",
                                    labels={"value_as_number": "Value"})
            else:
                fig_ts = px.line(df_ts, x="measurement_date", y="value_as_number",
                                 markers=True,
                                 title=f"{person_source_input} - {measurement_source_input}",
                                 labels={"value_as_number": "Value"})
            st.plotly_chart(fig_ts, use_container_width=True)

