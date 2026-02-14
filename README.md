# Hospital Readmission Prediction — Enterprise Healthcare Analytics Lakehouse
# 1. Executive Overview
Hospital readmissions within 30 days are a major quality and financial burden for healthcare systems. Unplanned readmissions increase operational cost, strain bed capacity, and directly impact hospital reimbursement under value-based care models.

This project builds a full enterprise-grade Healthcare Analytics Lakehouse that predicts 30-day readmissions using advanced SQL engineering, modern data lake architecture, and explainable machine learning.

It is not just a model.
It is an end-to-end data platform covering:

-> Clinical feature engineering in Snowflake
-> Lakehouse architecture using Apache Iceberg
-> Orchestrated pipelines via Apache Airflow
-> XGBoost modeling with SHAP explainability
-> SMOTE class imbalance handling
-> Great Expectations data quality enforcement
->Power BI executive dashboards
The system processes 100,000+ EHR records and transforms raw admissions data into actionable risk intelligence for clinical leadership.

# 2. Business Problem

Healthcare systems face:

-> High 30-day readmission penalties
->Inconsistent clinical risk scoring
-> Limited interpretability in ML systems
-> Fragmented data pipelines across systems

This project solves:

✔ Predict which patients are at high risk of readmission
✔ Provide interpretable risk drivers to clinicians
✔ Automate ingestion, validation, modeling, and reporting

✔ Reduce manual chart review effort
