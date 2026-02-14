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

# 3. Architecture Overview
Modern Healthcare Lakehouse Stack

Languages & Frameworks

Python - > Advanced SQL (CTEs, window functions, recursive queries) -> XGBoost -> SHAP -> SMOTE

Data Platform

Apache Iceberg (Lakehouse table format) -> Snowflake (Analytical warehouse) -> Parquet storage format

Data Engineering

Apache Airflow (workflow orchestration) -> Great Expectations (data quality framework)

Visualization -> Power BI (Executive BI layer)

# 4. Data Layer — Lakehouse Design
Bronze Layer (Raw Ingestion) -> Raw EHR admissions data stored in Parquet format and managed via Apache Iceberg.

From metadata: -> Table: bronze.raw_admissions

Format: PARQUET

Partitioned by: 

admit_year -> admit_month -> Records: 100,000+ -> Schema Versioned -> Columns Engineered at Raw Level -> patient_id -> admission_date -> age, gender -> los_days (length of stay) -> comorbidities (diabetes, CHF, COPD, CKD, cancer, dementia) -> charlson_index -> prior_visits_12m -> readmitted_30d (target variable)

Partitioning strategy improves: 

Query performance -> Time-based filtering -> Cost optimization -> Silver Layer (Feature Engineering in Snowflake) -> Advanced SQL transformations were implemented using: -> Recursive CTEs

Window functions

Aggregations over rolling windows -> Temporal joins -> 40+ Clinical Features Engineered

Examples:

Rolling 12-month visit counts -> Risk-adjusted Charlson comorbidity index -> LOS normalized by age bucket -> Readmission trends by weekday -> Seasonal admission patterns -> Procedure-to-diagnosis ratio -> Comorbidity interaction flags

This layer converts raw EHR data into model-ready analytical features.

Gold Layer (ML-Ready Feature Store) -> Final curated dataset optimized for: -> Feature consistency -> Model reproducibility -> Audit traceability -> Governance compliance

# 5. Data Quality Framework

Before modeling, data validation rules were enforced using Great Expectations.

60+ Data Validation Rules

Examples:

No negative LOS -> Age must be between 0 and 110 -> Null checks on target column -> Comorbidity flags must be binary -> Partition consistency validation

Impact:

Reduced ingestion errors -> Prevented silent data corruption -> Increased model reliability

# 6. Machine Learning Pipeline
Problem Type

Binary Classification:

1 → Readmitted within 30 days

0 → No readmission

Handling Class Imbalance

Healthcare readmissions are typically imbalanced.

Applied:

SMOTE (Synthetic Minority Oversampling Technique) -> This prevented the model from biasing toward majority class predictions. -> Model Used: XGBoost

Why XGBoost? > Handles non-linearity -> Robust to missing data -> Strong performance in tabular healthcare datasets -> Built-in regularization -> High interpretability via SHAP -> Model Performance

ROC-AUC: 0.89

Baseline ROC-AUC: 0.71

Improvement: +18 points

This indicates strong discriminative capability between readmitted vs non-readmitted patients.

# 7. Model Explainability (Clinical Interpretability)

Healthcare ML must be explainable. -> Used SHAP (SHapley Additive Explanations): -> Global feature importance -> Individual patient-level risk explanation -> Feature interaction visualization

Clinical leaders could see:

Which comorbidities drive risk -> How prior visits influence predictions -> Impact of LOS on readmission probability

This enabled:

12% projected reduction in readmission through targeted intervention

# 8. Orchestration with Apache Airflow

The pipeline is fully automated using DAGs:

Stages:

Ingest raw data -> Run validation checks -> Execute feature engineering SQL -> Train model -> Evaluate metrics -> Push predictions to BI layer

Airflow provides:

Retry mechanisms -> SLA monitoring -> Dependency management -> Logging & observability

# 9. BI & Executive Reporting

Power BI dashboards include:

Executive View -> Readmission rate trends -> Risk distribution by department -> Seasonal patterns -> Clinical Operations View -> Top risk drivers -> Patient segmentation -> Intervention prioritization

Impact

Reduced manual chart review by 45% -> Enabled proactive discharge planning -> Improved operational planning

# 10. Performance & Business Impact
Metric	Value -> Records Processed	100,000+ -> Features Engineered	40+ -> Data Quality Rules	60+ -> ROC-AUC	0.89 -> Readmission Reduction (Projected)	12% -> Manual Review Reduction	45%

#11. Governance & Scalability

This project was designed with:

Schema versioning (Iceberg) -> Partition pruning for cost optimization -> Modular SQL transformations -> Reproducible ML pipeline -> Audit-ready feature documentation

It can scale to:

Multi-hospital systems -> Real-time streaming ingestion -> Federated health networks

# 12. Key Technical Highlights

Enterprise-grade Lakehouse implementation

Advanced SQL feature engineering -> Production-oriented ML workflow -> Explainable AI in healthcare -> Automated orchestration -> BI-driven decision support

# 13. Why This Project Matters

This project demonstrates:

Ability to design modern data platforms -> Advanced SQL engineering capability -> Strong understanding of ML for tabular healthcare data -> Knowledge of data quality governance -> Business-aligned analytics development -> End-to-end ownership from ingestion to dashboard

# Short Summary
Hospital readmissions within 30 days remain one of the most persistent and costly challenges in modern healthcare systems. Beyond financial penalties imposed under value-based reimbursement models, readmissions reflect gaps in discharge planning, chronic disease management, and care coordination. This project was designed to address that problem from a systems perspective. Rather than building only a predictive model, the objective was to architect a scalable healthcare analytics platform that ingests raw EHR data, enforces quality standards, engineers clinically meaningful features, trains an interpretable machine learning model, and delivers decision-ready insights to operational leaders.

The foundation of this project is a modern Lakehouse architecture built using Apache Iceberg for table management and Snowflake for analytical processing. The raw EHR dataset consists of more than 100,000 admission records containing demographic attributes, comorbidity indicators, prior utilization metrics, and readmission outcomes. Data is stored in Parquet format and partitioned by admission year and month to optimize query performance and cost efficiency. Schema evolution is handled at the Iceberg layer to ensure long-term maintainability as clinical fields evolve.

From this raw dataset, a structured transformation layer was implemented in Snowflake using advanced SQL techniques. Over 40 clinical and utilization-based features were engineered using recursive CTEs, window functions, rolling aggregations, and temporal joins. For example, rolling 12-month prior visit counts were calculated to capture patient utilization patterns. Length of stay was normalized across age groups to contextualize acuity. Comorbidity burden was aggregated using the Charlson Comorbidity Index and extended through interaction features to model compounding clinical risk. These transformations were designed not just for statistical signal, but for clinical interpretability.

Because healthcare data is inherently messy and high risk, a strong emphasis was placed on data quality enforcement. Great Expectations was integrated into the ingestion pipeline, enforcing more than 60 validation rules at the point of data entry. These included constraints on age ranges, non-negative length of stay, binary validation of comorbidity flags, and null checks on critical predictive fields. By embedding validation into the pipeline, the system prevents silent data corruption and ensures that downstream model performance is not compromised by ingestion errors.

The machine learning component of the project frames readmission as a binary classification problem. Healthcare datasets typically exhibit class imbalance, as only a subset of patients are readmitted within 30 days. To address this, SMOTE was applied to balance minority class observations, preventing bias toward majority class predictions. The predictive model was implemented using XGBoost, selected for its robustness in structured tabular datasets, strong regularization capabilities, and ability to capture nonlinear relationships common in healthcare data.

The resulting model achieved a ROC-AUC score of 0.89, significantly outperforming the baseline performance of 0.71. This improvement reflects meaningful discriminative power in identifying high-risk patients prior to discharge. However, predictive accuracy alone is insufficient in clinical environments. Interpretability is mandatory. To address this requirement, SHAP (SHapley Additive Explanations) was used to provide both global and patient-level feature attribution. Clinicians can examine which variables most strongly influence readmission risk, such as prior visits, comorbidity burden, or extended length of stay. This transparency enables responsible deployment and builds trust among healthcare stakeholders.

The entire workflow is orchestrated using Apache Airflow. Directed Acyclic Graphs (DAGs) manage ingestion, validation, feature engineering, model training, evaluation, and prediction publishing. This orchestration layer provides retry logic, dependency management, logging, and SLA monitoring. The result is a production-style pipeline rather than a one-time experiment. Each component is modular, auditable, and reproducible.

On the analytics delivery side, Power BI dashboards translate model outputs into executive-level insights. Dashboards include readmission trends over time, risk segmentation by department, distribution of high-risk patients, and top predictive drivers. These visualizations are designed for operational leaders and clinical administrators who need to prioritize interventions and allocate resources. By automating risk scoring and surfacing clear insights, the system reduces manual chart review effort by an estimated 45 percent and supports a projected 12 percent reduction in readmission rates through targeted care coordination.

This project demonstrates a full-stack healthcare analytics workflow: modern lakehouse data architecture, advanced SQL engineering, production-ready machine learning, explainable AI, automated orchestration, and executive reporting. It reflects not only technical implementation skills but also domain awareness of regulatory constraints, clinical interpretability requirements, and operational impact.

From an engineering standpoint, the system is scalable to multi-hospital environments, supports schema evolution, and is designed for reproducibility. From a business standpoint, it aligns predictive modeling with measurable operational outcomes. The objective was not to build a model in isolation, but to design a structured analytics system that mirrors enterprise healthcare environments.
