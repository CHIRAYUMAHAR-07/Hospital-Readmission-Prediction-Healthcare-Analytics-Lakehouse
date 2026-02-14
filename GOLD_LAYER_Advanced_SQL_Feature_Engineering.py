con2 = duckdb.connect()
con2.execute("""CREATE TABLE silver AS
              SELECT * FROM read_parquet('/content/lakehouse/silver/admissions_clean.parquet')""")

gold_sql = """
WITH

-- ── CTE 1: Base with row numbering ─────────────────────────────────────────
base AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY patient_id
               ORDER BY admission_date
           ) AS visit_number
    FROM silver
),

-- ── CTE 2: Rolling Window Features (KEY resume claim) ──────────────────────
rolling AS (
    SELECT
        patient_id,
        admission_date,

        -- Rolling visit counts
        COUNT(*) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date
            ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING
        )                                    AS visits_prior_90d,

        COUNT(*) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date
            ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
        )                                    AS visits_prior_365d,

        -- Rolling avg LOS (care intensity signal)
        ROUND(AVG(los_days) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date
            ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ), 2)                                AS avg_los_last_3_visits,

        -- Cumulative procedures
        SUM(num_procedures) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                    AS cumulative_procedures,

        -- Max charlson historically
        MAX(charlson_index) OVER (
            PARTITION BY patient_id
            ORDER BY admission_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                    AS max_charlson_ever,

        -- Days since last admission (LAG)
        DATEDIFF('day',
            LAG(admission_date) OVER (
                PARTITION BY patient_id ORDER BY admission_date
            ),
            admission_date
        )                                    AS days_since_last_admit
    FROM base
),

-- ── CTE 3: Seasonal & Temporal Patterns ─────────────────────────────────────
seasonal AS (
    SELECT
        patient_id, admission_date,
        CASE admit_season
            WHEN 'WINTER' THEN 1
            WHEN 'SPRING' THEN 2
            WHEN 'SUMMER' THEN 3
            ELSE 4
        END AS season_code,
        CASE WHEN admit_dow IN (5, 6) THEN 1 ELSE 0
        END AS is_weekend_admit
    FROM base
),

-- ── CTE 4: Interaction Features ─────────────────────────────────────────────
interactions AS (
    SELECT
        patient_id, admission_date,
        los_days * charlson_index                           AS los_x_comorbidity,
        ROUND(num_procedures / NULLIF(CAST(los_days AS DOUBLE), 0), 3) AS procedures_per_day,
        has_chf + has_ckd + has_copd                       AS cardio_burden,
        has_diabetes + has_cancer + has_dementia            AS metabolic_burden
    FROM base
)

-- ── FINAL GOLD: Join all feature CTEs ───────────────────────────────────────
SELECT
    b.patient_id, b.admission_date, b.visit_number,
    b.age, b.age_bucket, b.gender,
    b.los_days, b.num_procedures, b.num_diagnoses,
    b.has_diabetes, b.has_chf, b.has_copd,
    b.has_ckd, b.has_cancer, b.has_dementia,
    b.charlson_index, b.prior_visits_12m,
    b.risk_tier, b.ten_yr_survival_prob,
    b.admit_month, b.admit_dow, b.admit_season,

    -- Rolling features (from CTE 2)
    COALESCE(r.visits_prior_90d,      0) AS visits_prior_90d,
    COALESCE(r.visits_prior_365d,     0) AS visits_prior_365d,
    COALESCE(r.avg_los_last_3_visits, b.los_days) AS avg_los_last_3_visits,
    COALESCE(r.cumulative_procedures, 0) AS cumulative_procedures,
    COALESCE(r.max_charlson_ever,     b.charlson_index) AS max_charlson_ever,
    COALESCE(r.days_since_last_admit, 999) AS days_since_last_admit,

    -- Seasonal features (from CTE 3)
    s.season_code,
    s.is_weekend_admit,

    -- Interaction features (from CTE 4)
    i.los_x_comorbidity,
    COALESCE(i.procedures_per_day, 0) AS procedures_per_day,
    i.cardio_burden,
    i.metabolic_burden,

    -- TARGET
    b.readmitted_30d

FROM base b
LEFT JOIN rolling      r ON b.patient_id = r.patient_id AND b.admission_date = r.admission_date
LEFT JOIN seasonal     s ON b.patient_id = s.patient_id AND b.admission_date = s.admission_date
LEFT JOIN interactions i ON b.patient_id = i.patient_id AND b.admission_date = i.admission_date
ORDER BY b.patient_id, b.admission_date
"""

df_gold = con2.execute(gold_sql).df()
df_gold.to_parquet('/content/lakehouse/gold/readmission_features.parquet', index=False)

print(" GOLD LAYER: Feature Engineering Complete")
print(f"   Rows: {len(df_gold):,}")
print(f"   Total features engineered: {len(df_gold.columns) - 1}")
print(f"   Saved to: /content/lakehouse/gold/readmission_features.parquet")
print(f"\n📋 Sample features:")
print(df_gold[['patient_id','age','charlson_index','los_x_comorbidity',
               'visits_prior_90d','days_since_last_admit','risk_tier',
               'readmitted_30d']].head(5).to_string(index=False))