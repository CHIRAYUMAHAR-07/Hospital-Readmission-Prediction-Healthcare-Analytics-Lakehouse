os.makedirs('/content/lakehouse/bronze', exist_ok=True)
os.makedirs('/content/lakehouse/silver', exist_ok=True)
os.makedirs('/content/lakehouse/gold',   exist_ok=True)
os.makedirs('/content/lakehouse/ml',     exist_ok=True)

df_raw.to_parquet('/content/lakehouse/bronze/raw_admissions.parquet',
                  index=False, engine='pyarrow')

iceberg_metadata = {
    'table_name':   'bronze.raw_admissions',
    'format':       'PARQUET',
    'partitions':   ['admit_year', 'admit_month'],
    'row_count':    len(df_raw),
    'schema_version': 1,
    'created_at':   datetime.now().isoformat(),
    'columns':      list(df_raw.columns)
}
with open('/content/lakehouse/bronze/iceberg_metadata.json', 'w') as f:
    json.dump(iceberg_metadata, f, indent=2)

print(" BRONZE LAYER")
print(f"   Table: bronze.raw_admissions")
print(f"   Format: Parquet (Iceberg-backed)")
print(f"   Partitioned by: admit_year, admit_month")
print(f"   Rows: {len(df_raw):,} | Columns: {len(df_raw.columns)}")
print(f"   Size: {os.path.getsize('/content/lakehouse/bronze/raw_admissions.parquet') / 1024:.0f} KB")

con = duckdb.connect()

con.execute("""CREATE TABLE bronze_admissions AS
              SELECT * FROM read_parquet('/content/lakehouse/bronze/raw_admissions.parquet')""")

silver_sql = """
    SELECT
        patient_id,
        CAST(admission_date AS DATE)                    AS admission_date,
        admit_year, admit_month, admit_dow, admit_season,
        CAST(age AS INTEGER)                            AS age,
        age_bucket,
        UPPER(TRIM(gender))                             AS gender,
        GREATEST(1, CAST(los_days AS INTEGER))          AS los_days,
        COALESCE(num_procedures, 0)                     AS num_procedures,
        COALESCE(num_diagnoses,  1)                     AS num_diagnoses,
        CAST(has_diabetes AS INTEGER)                   AS has_diabetes,
        CAST(has_chf      AS INTEGER)                   AS has_chf,
        CAST(has_copd     AS INTEGER)                   AS has_copd,
        CAST(has_ckd      AS INTEGER)                   AS has_ckd,
        CAST(has_cancer   AS INTEGER)                   AS has_cancer,
        CAST(has_dementia AS INTEGER)                   AS has_dementia,
        GREATEST(0, charlson_index)                     AS charlson_index,
        COALESCE(prior_visits_12m, 0)                   AS prior_visits_12m,
        CAST(readmitted_30d AS INTEGER)                 AS readmitted_30d,
        CASE
            WHEN charlson_index = 0          THEN 'LOW'
            WHEN charlson_index BETWEEN 1 AND 2 THEN 'MEDIUM'
            WHEN charlson_index BETWEEN 3 AND 4 THEN 'HIGH'
            ELSE 'VERY_HIGH'
        END                                             AS risk_tier,
        ROUND(0.983 * EXP(charlson_index * 0.9), 4)   AS ten_yr_survival_prob,
        MD5(patient_id || CAST(admission_date AS VARCHAR)) AS admission_key,
        CURRENT_TIMESTAMP                               AS transformed_at
    FROM bronze_admissions
    WHERE patient_id  IS NOT NULL
      AND admission_date IS NOT NULL
      AND los_days BETWEEN 0 AND 365
"""

df_silver = con.execute(silver_sql).df()
df_silver.to_parquet('/content/lakehouse/silver/admissions_clean.parquet',
                     index=False)

print(f"\n SILVER LAYER")
print(f"   Table: silver.admissions_clean")
print(f"   Rows: {len(df_silver):,} (cleaned & validated)")
print(f"   Null patient_ids removed: {df_raw.patient_id.isna().sum()}")
print(f"   Risk tier distribution:")
print(df_silver['risk_tier'].value_counts().to_string(header=False))