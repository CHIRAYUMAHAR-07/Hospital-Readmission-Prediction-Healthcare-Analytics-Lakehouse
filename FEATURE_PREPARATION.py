df_ml = df_gold.copy()

le = LabelEncoder()
df_ml['gender_enc']     = le.fit_transform(df_ml['gender'])
df_ml['risk_tier_enc']  = le.fit_transform(df_ml['risk_tier'])
df_ml['age_bucket_enc'] = le.fit_transform(df_ml['age_bucket'])

FEATURES = [
    'age', 'gender_enc', 'age_bucket_enc',

    'los_days', 'num_procedures', 'num_diagnoses',
    'has_diabetes', 'has_chf', 'has_copd', 'has_ckd', 'has_cancer', 'has_dementia',
    'charlson_index', 'max_charlson_ever', 'ten_yr_survival_prob',
    
    'prior_visits_12m', 'visits_prior_90d', 'visits_prior_365d',
    'avg_los_last_3_visits', 'cumulative_procedures', 'days_since_last_admit',
    
    'admit_month', 'admit_dow', 'season_code', 'is_weekend_admit', 'visit_number',
    
    'los_x_comorbidity', 'procedures_per_day',
    'cardio_burden', 'metabolic_burden'
]

X = df_ml[FEATURES].fillna(0)
y = df_ml['readmitted_30d']

print(f" Dataset Summary:")
print(f"   Total samples  : {len(X):,}")
print(f"   Features       : {len(FEATURES)}")
print(f"   Readmissions   : {y.sum():,} ({y.mean():.1%}) — class imbalance")
print(f"   Non-readmit    : {(y==0).sum():,} ({(y==0).mean():.1%})")