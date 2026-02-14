X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.20, stratify=y, random_state=42
)

baseline = LogisticRegression(max_iter=1000, random_state=42)
baseline.fit(X_train, y_train)
baseline_auc = roc_auc_score(y_test, baseline.predict_proba(X_test)[:, 1])

print(f" Baseline (Logistic Regression) ROC-AUC: {baseline_auc:.4f}")

print(f"\n⚖️  Applying SMOTE to fix class imbalance...")
print(f"   Before: {dict(pd.Series(y_train).value_counts())}")

smote = SMOTE(random_state=42, k_neighbors=5)
X_resampled, y_resampled = smote.fit_resample(X_train, y_train)

print(f"   After:  {dict(pd.Series(y_resampled).value_counts())}")
print(f"   New training size: {len(X_resampled):,}")

print(f"\n Training XGBoost...")

xgb_params = {
    'n_estimators':     500,
    'max_depth':          6,
    'learning_rate':    0.05,
    'subsample':        0.80,
    'colsample_bytree': 0.80,
    'min_child_weight':   3,
    'gamma':            0.1,
    'reg_alpha':        0.1,
    'reg_lambda':       1.0,
    'eval_metric':     'auc',
    'random_state':      42,
    'n_jobs':            -1
}

model = XGBClassifier(**xgb_params)
model.fit(
    X_resampled, y_resampled,
    eval_set=[(X_test, y_test)],
    verbose=100
)

y_prob = model.predict_proba(X_test)[:, 1]
xgb_auc = roc_auc_score(y_test, y_prob)

print(f"\n{'='*50}")
print(f"  Baseline ROC-AUC : {baseline_auc:.4f}")
print(f"  XGBoost ROC-AUC  : {xgb_auc:.4f}  ← Resume claim: 0.89")
print(f"  Improvement      : +{xgb_auc - baseline_auc:.4f}")
print(f"{'='*50}")