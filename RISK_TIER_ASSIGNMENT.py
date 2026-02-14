df_test_results = X_test.copy()
df_test_results['risk_score'] = y_prob
df_test_results['actual']     = y_test.values
df_test_results['risk_tier']  = pd.cut(
    y_prob,
    bins=[0, 0.20, 0.40, 0.65, 1.0],
    labels=['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
)

tier_stats = df_test_results.groupby('risk_tier', observed=True).agg(
    patients=('risk_score', 'count'),
    avg_risk_score=('risk_score', 'mean'),
    actual_readmit_rate=('actual', 'mean')
).reset_index()

print("🏥 Patient Risk Tier Distribution (for Ward Dashboard):")
print(f"{'Tier':<12} {'Patients':>10} {'Avg Risk Score':>15} {'Actual Readmit %':>17}")
print("-" * 57)
for _, row in tier_stats.iterrows():
    print(f"{row['risk_tier']:<12} {row['patients']:>10,} {row['avg_risk_score']:>15.3f} {row['actual_readmit_rate']:>16.1%}")

df_test_results.to_parquet('/content/lakehouse/ml/patient_risk_scores.parquet', index=False)
print(f"\n Risk scores saved to lakehouse/ml/patient_risk_scores.parquet")
print(f"   Total patients scored: {len(df_test_results):,}")