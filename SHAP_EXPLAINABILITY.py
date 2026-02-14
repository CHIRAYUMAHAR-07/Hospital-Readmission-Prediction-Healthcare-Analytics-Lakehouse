print(" Computing SHAP values (TreeExplainer)...")

X_sample = X_test.sample(n=min(3000, len(X_test)), random_state=42)

explainer   = shap.TreeExplainer(model)
shap_values = explainer.shap_values(X_sample)

plt.figure(figsize=(12, 8))
shap.summary_plot(shap_values, X_sample, feature_names=FEATURES,
                  show=False, max_display=20)
plt.title('SHAP Feature Impact on Readmission Prediction\n(Each dot = one patient)',
          fontsize=14, fontweight='bold', color=BLUE, pad=15)
plt.tight_layout()
plt.savefig('/content/lakehouse/ml/shap_beeswarm.png', dpi=150, bbox_inches='tight')
plt.show()
print(" SHAP beeswarm plot saved")

shap_df = pd.DataFrame({
    'Feature':    FEATURES,
    'Mean_SHAP':  np.abs(shap_values).mean(axis=0)
}).sort_values('Mean_SHAP', ascending=True).tail(15)

fig, ax = plt.subplots(figsize=(10, 7), facecolor='#F8FAFC')
colors = [LBLUE if v > shap_df['Mean_SHAP'].median() else '#A8C4E0' for v in shap_df['Mean_SHAP']]
bars = ax.barh(shap_df['Feature'], shap_df['Mean_SHAP'], color=colors, edgecolor='white', height=0.7)
ax.set_xlabel('Mean |SHAP Value| — Feature Importance', fontsize=11)
ax.set_title('Top 15 Features Driving Hospital Readmission\n(SHAP Global Importance)',
             fontsize=13, fontweight='bold', color=BLUE)
for bar, val in zip(bars, shap_df['Mean_SHAP']):
    ax.text(val + 0.0002, bar.get_y() + bar.get_height()/2,
            f'{val:.4f}', va='center', fontsize=9, color=GRAY)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
plt.tight_layout()
plt.savefig('/content/lakehouse/ml/shap_importance.png', dpi=150, bbox_inches='tight')
plt.show()

print(f"\n🏆 Top 5 Readmission Predictors:")
for i, row in shap_df.tail(5).sort_values('Mean_SHAP', ascending=False).iterrows():
    print(f"   {row['Feature']:30s} SHAP: {row['Mean_SHAP']:.4f}")