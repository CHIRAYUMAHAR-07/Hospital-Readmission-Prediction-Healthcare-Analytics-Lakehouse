
fig = plt.figure(figsize=(20, 14), facecolor='#0D1117')
fig.suptitle('🏥 Hospital Readmission Prediction — Ward Analytics Dashboard',
             fontsize=18, fontweight='bold', color='white', y=0.98)

gs = fig.add_gridspec(3, 4, hspace=0.45, wspace=0.35,
                       left=0.06, right=0.97, top=0.93, bottom=0.06)

kpi_style  = dict(facecolor='#161B22')
plot_style = dict(facecolor='#161B22')

kpi_data = [
    ('ROC-AUC', f'{xgb_auc:.3f}', '↑ vs 0.71 baseline', '#00D4FF'),
    ('Readmit Rate', f'{y.mean():.1%}',  'Across 100K records', '#FF6B35'),
    ('Records', '100,000+', 'EHR admissions', '#00FF87'),
    ('Features', str(len(FEATURES)), 'Engineered from SQL', '#FFD700'),
]
for idx, (title, value, sub, color) in enumerate(kpi_data):
    ax = fig.add_subplot(gs[0, idx], **kpi_style)
    ax.set_facecolor('#161B22')
    ax.axis('off')
    ax.text(0.5, 0.72, value, ha='center', va='center', fontsize=26,
            fontweight='bold', color=color, transform=ax.transAxes)
    ax.text(0.5, 0.42, title, ha='center', va='center', fontsize=12,
            color='white', transform=ax.transAxes)
    ax.text(0.5, 0.18, sub, ha='center', va='center', fontsize=9,
            color='#888888', transform=ax.transAxes)
    for spine in ax.spines.values():
        spine.set_edgecolor(color)
        spine.set_linewidth(2)
    ax.set_visible(True)

ax1 = fig.add_subplot(gs[1, :2], **plot_style)
ax1.set_facecolor('#161B22')
fpr_b, tpr_b, _ = roc_curve(y_test, baseline.predict_proba(X_test)[:,1])
fpr_x, tpr_x, _ = roc_curve(y_test, y_prob)
ax1.plot(fpr_b, tpr_b, color='#FF6B35', lw=2, linestyle='--',
         label=f'Logistic Regression (AUC={baseline_auc:.3f})')
ax1.plot(fpr_x, tpr_x, color='#00D4FF', lw=2.5,
         label=f'XGBoost + SMOTE  (AUC={xgb_auc:.3f})')
ax1.plot([0,1],[0,1], 'gray', lw=1, linestyle=':')
ax1.fill_between(fpr_x, tpr_x, alpha=0.08, color='#00D4FF')
ax1.set_xlabel('False Positive Rate', color='#AAAAAA', fontsize=10)
ax1.set_ylabel('True Positive Rate', color='#AAAAAA', fontsize=10)
ax1.set_title('ROC Curve — Baseline vs XGBoost', color='white', fontsize=12, fontweight='bold')
ax1.legend(fontsize=9, facecolor='#1E1E2E', labelcolor='white')
ax1.tick_params(colors='#AAAAAA')
ax1.spines[:].set_color('#333333')

ax2 = fig.add_subplot(gs[1, 2], **plot_style)
ax2.set_facecolor('#161B22')
tier_counts = df_test_results['risk_tier'].value_counts().sort_index()
tier_colors = ['#00FF87','#FFD700','#FF6B35','#FF0000']
bars = ax2.bar(tier_counts.index, tier_counts.values, color=tier_colors, edgecolor='#333333')
for bar in bars:
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
             f'{bar.get_height():,}', ha='center', fontsize=9, color='white')
ax2.set_title('Patient Risk Tiers', color='white', fontsize=12, fontweight='bold')
ax2.set_ylabel('Patients', color='#AAAAAA')
ax2.tick_params(colors='#AAAAAA')
ax2.spines[:].set_color('#333333')

ax3 = fig.add_subplot(gs[1, 3], **plot_style)
ax3.set_facecolor('#161B22')
top_features = shap_df.tail(8)
ax3.barh(top_features['Feature'], top_features['Mean_SHAP'],
         color='#00D4FF', alpha=0.85, edgecolor='#333333')
ax3.set_title('Top SHAP Features', color='white', fontsize=12, fontweight='bold')
ax3.set_xlabel('Mean |SHAP|', color='#AAAAAA', fontsize=9)
ax3.tick_params(colors='#AAAAAA', labelsize=8)
ax3.spines[:].set_color('#333333')

ax4 = fig.add_subplot(gs[2, 0], **plot_style)
ax4.set_facecolor('#161B22')
age_readmit = df_gold.groupby('age_bucket')['readmitted_30d'].mean().sort_index()
bars = ax4.bar(age_readmit.index, age_readmit.values * 100,
               color=['#00FF87','#FFD700','#FF6B35','#FF0000'], edgecolor='#333333')
ax4.set_title('Readmission Rate by Age', color='white', fontsize=11, fontweight='bold')
ax4.set_ylabel('Readmission %', color='#AAAAAA')
ax4.tick_params(colors='#AAAAAA', labelsize=9)
ax4.spines[:].set_color('#333333')

ax5 = fig.add_subplot(gs[2, 1], **plot_style)
ax5.set_facecolor('#161B22')
y_pred_thresh = (y_prob >= 0.40).astype(int)
cm = confusion_matrix(y_test, y_pred_thresh)
im = ax5.imshow(cm, cmap='Blues', aspect='auto')
ax5.set_xticks([0,1]); ax5.set_yticks([0,1])
ax5.set_xticklabels(['No Readmit','Readmit'], color='white', fontsize=9)
ax5.set_yticklabels(['No Readmit','Readmit'], color='white', fontsize=9)
for i in range(2):
    for j in range(2):
        ax5.text(j, i, f'{cm[i,j]:,}', ha='center', va='center',
                 fontsize=14, fontweight='bold',
                 color='white' if cm[i,j] < cm.max()/2 else 'black')
ax5.set_title('Confusion Matrix\n(threshold=0.40)', color='white', fontsize=11, fontweight='bold')

ax6 = fig.add_subplot(gs[2, 2:], **plot_style)
ax6.set_facecolor('#161B22')
tier_readmit = df_gold.groupby('charlson_index')['readmitted_30d'].agg(['mean','count']).reset_index()
tier_readmit = tier_readmit[tier_readmit['count'] > 500].head(8)
ax6.bar(tier_readmit['charlson_index'], tier_readmit['mean']*100,
        color='#00D4FF', alpha=0.85, edgecolor='#333333')
ax6.plot(tier_readmit['charlson_index'], tier_readmit['mean']*100,
         'o-', color='#FFD700', lw=2, ms=6)
ax6.set_title('Readmission Rate by Charlson Comorbidity Index\n(Key clinical predictor)',
              color='white', fontsize=11, fontweight='bold')
ax6.set_xlabel('Charlson Comorbidity Index', color='#AAAAAA')
ax6.set_ylabel('Readmission Rate (%)', color='#AAAAAA')
ax6.tick_params(colors='#AAAAAA')
ax6.spines[:].set_color('#333333')

plt.savefig('/content/lakehouse/ml/ward_dashboard.png', dpi=150,
            bbox_inches='tight', facecolor='#0D1117')
plt.show()
print(" Ward Dashboard saved to /content/lakehouse/ml/ward_dashboard.png")