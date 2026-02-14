mlflow.set_tracking_uri('/content/mlruns')
mlflow.set_experiment('hospital_readmission_prediction')

with mlflow.start_run(run_name='logistic_regression_baseline'):
    mlflow.log_param('model_type',       'LogisticRegression')
    mlflow.log_param('smote_applied',    False)
    mlflow.log_param('n_features',       len(FEATURES))
    mlflow.log_param('train_size',       len(X_train))
    mlflow.log_metric('roc_auc',         baseline_auc)
    mlflow.log_metric('train_rows',      len(X_train))
    mlflow.log_metric('test_rows',       len(X_test))
    baseline_run_id = mlflow.active_run().info.run_id

with mlflow.start_run(run_name='xgboost_smote_v2_champion'):
    
    mlflow.log_params(xgb_params)
    mlflow.log_param('smote_applied',        True)
    mlflow.log_param('smote_k_neighbors',    5)
    mlflow.log_param('n_features',           len(FEATURES))
    mlflow.log_param('train_size_smote',     len(X_resampled))

    ap_score    = average_precision_score(y_test, y_prob)
    y_pred      = (y_prob >= 0.40).astype(int)
    report      = classification_report(y_test, y_pred, output_dict=True)

    mlflow.log_metric('roc_auc',             xgb_auc)
    mlflow.log_metric('avg_precision',       ap_score)
    mlflow.log_metric('precision_readmit',   report['1']['precision'])
    mlflow.log_metric('recall_readmit',      report['1']['recall'])
    mlflow.log_metric('f1_readmit',          report['1']['f1-score'])
    mlflow.log_metric('baseline_auc',        baseline_auc)
    mlflow.log_metric('improvement',         xgb_auc - baseline_auc)

    mlflow.log_artifact('/content/lakehouse/ml/shap_importance.png')

    mlflow.xgboost.log_model(model, 'xgb_readmission_model',
                              registered_model_name='readmission_champion')
    xgb_run_id = mlflow.active_run().info.run_id

print(" MLflow Experiment Comparison:")
print(f"{'Run':<40} {'ROC-AUC':>10} {'SMOTE':>8}")
print("-" * 60)
print(f"{'logistic_regression_baseline':<40} {baseline_auc:>10.4f} {'No':>8}")
print(f"{'xgboost_smote_v2_champion ← BEST':<40} {xgb_auc:>10.4f} {'Yes':>8}")
print(f"\n Model registered in MLflow Model Registry as 'readmission_champion'")
print(f"   Run ID: {xgb_run_id}")
print(f"   Experiment: hospital_readmission_prediction")
print(f"\n In production: launch 'mlflow ui' to see the full dashboard")