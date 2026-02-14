# ─── INSTALL ALL REQUIRED PACKAGES ───────────────────────────────────────────
!pip install -q xgboost shap imbalanced-learn great_expectations mlflow \
               pyiceberg plotly kaleido faker duckdb pandas numpy \
               scikit-learn matplotlib seaborn pyarrow

print("All packages installed successfully!")