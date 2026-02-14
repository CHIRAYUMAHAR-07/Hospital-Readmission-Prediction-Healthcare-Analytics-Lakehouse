import pandas as pd
import numpy as np
import warnings
import os
import json
import sqlite3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from faker import Faker
import random
from datetime import datetime, timedelta

from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import (roc_auc_score, classification_report,
                              confusion_matrix, roc_curve, precision_recall_curve,
                              average_precision_score)
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
from imblearn.over_sampling import SMOTE
import shap
import mlflow
import mlflow.xgboost

import great_expectations as gx

warnings.filterwarnings('ignore')
np.random.seed(42)
random.seed(42)

BLUE   = '#1F4E79'
LBLUE  = '#2E75B6'
TEAL   = '#00B0F0'
GREEN  = '#1A6B3C'
ORANGE = '#C55A11'
RED    = '#C00000'
GRAY   = '#595959'

plt.rcParams.update({
    'figure.facecolor': '#F8FAFC',
    'axes.facecolor':   '#F8FAFC',
    'axes.grid':        True,
    'grid.alpha':       0.3,
    'font.family':      'DejaVu Sans'
})

print(" All imports successful!")
print(f"   pandas {pd.__version__} | numpy {np.__version__} | xgboost ready | shap ready")