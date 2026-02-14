fake = Faker()
N = 100_000

print(f" Generating {N:,} synthetic EHR records...")

def generate_ehr(n):
    records = []
    start_date = datetime(2021, 1, 1)
    end_date   = datetime(2024, 12, 31)
    date_range = (end_date - start_date).days

    for i in range(n):
        age = int(np.clip(np.random.normal(62, 18), 18, 95))

        has_diabetes = int(random.random() < (0.12 + age * 0.003))
        has_chf      = int(random.random() < (0.07 + age * 0.002))
        has_copd     = int(random.random() < (0.09 + age * 0.002))
        has_ckd      = int(random.random() < (0.08 + age * 0.002))
        has_cancer   = int(random.random() < 0.06)
        has_dementia = int(random.random() < (0.02 + (age > 75) * 0.10))

        cci = (has_diabetes * 1 + has_chf * 2 + has_copd * 1 +
               has_ckd * 2 + has_cancer * 2 + has_dementia * 2)

        los     = max(1, int(np.random.exponential(5)))
        procs   = random.randint(0, 12)
        diags   = random.randint(1, 20)
        prior   = random.randint(0, 8)

        readmit_prob = min(0.90,
            0.05
            + cci      * 0.04
            + (age>70) * 0.07
            + has_chf  * 0.12
            + prior    * 0.02
            + (los>7)  * 0.05
        )
        readmitted = int(random.random() < readmit_prob)

        admit_date = start_date + timedelta(days=random.randint(0, date_range))

        records.append({
            'patient_id':        f'PAT-{i:07d}',
            'admission_date':    admit_date.strftime('%Y-%m-%d'),
            'admit_year':        admit_date.year,
            'admit_month':       admit_date.month,
            'admit_dow':         admit_date.weekday(),
            'admit_season':      ['WINTER','SPRING','SUMMER','FALL'][
                                  [12,1,2,3,4,5,6,7,8,9,10,11].index(
                                   admit_date.month) // 3],
            'age':               age,
            'age_bucket':        ('18-39' if age<40 else
                                  '40-59' if age<60 else
                                  '60-74' if age<75 else '75+'),
            'gender':            random.choice(['M','F']),
            'los_days':          los,
            'num_procedures':    procs,
            'num_diagnoses':     diags,
            'has_diabetes':      has_diabetes,
            'has_chf':           has_chf,
            'has_copd':          has_copd,
            'has_ckd':           has_ckd,
            'has_cancer':        has_cancer,
            'has_dementia':      has_dementia,
            'charlson_index':    cci,
            'prior_visits_12m':  prior,
            'readmitted_30d':    readmitted
        })

    return pd.DataFrame(records)

df_raw = generate_ehr(N)

print(f" Generated {len(df_raw):,} records")
print(f"   Readmission rate: {df_raw.readmitted_30d.mean():.1%}")
print(f"   Age range: {df_raw.age.min()}–{df_raw.age.max()} (mean: {df_raw.age.mean():.1f})")
print(f"   Diabetes prevalence: {df_raw.has_diabetes.mean():.1%}")
print(f"   CHF prevalence: {df_raw.has_chf.mean():.1%}")
df_raw.head(3)