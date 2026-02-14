context = gx.get_context()

datasource = context.sources.add_or_update_pandas(name="ehr_lakehouse")
data_asset = datasource.add_dataframe_asset(name="bronze_admissions")
batch_request = data_asset.build_batch_request(dataframe=df_raw)

suite_name = "bronze_ehr_quality_suite"
try:
    context.delete_expectation_suite(suite_name)
except:
    pass
context.add_expectation_suite(suite_name)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name
)

rules_run = 0

for col in ['patient_id','admission_date','age','gender','los_days',
            'num_procedures','num_diagnoses','charlson_index',
            'prior_visits_12m','readmitted_30d']:
    validator.expect_column_values_to_not_be_null(col)
    rules_run += 1

validator.expect_column_values_to_be_between("age",            0,   120)
validator.expect_column_values_to_be_between("los_days",       0,   365)
validator.expect_column_values_to_be_between("charlson_index", 0,    37)
validator.expect_column_values_to_be_between("num_procedures", 0,    50)
validator.expect_column_values_to_be_between("num_diagnoses",  1,    50)
validator.expect_column_values_to_be_between("prior_visits_12m", 0,  30)
validator.expect_column_values_to_be_between("admit_month",    1,    12)
validator.expect_column_values_to_be_between("admit_dow",      0,     6)
validator.expect_column_values_to_be_between("has_diabetes",   0,     1)
validator.expect_column_values_to_be_between("has_chf",        0,     1)
validator.expect_column_values_to_be_between("has_copd",       0,     1)
validator.expect_column_values_to_be_between("has_ckd",        0,     1)
validator.expect_column_values_to_be_between("has_cancer",     0,     1)
validator.expect_column_values_to_be_between("has_dementia",   0,     1)
rules_run += 14

validator.expect_column_values_to_be_in_set("gender",         ['M','F'])
validator.expect_column_values_to_be_in_set("readmitted_30d", [0, 1])
validator.expect_column_values_to_be_in_set("age_bucket",     ['18-39','40-59','60-74','75+'])
validator.expect_column_values_to_be_in_set("admit_season",   ['WINTER','SPRING','SUMMER','FALL'])
rules_run += 4

validator.expect_column_mean_to_be_between("readmitted_30d",  0.05, 0.45)
validator.expect_column_mean_to_be_between("age",             40,   75)
validator.expect_column_stdev_to_be_between("age",            5,    25)
validator.expect_column_mean_to_be_between("los_days",        1,    15)
validator.expect_column_mean_to_be_between("charlson_index",  0,     5)
validator.expect_column_mean_to_be_between("num_procedures",  0,     8)
rules_run += 6

validator.expect_column_values_to_be_unique("patient_id", mostly=0.98)
validator.expect_table_row_count_to_be_between(50_000, 200_000)
validator.expect_table_column_count_to_equal(len(df_raw.columns))
validator.expect_column_value_lengths_to_be_between("patient_id", 10, 15)
rules_run += 4

validator.expect_column_values_to_match_regex("patient_id",       r'^PAT-\d{7}$')
validator.expect_column_values_to_match_regex("admission_date",   r'^\d{4}-\d{2}-\d{2}$')
validator.expect_column_values_to_not_match_regex("patient_id",   r'\s')
validator.expect_column_values_to_not_be_null("age_bucket")
rules_run += 4

validator.expect_column_proportion_of_unique_values_to_be_between("risk_tier",    0.01, 0.50)
validator.expect_column_proportion_of_unique_values_to_be_between("admit_season", 0.01, 0.50)
validator.expect_column_min_to_be_between("los_days",       0,  3)
validator.expect_column_max_to_be_between("charlson_index", 5, 37)
validator.expect_column_sum_to_be_between("has_chf",        100, 50000)
validator.expect_column_sum_to_be_between("has_diabetes",   500, 80000)
validator.expect_column_pair_cramers_phi_value_to_be_less_than("has_chf", "has_ckd", 0.9)
validator.expect_select_column_values_to_be_unique_within_record(
    column_list=['patient_id','admission_date'])
rules_run += 8

validator.save_expectation_suite()

result = validator.validate()

passed  = result['statistics']['successful_expectations']
total   = result['statistics']['evaluated_expectations']
pct     = result['statistics']['success_percent']

print(f"\n GREAT EXPECTATIONS — Data Quality Report")
print(f"   Rules Evaluated : {total}")
print(f"   Rules Passed    : {passed}")
print(f"   Rules Failed    : {total - passed}")
print(f"   Success Rate    : {pct:.1f}%")
print(f"\n   ✓ Completeness checks (10 rules)")
print(f"   ✓ Domain validity  (14 rules)")
print(f"   ✓ Categorical      ( 4 rules)")
print(f"   ✓ Statistical      ( 6 rules)")
print(f"   ✓ Uniqueness       ( 4 rules)")
print(f"   ✓ Format           ( 4 rules)")
print(f"   ✓ Advanced         ( 8 rules)")