import pandas as pd
from os import getcwd

# Load files
pbj = pd.read_csv(getcwd() + '/PBJ_Daily_Nurse_Staffing_Q2_2024.csv', encoding='cp1252',dtype={'PROVNUM':'str'})
prov = pd.read_csv(getcwd() + '/NH_ProviderInfo_Oct2024.csv')
vbp = pd.read_csv(getcwd() + '/FY_2024_SNF_VBP_Facility_Performance.csv')
penalties = pd.read_csv(getcwd() + '/NH_Penalties_Oct2024.csv')
print(penalties.columns)
# print("Files loaded successfully")
# print(f"PBJ shape: {pbj.shape}")
# print(f"ProviderInfo shape: {prov.shape}")
# print(f"VBP shape: {vbp.shape}")
# print(f"Penalties shape: {penalties.shape}")

# print("=== PBJ BASIC PROFILE ===")
# print(f"\nShape: {pbj.shape}")
# print(f"\nDtypes:\n{pbj.dtypes}")
# print(f"\nDate range of WorkDate: {pbj['WorkDate'].min()} to {pbj['WorkDate'].max()}")
# print(f"\nUnique facilities: {pbj['PROVNUM'].nunique()}")
# print(f"Unique states: {pbj['STATE'].nunique()}")
# print(f"\nCY_Qtr values:\n{pbj['CY_Qtr'].value_counts()}")

# print("=== PBJ NULL CHECK ===")
# null_counts = pbj.isnull().sum()
# null_pct = (pbj.isnull().sum() / len(pbj) * 100).round(2)
# null_df = pd.DataFrame({
#     'Null Count': null_counts,
#     'Null %': null_pct
# }).query('`Null Count` > 0').sort_values('Null %', ascending=False)
# print(null_df)

# print("=== MDScensus DISTRIBUTION ===")
# print(f"\nBasic stats:\n{pbj['MDScensus'].describe()}")
# print(f"\nZero values: {(pbj['MDScensus'] == 0).sum()}")
# print(f"Negative values: {(pbj['MDScensus'] < 0).sum()}")
# print(f"Null values: {pbj['MDScensus'].isnull().sum()}")
# print(f"\nRows where MDScensus = 0 as % of total: {(pbj['MDScensus'] == 0).mean()*100:.2f}%")

# print("=== STAFFING HOURS DISTRIBUTION ===")
# hrs_cols = [c for c in pbj.columns if c.startswith('Hrs_') 
#             and not c.endswith('_emp') 
#             and not c.endswith('_ctr')]

# for col in hrs_cols:
#     print(f"\n{col}:")
#     print(f"  Min: {pbj[col].min():.2f}  Max: {pbj[col].max():.2f}  "
#           f"Mean: {pbj[col].mean():.2f}  Zeros: {(pbj[col]==0).sum()}  "
#           f"Negatives: {(pbj[col]<0).sum()}  Nulls: {pbj[col].isnull().sum()}")
    
# print("=== LPN OUTLIER INVESTIGATION ===")
# outlier = pbj[pbj['Hrs_LPN'] > 5000]
# print(f"Rows with Hrs_LPN > 5000: {len(outlier)}")
# print(outlier[['PROVNUM', 'PROVNAME', 'STATE', 'WorkDate', 
#                'MDScensus', 'Hrs_LPN', 'Hrs_LPN_emp', 'Hrs_LPN_ctr']])

# # Also check overall LPN distribution above 99th percentile
# p99 = pbj['Hrs_LPN'].quantile(0.99)
# p999 = pbj['Hrs_LPN'].quantile(0.999)
# print(f"\n99th percentile Hrs_LPN: {p99:.2f}")
# print(f"99.9th percentile Hrs_LPN: {p999:.2f}")
# print(f"Rows above 99.9th percentile: {(pbj['Hrs_LPN'] > p999).sum()}")

# print("=== TOP END LPN HOURS INVESTIGATION ===")

# # Exclude the known bad row
# pbj_clean = pbj[pbj['Hrs_LPN'] < 5000].copy()

# # Look at rows above 99.9th percentile
# p999 = pbj_clean['Hrs_LPN'].quantile(0.999)
# high_lpn = pbj_clean[pbj_clean['Hrs_LPN'] > p999].copy()

# # Check if high hours correlate with high census (legitimate large facilities)
# high_lpn['LPN_per_resident'] = high_lpn['Hrs_LPN'] / high_lpn['MDScensus']

# print(f"Rows above 99.9th percentile (excl. bad row): {len(high_lpn)}")
# print(f"\nCensus distribution for these rows:")
# print(high_lpn['MDScensus'].describe())
# print(f"\nLPN hours per resident distribution:")
# print(high_lpn['LPN_per_resident'].describe())
# print(f"\nTop 10 highest LPN hours per resident (most suspicious):")
# print(high_lpn.nlargest(10, 'LPN_per_resident')[
#     ['PROVNUM','PROVNAME','STATE','WorkDate',
#      'MDScensus','Hrs_LPN','LPN_per_resident']
# ])

# print("=== PROVNUM FORMAT CHECK ===")
# print(f"\nPBJ PROVNUM dtype: {pbj['PROVNUM'].dtype}")
# print(f"Sample values:\n{pbj['PROVNUM'].head(10).tolist()}")
# print(f"Values shorter than 6 chars: {(pbj['PROVNUM'].astype(str).str.len() < 6).sum()}")

# print(f"\nProviderInfo CCN dtype: {prov['CMS Certification Number (CCN)'].dtype}")
# print(f"Sample values:\n{prov['CMS Certification Number (CCN)'].head(10).tolist()}")
# print(f"Values shorter than 6 chars: {(prov['CMS Certification Number (CCN)'].astype(str).str.len() < 6).sum()}")

# print("=== JOIN COVERAGE ===")

# # Normalize both keys
# pbj_ids = set(pbj['PROVNUM'].astype(str).str.zfill(6).unique())
# prov_ids = set(prov['CMS Certification Number (CCN)'].astype(str).str.zfill(6).unique())
# vbp_ids = set(vbp['CMS Certification Number (CCN)'].astype(str).str.zfill(6).unique())
# pen_ids = set(penalties['CMS Certification Number (CCN)'].astype(str).str.zfill(6).unique())

# print(f"\nPBJ unique facilities: {len(pbj_ids)}")
# print(f"ProviderInfo unique facilities: {len(prov_ids)}")
# print(f"VBP unique facilities: {len(vbp_ids)}")
# print(f"Penalties unique facilities: {len(pen_ids)}")

# print(f"\nPBJ facilities matched in ProviderInfo: {len(pbj_ids & prov_ids)} "
#       f"({len(pbj_ids & prov_ids)/len(pbj_ids)*100:.1f}%)")
# print(f"PBJ facilities matched in VBP: {len(pbj_ids & vbp_ids)} "
#       f"({len(pbj_ids & vbp_ids)/len(pbj_ids)*100:.1f}%)")
# print(f"PBJ facilities matched in Penalties: {len(pbj_ids & pen_ids)} "
#       f"({len(pbj_ids & pen_ids)/len(pbj_ids)*100:.1f}%)")
# print(f"\nPBJ facilities NOT in ProviderInfo: {len(pbj_ids - prov_ids)}")
# print(f"PBJ facilities NOT in VBP: {len(pbj_ids - vbp_ids)}")

# print("=== PROVIDERINFO KEY COLUMNS ===")
# key_cols = [
#     'Number of Certified Beds',
#     'Average Number of Residents per Day',
#     'Provider Type',
#     'Overall Rating',
#     'Staffing Rating',
#     'QM Rating',
#     'Total nursing staff turnover',
#     'Registered Nurse turnover',
#     'Total Amount of Fines in Dollars',
#     'Total Number of Penalties'
# ]

# for col in key_cols:
#     nulls = prov[col].isnull().sum()
#     null_pct = nulls / len(prov) * 100
#     print(f"{col}: {nulls} nulls ({null_pct:.1f}%)")
#     if prov[col].dtype in ['float64', 'int64']:
#         print(f"  Min: {prov[col].min():.2f}  Max: {prov[col].max():.2f}  "
#               f"Mean: {prov[col].mean():.2f}")

# print("=== VBP KEY COLUMNS ===")
# vbp_key_cols = [
#     'Performance Period: FY 2022 Risk-Standardized Readmission Rate',
#     'Baseline Period: FY 2019 Risk-Standardized Readmission Rate',
#     'Achievement Score',
#     'Performance Score',
#     'SNF VBP Program Ranking'
# ]

# for col in vbp_key_cols:
#     nulls = vbp[col].isnull().sum()
#     null_pct = nulls / len(vbp) * 100
#     print(f"\n{col}:")
#     print(f"  Nulls: {nulls} ({null_pct:.1f}%)")
#     print(f"  Dtype: {vbp[col].dtype}")
#     if vbp[col].dtype in ['float64','int64']:
#         print(f"  Min: {vbp[col].min():.4f}  Max: {vbp[col].max():.4f}  "
#               f"Mean: {vbp[col].mean():.4f}")

# # Check footnote columns for suppressed values
# fn_cols = [c for c in vbp.columns if 'Footnote' in c]
# print(f"\nFootnote columns value counts:")
# for col in fn_cols:
#     vc = vbp[col].value_counts(dropna=False)
#     if len(vc) > 0:
#         print(f"\n{col}:\n{vc}")

# print("=== BASELINE RSRR DTYPE FIX ===")
# print(f"Non-numeric values in Baseline RSRR:")
# baseline_col = 'Baseline Period: FY 2019 Risk-Standardized Readmission Rate'
# non_numeric = vbp[pd.to_numeric(vbp[baseline_col], errors='coerce').isnull()]
# print(f"Count: {len(non_numeric)}")
# print(non_numeric[baseline_col].value_counts())

# print("=== PENALTIES PROFILE ===")
# print(f"\nShape: {penalties.shape}")
# print(f"Unique facilities with penalties: {penalties['CMS Certification Number (CCN)'].nunique()}")
# print(f"\nPenalty Type breakdown:\n{penalties['Penalty Type'].value_counts()}")
# print(f"\nFine Amount stats:")
# fines = penalties[penalties['Penalty Type'] == 'Fine']['Fine Amount']
# print(f"  Count: {fines.count()}")
# print(f"  Min: {fines.min()}  Max: {fines.max()}  Mean: {fines.mean():.2f}")
# print(f"  Nulls in Fine Amount: {penalties['Fine Amount'].isnull().sum()}")
# print(f"  Nulls in Payment Denial Length: {penalties['Payment Denial Length in Days'].isnull().sum()}")
# print(f"\nPenalties per facility distribution:")
# print(penalties.groupby('CMS Certification Number (CCN)').size().describe())