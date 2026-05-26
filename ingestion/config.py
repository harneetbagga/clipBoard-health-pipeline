# ingestion/config.py
#
# Central registry of all CMS source files.
# This is the only place you need to update when CMS adds a new file
# or changes a filename.
#
# criticality:
#   'critical'  — pipeline halts if this file fails (PBJ has all staffing data)
#   'important' — pipeline warns but continues (monthly refreshes)
#   'optional'  — pipeline continues silently (annual refreshes)

FILE_REGISTRY = [
    {
        "name":          "pbj_staffing",
        "drive_filename": "PBJ_Daily_Nurse_Staffing_Q2_2024.csv",
        "s3_prefix":     "bronze/pbj_staffing",
        "raw_table":     "HEALTHCARE_METRICS.RAW.RAW_PBJ_STAFFING",
        "criticality":   "critical",
        "description":   "Daily nurse staffing hours by facility — Q2 2024",
    },
    {
        "name":           "provider_info",
        "drive_filename": "NH_ProviderInfo_Oct2024.csv",
        "s3_prefix":      "bronze/provider_info",
        "raw_table":      "HEALTHCARE_METRICS.RAW.RAW_PROVIDER_INFO",
        "criticality":    "important",
        "description":    "Facility ratings, beds, provider type — monthly refresh",
    },
    {
        "name":           "snf_vbp",
        "drive_filename": "FY_2024_SNF_VBP_Facility_Performance.csv",
        "s3_prefix":      "bronze/snf_vbp",
        "raw_table":      "HEALTHCARE_METRICS.RAW.RAW_SNF_VBP",
        "criticality":    "optional",
        "description":    "VBP readmission scores — annual refresh",
    },
    {
        "name":           "penalties",
        "drive_filename": "NH_Penalties_Oct2024.csv",
        "s3_prefix":      "bronze/penalties",
        "raw_table":      "HEALTHCARE_METRICS.RAW.RAW_PENALTIES",
        "criticality":    "optional",
        "description":    "Fines and payment denials — monthly refresh",
    },
]

# Files that must succeed for the pipeline to proceed to dbt
CRITICAL_FILES = {
    f["name"] for f in FILE_REGISTRY
    if f["criticality"] == "critical"
}