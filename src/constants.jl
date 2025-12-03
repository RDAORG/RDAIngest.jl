# Value types
const RDA_TYPE_INTEGER = 1
const RDA_TYPE_FLOAT = 2
const RDA_TYPE_STRING = 3
const RDA_TYPE_DATE = 4
const RDA_TYPE_DATETIME = 5
const RDA_TYPE_TIME = 6
const RDA_TYPE_CATEGORY = 7

"""
    initvalue_types()

Add default value types
"""
initvalue_types() = DataFrame([(value_type_id=RDA_TYPE_INTEGER, value_type="Integer", description=""),
    (value_type_id=RDA_TYPE_FLOAT, value_type="Float", description=""),
    (value_type_id=RDA_TYPE_STRING, value_type="String", description=""),
    (value_type_id=RDA_TYPE_DATE, value_type="Date", description="ISO Date yyyy-mm-dd"),
    (value_type_id=RDA_TYPE_DATETIME, value_type="Datetime", description="ISO Datetime yyyy-mm-ddTHH:mm:ss.sss"),
    (value_type_id=RDA_TYPE_TIME, value_type="Time", description="ISO Time HH:mm:ss.sss"),
    (value_type_id=RDA_TYPE_CATEGORY, value_type="Categorical", description="Category represented by a Vocabulary with integer value and string code, stored as Integer")
])


# Transformation types
const RDA_TRANSFORMATION_TYPE_INGEST = 1
const RDA_TRANSFORMATION_TYPE_TRANSFORM = 2

"""
    inittypes()

Default transformation types
"""
inittypes() = DataFrame([(transformation_type_id=RDA_TRANSFORMATION_TYPE_INGEST, name="Raw data ingest"),
    (transformation_type_id=RDA_TRANSFORMATION_TYPE_TRANSFORM, name="Dataset transform")])

# Transformation statuses
const RDA_TRANSFORMATION_STATUS_UNVERIFIED = 1
const RDA_TRANSFORMATION_STATUS_VERIFIED = 2

"""
    initstatuses()

Default transformation statuses
"""
initstatuses() = DataFrame([(transformation_status_id=RDA_TRANSFORMATION_STATUS_UNVERIFIED, name="Unverified"),
    (transformation_status_id=RDA_TRANSFORMATION_STATUS_VERIFIED, name="Verified")])


# Unit of analysis
const RDA_UNIT_OF_ANALYSIS_INDIVIDUAL = 1
const RDA_UNIT_OF_ANALYSIS_AGGREGATION = 2

"""
    initunitanalysis()

Default unit of analysis
"""
initunitanalysis() = DataFrame([(unit_of_analysis_id=RDA_UNIT_OF_ANALYSIS_INDIVIDUAL, name="Individual"),
    (unit_of_analysis_id=RDA_UNIT_OF_ANALYSIS_AGGREGATION, name="Aggregation")])

# Study types
const RDA_STUDY_TYPE_DSS = 1
const RDA_STUDY_TYPE_COHORT = 2
const RDA_STUDY_TYPE_SURVEY = 3
const RDA_STUDY_TYPE_PANEL = 4

"""
    initstudytypes()

Default transformation types
"""
initstudytypes() = DataFrame([(study_type_id=RDA_STUDY_TYPE_DSS, name="Demographic Surveillance"),
    (study_type_id=RDA_STUDY_TYPE_COHORT, name="Cohort study"),
    (study_type_id=RDA_STUDY_TYPE_SURVEY, name="Cross-sectional survey"),
    (study_type_id=RDA_STUDY_TYPE_PANEL, name="Panel data")])
