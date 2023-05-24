using RDAIngest
using ConfigEnv

dotenv()
#pwd()

"""
Ingest COMSA Data - ingest_comsa()

1. A COMSA source is created.
2. The COMSA sites (provincia) are extracted from VA dataset and added to site_iso_code column, 
    "Mozambique" is added as the country name prefix in name column. 
3. Protocols are not added in this version.!!!
4. Data ingest, transformation are created.
6. A entry for each death is inserted in the deaths table, for each row of VA using ingest_comsa_deaths function.
7. The COMSA dataset variables are imported from data dictionary file created by R (Create_dictionaries.R) 
    from publicly released data dictionary, in the add_comsa_variables function.
8. The COMSA VA datasets are imported using the import_comsa_dataset function.
9. The COMSA deaths are linked to the dataset rows containing the detail data about each death in
the COMSA data distribution, using the function link_deathrows.
"""

ingest_comsa(ENV["RDA_DATABASE_PATH"], 
              "RDA", #"COMSA", 
              ENV["DATA_INGEST_PATH"], 
              "COMSA Level2 Data Version 20230308",
              "Ingest of COMSA de-identified VA data", "ingest_comsa - testing",
              "Yue Chu",
              "Raw VA data from COMSA level 2 20230308 release",
              ENV["DATA_DICTIONARY_PATH"])
