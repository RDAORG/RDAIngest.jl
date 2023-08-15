using RDAIngest
using ConfigEnv

using DBInterface
using DataFrames
using Dates
#using BenchmarkTools

#get environment variables
dotenv()

dbname = "RDA"
@time createdatabase(ENV["RDA_DATABASE_PATH"], dbname, replace=true)


"""
INGEST CHAMPS DATA
"""
#Step 1: Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 
source = CHAMPSSource()
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_INGEST_PATH"])


# Step 2: Ingest data dictionaries, add variables and vocabularies
CHAMPSdict = AbstractDictionary(
    domain_name="CHAMPS",
    dictionaries=["Format_CHAMPS_deid_basic_demographics", 
    "Format_CHAMPS_deid_verbal_autopsy", 
    "Format_CHAMPS_deid_decode_results",
    "Format_CHAMPS_deid_tac_results", 
    "Format_CHAMPS_deid_lab_results"],
    id_col = "champs_deid", site_col = "site_iso_code"
    )

@time ingest_dictionary(CHAMPSdict, ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_DICTIONARY_PATH"])

# Step 3: Ingest deaths to deathrows, return transformation_id and ingestion_id
CHAMPSIngest = Ingest(source_name = "CHAMPS",
                datafolder = "De_identified_data",
                death_file = "CHAMPS_deid_basic_demographics",
                death_idcol = "champs_deid",
                site_col = "site_iso_code",
                datasets = Dict("CHAMPS deid basic demographics" => "CHAMPS_deid_basic_demographics", 
                "CHAMPS deid verbal autopsy" => "CHAMPS_deid_verbal_autopsy", 
                "CHAMPS deid decode results" => "CHAMPS_deid_decode_results",
                "CHAMPS deid tac results" => "CHAMPS_deid_tac_results", 
                "CHAMPS deid lab results" => "CHAMPS_deid_lab_results"),
                delim = ',',
                quotechar = '"',
                dateformat = "yyyy-mm-dd",
                decimal = '.',
                ingest_desc = "Ingest raw CHAMPS Level-2 Data accessed 20230518",
                transform_desc = "Ingest of CHAMPS Level-2 Data",
                code_reference = "multiple dispatch testing",
                author = "YC"
                )
                
meta_info = ingest_deaths(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_INGEST_PATH"])

# Step 4: Import datasets, and link datasets to deaths

@time ingest_datasets(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_INGEST_PATH"],
                meta_info["transformation_id"], meta_info["ingestion_id"])



"""
INGEST COMSA DATA
"""
#Step 1: Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 
source = COMSASource()
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_INGEST_PATH"])


# Step 2: Ingest data dictionaries, add variables and vocabularies
COMSAdict = AbstractDictionary(domain_name="COMSA",dictionaries=["Format_Comsa_WHO_VA_20230308"],
                         id_col = "comsa_id", site_col = "provincia")
@time ingest_dictionary(COMSAdict, ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_DICTIONARY_PATH"])


# Step 3: Ingest deaths to deathrows, return transformation_id and ingestion_id
COMSAIngest2 = Ingest(source_name = "COMSA",
                datafolder = "De_identified_data",
                death_file = "Comsa_WHO_VA_20230308",
                death_idcol = "comsa_id",
                site_col = "provincia",
                datasets = Dict("COMSA deid verbal autopsy" => "Comsa_WHO_VA_20230308"),
                delim = ',',
                quotechar = '"',
                dateformat = "dd-u-yyyy", #"mmm dd, yyyy"
                decimal = '.',
                ingest_desc = "Ingest raw COMSA Level-2 Data accessed 20230518",
                transform_desc = "Ingest of COMSA Level-2 Data",
                code_reference = "multiple dispatch testing",
                author = "YC"
                )
                
meta_info = ingest_deaths(COMSAIngest2,ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_INGEST_PATH"])

# Step 4: Import datasets, and link datasets to deaths

@time ingest_datasets(COMSAIngest2, ENV["RDA_DATABASE_PATH"], dbname, ENV["DATA_INGEST_PATH"],
                meta_info["transformation_id"], meta_info["ingestion_id"])


