using RDAIngest
using ConfigEnv

using DBInterface
using DataFrames
using Dates
#using BenchmarkTools

#get environment variables
dotenv()

#ENV["RDA_DBNAME"] = "RDA" #Don't use global variables
# Use datbasesetup.jl to create the database
@time createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true, sqlite = true)
@time createdatabase(ENV["RDA_SERVER"], ENV["RDA_DBNAME"], replace=true, sqlite = false)


"""
INGEST CHAMPS DATA
"""
#Step 1: Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 
source = CHAMPSSource()
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite = true)
@time ingest_source(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite = false)

#
# Step 2: Ingest data dictionaries, add variables and vocabularies
CHAMPSdict = AbstractDictionary(
    domain_name="CHAMPS",
    domain_description = "Raw CHAMPS level-2 deidentified data",
    dictionaries=["Format_CHAMPS_deid_basic_demographics", 
    "Format_CHAMPS_deid_verbal_autopsy", 
    "Format_CHAMPS_deid_decode_results",
    "Format_CHAMPS_deid_tac_results", 
    "Format_CHAMPS_deid_lab_results"],
    id_col = "champs_deid", site_col = "site_iso_code"
    )

@time ingest_dictionary(CHAMPSdict, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"])
@time ingest_dictionary(CHAMPSdict, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], sqlite = false)

#=
# For CHAMPS, add vocabularies for TAC results with multi-gene
@time ingest_voc_CHAMPSMITS(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], 
                            ENV["DATA_INGEST_PATH"], "CHAMPS", "De_identified_data", 
                            "CHAMPS_deid_tac_vocabulary.xlsx")
                            

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
                "CHAMPS deid lab results" => "CHAMPS_deid_lab_results"
                ),
                delim = ',',
                quotechar = '"',
                dateformat = "yyyy-mm-dd",
                decimal = '.',
                datainstruments = Dict("cdc_93759_DS9.pdf" => "CHAMPS_deid_verbal_autopsy"),
                ingest_desc = "Raw CHAMPS Level-2 Data accessed 20230518",
                transform_desc = "Ingest of CHAMPS Level-2 Data",
                code_reference = "Multiple dispatch testing",
                author = "YUE CHU"
                )
                
meta_info = ingest_deaths(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])

# Step 4: Import datasets, and link datasets to deaths

@time ingest_data(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"],
                meta_info["transformation_id"], meta_info["ingestion_id"])

#=
# Step 4.1: Test adding more datasets later without ingesting deaths first
CHAMPSIngest1 = Ingest(source_name = "CHAMPS",
                datafolder = "De_identified_data",
                death_file = "CHAMPS_deid_basic_demographics",
                death_idcol = "champs_deid",
                site_col = "site_iso_code",
                datasets = Dict("CHAMPS deid tac results" => "CHAMPS_deid_tac_results", 
                "CHAMPS deid lab results" => "CHAMPS_deid_lab_results"),
                datainstruments = Dict("cdc_93759_DS9.pdf" => "CHAMPS_deid_verbal_autopsy"),
                ingest_desc = "Ingest 2",
                transform_desc = "Ingest 2",
                code_reference = "step testing",
                author = "YC"
                )

db = opendatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"])
source_id = get_source(db, CHAMPSIngest1.source_name)
ingestion_id = add_ingestion(db, source_id, today(), CHAMPSIngest1.ingest_desc)
transformation_id = add_transformation(db, 1, 1, CHAMPSIngest1.transform_desc, 
                                        CHAMPSIngest1.code_reference, today(), CHAMPSIngest1.author)
@time ingest_data(CHAMPSIngest1, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"],
                transformation_id,ingestion_id)

# check data
check = dataset_to_dataframe(db, get_namedkey(db, "datasets", "CHAMPS_deid_tac_results", "dataset_id"))
check = dataset_to_dataframe(db, get_namedkey(db, "datasets", "CHAMPS_deid_lab_results", "dataset_id"))

check = DBInterface.execute(db, "SELECT * FROM death_rows") |> DataFrame
nrow(check)
check = DBInterface.execute(db, "SELECT dataset_id, COUNT(*) As n FROM datarows GROUP BY dataset_id") |> DataFrame
print(check)
=#
            

"""
INGEST COMSA DATA
"""
#Step 1: Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 
source = COMSASource()
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])


# Step 2: Ingest data dictionaries, add variables and vocabularies
COMSAdict = AbstractDictionary(domain_name="COMSA",dictionaries=["Format_Comsa_WHO_VA_20230308"],
                         id_col = "comsa_id", site_col = "provincia")
@time ingest_dictionary(COMSAdict, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"])


# Step 3: Ingest deaths to deathrows, return transformation_id and ingestion_id
COMSAIngest = Ingest(source_name = "COMSA",
                datafolder = "De_identified_data",
                death_file = "Comsa_WHO_VA_20230308",
                death_idcol = "comsa_id",
                site_col = "provincia",
                datasets = Dict("COMSA deid verbal autopsy" => "Comsa_WHO_VA_20230308"),
                delim = ',',
                quotechar = '"',
                dateformat = "dd-u-yyyy", #"mmm dd, yyyy"
                decimal = '.',
                datainstruments = Dict("5a_2018_COMSA_VASA_ADULTS-EnglishOnly_01262019_clean.pdf" => "Comsa_WHO_VA_20230308",
                "5a_2018_COMSA_VASA_CHILD-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308",
                "5a_2018_COMSA_VASA_SB_NN-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308",
                "5a_2018_COMSA_VASA-GenInfo_English_06272018_clean.pdf" => "Comsa_WHO_VA_20230308"),
                ingest_desc = "Ingest raw COMSA Level-2 Data accessed 20230518",
                transform_desc = "Ingest of COMSA Level-2 Data",
                code_reference = "multiple dispatch testing",
                author = "YC"
                )
                
meta_info = ingest_deaths(COMSAIngest,ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])

# Step 4: Import datasets, and link datasets to deaths

@time ingest_data(COMSAIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"],
                meta_info["transformation_id"], meta_info["ingestion_id"])

=#