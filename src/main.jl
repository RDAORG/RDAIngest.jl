using RDAIngest
using ConfigEnv
using Logging
using DBInterface
using DataFrames
using Dates
#using BenchmarkTools

#get environment variables
dotenv()
#region Setup Logging
l = open("log.log", "a+")
io = IOContext(l, :displaysize => (100, 100))
logger = SimpleLogger(io)
old_logger = global_logger(logger)
@info "Execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
flush(io)
#endregion

t = now()

@info "============================== Using sqlite database: $(ENV["RDA_DATABASE_PATH"])"
@info "Creating database"
@time createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true, sqlite=true)
flush(io)

#"""
#INGEST CHAMPS DATA
#"""

@info "Ingesting CHAMPS data"
#Step 1: Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 
source = CHAMPSSource()
@info "Ingesting CHAMPS source"
flush(io)
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite=true)
#
# Step 2: Ingest data dictionaries, add variables and vocabularies, including TAC results with multi-gene
@info "Ingesting CHAMPS dictionaries"
flush(io)
@time ingest_dictionary(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"],
    ENV["DATA_INGEST_PATH"], sqlite=true)
#
# Step 3: Ingest deaths to deathrows, return transformation_id and ingestion_id
CHAMPSIngest = Ingest(source=source,
    death_file="CHAMPS_deid_basic_demographics",
    datasets=Dict("CHAMPS deid basic demographics" => "CHAMPS_deid_basic_demographics",
        "CHAMPS deid verbal autopsy" => "CHAMPS_deid_verbal_autopsy",
        "CHAMPS deid decode results" => "CHAMPS_deid_decode_results",
        "CHAMPS deid tac results" => "CHAMPS_deid_tac_results",
        "CHAMPS deid lab results" => "CHAMPS_deid_lab_results"
    ),
    datainstruments=Dict("cdc_93759_DS9.pdf" => "CHAMPS_deid_verbal_autopsy"),
    ingest_desc="Raw CHAMPS Level-2 Data accessed 20230518",
    transform_desc="Ingest of CHAMPS Level-2 Data",
    code_reference="RDAIngest.ingest_data",
    author="Kobus Herbst; YUE CHU"
)
@info "Ingesting CHAMPS deaths"
flush(io)
@time ingestion_id_sqlite = ingest_deaths(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)
#
# Step 4: Import datasets, and link datasets to deaths
@info "Ingesting CHAMPS datasets"
flush(io)
@time ingest_data(CHAMPSIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id_sqlite, sqlite=true)
d = now() - t
@info "===== Ingesting CHAMPS into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)

#= check data
check = dataset_to_dataframe(db, get_namedkey(db, "datasets", "CHAMPS_deid_tac_results", "dataset_id"))
check = dataset_to_dataframe(db, get_namedkey(db, "datasets", "CHAMPS_deid_lab_results", "dataset_id"))

check = DBInterface.execute(db, "SELECT * FROM death_rows") |> DataFrame
nrow(check)
check = DBInterface.execute(db, "SELECT dataset_id, COUNT(*) As n FROM datarows GROUP BY dataset_id") |> DataFrame
print(check)
=#

t = now()
#ENV["RDA_DBNAME"] = "RDA" #Don't use global variables
@info "===================== Using SQL Server database on server: $(ENV["RDA_SERVER"])"
@info "Ingesting CHAMPS data"
@info "Creating database"
@time createdatabase(ENV["RDA_SERVER"], ENV["RDA_DBNAME"], replace=true, sqlite=false)
@info "Ingesting CHAMPS source"
@time ingest_source(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite=false)
flush(io)
@time ingest_dictionary(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], ENV["DATA_INGEST_PATH"], sqlite=false)
@info "Ingested CHAMPS dictionaries"
flush(io)
@time ingestion_id = ingest_deaths(CHAMPSIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=false)
@info "Ingested CHAMPS deaths"
flush(io)
@time ingest_data(CHAMPSIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=false)
@info "Ingested CHAMPS datasets"
flush(io)
d = now() - t
@info "===== Ingesting CHAMPS into SQL Server completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)

#"""
#INGEST COMSA DATA
#"""
t = now()
@info "============================== Using sqlite database: $(ENV["RDA_DATABASE_PATH"])"
@info "Ingesting COMSA data"
#Step 1: Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 
source = COMSASource()
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])
@info "Ingested COMSA source"
flush(io)
# Step 2: Ingest data dictionaries, add variables and vocabularies
@time ingest_dictionary(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"],
    ENV["DATA_INGEST_PATH"], sqlite=true)
@info "Ingested COMSA dictionaries"
flush(io)
# Step 3: Ingest deaths to deathrows, return transformation_id and ingestion_id
COMSAIngest = Ingest(source=source,
    death_file="Comsa_WHO_VA_20230308",
    datasets=Dict("COMSA deid verbal autopsy" => "Comsa_WHO_VA_20230308"),
    datainstruments=Dict("5a_2018_COMSA_VASA_ADULTS-EnglishOnly_01262019_clean.pdf" => "Comsa_WHO_VA_20230308",
        "5a_2018_COMSA_VASA_CHILD-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308",
        "5a_2018_COMSA_VASA_SB_NN-EnglishOnly_12152018Clean.pdf" => "Comsa_WHO_VA_20230308",
        "5a_2018_COMSA_VASA-GenInfo_English_06272018_clean.pdf" => "Comsa_WHO_VA_20230308"),
    ingest_desc="Ingest raw COMSA Level-2 Data accessed 20230518",
    transform_desc="Ingest of COMSA Level-2 Data",
    code_reference="RDAIngest.ingest_data",
    author="Kobus Herbst; YUE CHU"
)
@time ingestion_id_sqlite = ingest_deaths(COMSAIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)
@info "Ingested COMSA deaths"
flush(io)
# Step 4: Import datasets, and link datasets to deaths
@time ingest_data(COMSAIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id_sqlite, sqlite=true)
@info "Ingested COMSA datasets"
d = now() - t
@info "===== Ingesting COMSA into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)

t = now()
@info "===================== Using SQL Server database on server: $(ENV["RDA_SERVER"])"
@info "Ingesting COMSA data"
@time ingest_source(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite=false)
@info "Ingested COMSA source"
flush(io)
@time ingest_dictionary(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"],
    ENV["DATA_INGEST_PATH"], sqlite=false)
@info "Ingested COMSA dictionaries"
flush(io)
@time ingestion_id = ingest_deaths(COMSAIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=false)
@info "Ingested COMSA deaths"
flush(io)
@time ingest_data(COMSAIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=false)
@info "Ingested COMSA datasets"
d = now() - t
@info "===== Ingesting COMSA into SQL Server completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)

#region clean up
global_logger(old_logger)
close(io)
#endregion

