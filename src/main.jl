using Pkg
using RDAIngest
using ConfigEnv
using Logging
using DBInterface
using DataFrames
using Dates
using CSV
using SQLite
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

# t = now()
# #ENV["RDA_DBNAME"] = "RDA" #Don't use global variables
# @info "===================== Using SQL Server database on server: $(ENV["RDA_SERVER"])"
# @info "Ingesting CHAMPS data"
# @info "Creating database"
# @time createdatabase(ENV["RDA_SERVER"], ENV["RDA_DBNAME"], replace=true, sqlite=false)
# @info "Ingesting CHAMPS source"
# @time ingest_source(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite=false)
# flush(io)
# @time ingest_dictionary(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], ENV["DATA_INGEST_PATH"], sqlite=false)
# @info "Ingested CHAMPS dictionaries"
# flush(io)
# @time ingestion_id = ingest_deaths(CHAMPSIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=false)
# @info "Ingested CHAMPS deaths"
# flush(io)
# @time ingest_data(CHAMPSIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=false)
# @info "Ingested CHAMPS datasets"
# flush(io)
# d = now() - t
# @info "===== Ingesting CHAMPS into SQL Server completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
# flush(io)

#"""
#INGEST COMSA Mozambique DATA
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

# t = now()
# @info "===================== Using SQL Server database on server: $(ENV["RDA_SERVER"])"
# @info "Ingesting COMSA data"
# @time ingest_source(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], sqlite=false)
# @info "Ingested COMSA source"
# flush(io)
# @time ingest_dictionary(source, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"],
#     ENV["DATA_INGEST_PATH"], sqlite=false)
# @info "Ingested COMSA dictionaries"
# flush(io)
# @time ingestion_id = ingest_deaths(COMSAIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=false)
# @info "Ingested COMSA deaths"
# flush(io)
# @time ingest_data(COMSAIngest, ENV["RDA_SERVER"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=false)
# @info "Ingested COMSA datasets"
# d = now() - t
# @info "===== Ingesting COMSA into SQL Server completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
# flush(io)



#"""
#INGEST HEALSL (COMSA Sierra Leone) DATA
#"""
t = now()
@info "============================== Using sqlite database: $(ENV["RDA_DATABASE_PATH"])"

@info "Combine VA datasets"
# df1 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd1_who_adult_v1.csv", DataFrame)
# df2 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd2_who_adult_v1.csv", DataFrame)
# df3 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd3_who_adult_v1.csv", DataFrame)

# df4 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd1_who_child_v1.csv", DataFrame)
# df5 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd2_who_child_v1.csv", DataFrame)
# df6 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd3_who_child_v1.csv", DataFrame)

# df7 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd1_who_neo_v1.csv", DataFrame)
# df8 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd2_who_neo_v1.csv", DataFrame)
# df9 = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_rd3_who_neo_v1.csv", DataFrame)

# df = rbind([df1,df2,df3,df4,df5,df6,df7,df8,df9])
# CSV.write("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_all_v1.csv", df)

df = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/healsl_all_v1.csv", DataFrame)


@info "Format dictionary for RDA"
# raw_dict = CSV.read("/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/Data/HEALSL/De_identified_data/ddict_healsl.csv", DataFrame)

# # raw_dict = combine(groupby(raw_dict, [:column_name, :description, :type]), 
# #                    :data_name => (x -> join(x, " & ")) => :Note)
# raw_dict.Note = fill("", nrow(raw_dict))

# # Fix Data Type errors
# fix = ["id10011", "id10481"]
# #map!(a -> in(a, fix) ? "timestamp with time zone" : a, raw_dict.type, raw_dict.column_name)
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="timestamp with time zone" 

# fix = raw_dict[occursin.(r"(?i)record the date", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="date" #.& in.(raw_dict.column_name, Ref(duplicates))

# fix = ["id10021","id10023_a"]
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="date" 

# fix = raw_dict[occursin.(r"(?i)calculated number", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)what was the weight", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)how old", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)how many", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision" 

# fix = raw_dict[occursin.(r"(?i)how long", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="double precision"

# fix = raw_dict[occursin.(r"(?i)_unit", raw_dict.description), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="text"

# fix = raw_dict[occursin.(r"(?i)_unit", raw_dict.column_name), :].column_name
# raw_dict[in.(raw_dict.column_name,Ref(fix)), :type] .="text"

# raw_dict[occursin.(r"(?i)when ", raw_dict.description), :]

# raw_dict = unique(raw_dict,[:column_name, :description, :type])

# # Multiple datatype provided in the data dictionary for the same variable
# duplicates = filter(r -> count(x -> x == r.column_name, raw_dict.column_name) > 1, eachrow(raw_dict)).column_name
# check = DataFrame(filter(row -> row.column_name in duplicates, eachrow(raw_dict)))
# println(sort(check,:column_name))
# raw_dict[in.(raw_dict.column_name,Ref(duplicates)), :type] .="text"

# # Remove duplicates
# raw_dict = unique(raw_dict,[:column_name, :description, :type])

# # Map datatype to RDA datatypes 
# unique(raw_dict.type)
# value_type_mapping = Dict("integer" => 1, "double precision" => 2, "text" => 3,"date"=>4,
#                           "timestamp with time zone"=>5, "time"=>6, "category"=>7) 
#                           #type 3, 6, 7 doesn't exist in current version (v1), listed as placeholders
# raw_dict.DataType = map(x -> get(value_type_mapping, x, missing), raw_dict.type)

# # Flag key variable
# raw_dict.Key = map(x -> x == "rowid" ? "Yes" : "", raw_dict.column_name)

# # Format dictionary file and save a copy
# rename!(raw_dict, :column_name => :Column_Name)
# rename!(raw_dict, :description => :Description)

# raw_dict.Description = replace.(raw_dict.Description, "," => "^")

# new_dict = select(raw_dict, :Column_Name, :Key, :Description, :Note, :DataType)
# new_dict = DataFrame("Column_Name;Key;Description;Note;DataType" => [join(map(x -> string(x), row), ";") for row in eachrow(new_dict)])

# file = "/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDA/Data Dictionaries/HEALSL/Format_ddict_healsl.csv"
# CSV.write(file, new_dict; delim=';', quotechar='"', decimal='.')
# # Needs one last step of manual replacing ; in csv, otherwise CSV.File has trouble recognizing the delim.

# db_path = "/Users/chu.282/Library/CloudStorage/OneDrive-Personal/RDA/RDAIngest.jl/database/RDA.sqlite"
# db = SQLite.DB(db_path)

@info "Ingesting HEALSL data"
#Step 1: Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 
source = HEALSLSource()
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"])
@info "Ingested HEALSL source"
flush(io)
# Step 2: Ingest data dictionaries, add variables and vocabularies
@time ingest_dictionary(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"],
    ENV["DATA_INGEST_PATH"], sqlite=true)
@info "Ingested HEALSL dictionaries"
flush(io)
# Step 3: Ingest deaths to deathrows, return transformation_id and ingestion_id
HEALSLIngest = Ingest(source=source,
    death_file="healsl_all_v1",
    datasets=Dict("HEALSL deid verbal autopsy" => "healsl_all_v1"),
    datainstruments=Dict("Adult_eVA_Questionnaire-SL.pdf" => "healsl_all_v1",
        "Child_eVA_Questionnaire-SL.pdf" => "healsl_all_v1",
        "Neonate_eVA_Questionnaire-SL.pdf" => "healsl_all_v1"),
    ingest_desc="Ingest raw HEALSL Level-2 Data accessed 20240228",
    transform_desc="Ingest of HEALSL Level-2 Data",
    code_reference="RDAIngest.ingest_data",
    author="YUE CHU"
)
@time ingestion_id_sqlite = ingest_deaths(HEALSLIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)
@info "Ingested HEALSL deaths"
flush(io)
# Step 4: Import datasets, and link datasets to deaths
@time ingest_data(HEALSLIngest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id_sqlite, sqlite=true)
@info "Ingested HEALSL datasets"
d = now() - t
@info "===== Ingesting HEALSL into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)

#region clean up
global_logger(old_logger)
close(io)
#endregion



# # Update CHAMPS variable data type

# source = CHAMPSSource()
# dictionarypath = ENV["DATA_DICTIONARY_PATH"]
# filename = "Format_CHAMPS_deid_verbal_autopsy"

# file = joinpath(dictionarypath,source.name, "$(filename).csv") #default as .csv file
# raw_dict = CSV.File(file; delim=";", quotechar=source.quotechar,
#     dateformat=source.dateformat, decimal=source.decimal) |> DataFrame

# # Fix Data Type errors
# fix = raw_dict[occursin.(r"(?i)record the date", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.DataType,Ref(fix)), :DataType] .=4 #.& in.(raw_dict.Column_Name, Ref(duplicates))

# fix = raw_dict[occursin.(r"(?i)the weight", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)how old", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)how many", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)how long", raw_dict.Description), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=2

# fix = raw_dict[occursin.(r"(?i)_unit", raw_dict.Column_Name), :].Column_Name
# raw_dict[in.(raw_dict.Column_Name,Ref(fix)), :DataType] .=7

# CSV.write(file, raw_dict; delim=";")
