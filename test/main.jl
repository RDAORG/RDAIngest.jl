# Set working directory
cd("/Users/chu.282/Dropbox/OSU/RDA_private/RDAORG/RDAIngest.jl")

using Pkg
using ConfigEnv
using Logging
using DBInterface
using DataFrames
using Dates
using CSV
using SQLite
using XLSX

using RDAIngest

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

#"""
# Test SQLite
#"""

#"""
#CREATE RDA FROM SCRATCH
#"""

@time createdatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], replace=true, sqlite=true)
db = opendatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"]; sqlite=true)

#"""
#INGEST CHAMPS DATA
#"""

@info "Ingesting CHAMPS data"
t = now()

source = CHAMPSSource()
ingest = CHAMPSIngest()

@info "Ingesting CHAMPS source"
flush(io)
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], ENV["ISO3_PATH"], sqlite=true)

@info "Ingesting CHAMPS dictionaries"
flush(io)
@time ingest_dictionary(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], sqlite=true)

@info "Ingesting CHAMPS deaths"
flush(io)
@time ingestion_id = ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)

@info "Ingesting CHAMPS datasets"
flush(io)
@time ingest_data(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=true)

d = now() - t
@info "===== Ingesting CHAMPS into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)


#"""
#INGEST COMSA MZ DATA
#"""

@info "Ingesting COMSA MZ data"
t = now()

source = COMSAMZSource()
ingest = COMSAMZIngest()

@info "Ingesting COMSA MZ source"
flush(io)
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], ENV["ISO3_PATH"], sqlite=true)

@info "Ingesting COMSA MZ dictionaries"
flush(io)
@time ingest_dictionary(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], sqlite=true)

@info "Ingesting COMSA MZ deaths"
flush(io)
@time ingestion_id = ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)

@info "Ingesting COMSA MZ datasets"
flush(io)
@time ingest_data(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=true)

d = now() - t
@info "===== Ingesting COMSA MZ into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)


#"""
#INGEST HEALSL DATA
#"""
# #datasets = filter(f -> endswith(f, ".csv"), readdir(joinpath(ENV["DATA_INGEST_PATH"],source_name,"De_identified_data"); join=false))
# datasets = ["healsl_rd1_adult_v1.csv","healsl_rd1_child_v1.csv","healsl_rd1_neo_v1.csv",
#             "healsl_rd2_adult_v1.csv", "healsl_rd2_child_v1.csv", "healsl_rd2_neo_v1.csv"]

# # Add age group and round prefix to id, to avoid duplicated ID between rounds and modules
# source_name = "HEALSL"
# id_col = "rowid"

# @info "Fix duplicated id in HEALSL" # Last update 2025-05-30 

# sites = String[]

# for i in 1:length(datasets)

#     site_col = ifelse(occursin.("adult", datasets[i]), "province_cod", "province")

#     file = joinpath(ENV["DATA_INGEST_PATH"],source_name,"De_identified_data",datasets[i])
#     data = CSV.File(file) |> DataFrame

#     # Duplicated ID for different rounds, add dataset name as prefix to ensure unique id for source
#     pref = splitext(datasets[i])[1]
#     data[!,Symbol(id_col)] =  "$pref-" .* string.(data[!,Symbol(id_col)])

#     # Get full list of sites for ingestion
#     append!(sites, coalesce.(string.(data[!, Symbol(site_col)]), ""))
    
#     # Save as version for ingest
#     file = joinpath(ENV["DATA_INGEST_PATH"],source_name,"De_identified_data",splitext(datasets[i])[1] * "_ingest.csv")
#     CSV.write(file, data)

# end

# file = joinpath(ENV["DATA_INGEST_PATH"],source_name,"De_identified_data", "all_sites.csv")
# sites = DataFrame(province = unique(sites))
# CSV.write(file, sites)

# # Fix variable dictionaries 
# include("./test/prep_variables.jl")


@info "Ingesting HEALSL data"
t = now()

source = HEALSLSource(
    site_data = "all_sites.csv",
    site_col = "province"
)

@info "Ingesting HEALSL source"
flush(io)
@time ingest_source(source, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"], ENV["ISO3_PATH"], sqlite=true)

@info "Ingest deaths from multiple age groups and rounds from HEALSL"

datasets = filter(f -> endswith(f, "_ingest.csv"), readdir(joinpath(ENV["DATA_INGEST_PATH"],source.name,"De_identified_data"); join=false))

for i in 1:length(datasets)
    df_name = splitext(datasets[i])[1]
    instrument = ifelse(occursin.("_neo", df_name), "Neonate_eVA_Questionnaire-SL.pdf", 
                 ifelse(occursin.("_child", df_name), "Child_eVA_Questionnaire-SL.pdf", 
                 "Adult_eVA_Questionnaire-SL.pdf"))

    ingest = HEALSLIngest(
                source = source, 
                site_col = ifelse(occursin.("adult", df_name), "province_cod", "province"),
                death_file = datasets[i],
                datasets =  Dict("HEALSL deid verbal autopsy - " * replace.(df_name, "_" => " ") => datasets[i]),
                datadictionaries= [replace.(datasets[i], "_ingest.csv" => "_datadict.xlsx")], #[df_name * "_datadict.xlsx"],
                instruments = Dict(instrument => df_name) #update instrument - dataset mapping
                )

    @info "Ingesting HEALSL dictionaries"
    flush(io)
    @time ingest_dictionary(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_DICTIONARY_PATH"], sqlite=true)

    @info "Ingesting HEALSL deaths"
    flush(io)
    if i==1
        @time ingestion_id = ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)
    else
        @time ingest_deaths(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; sqlite=true)
    end

    @info "Ingesting HEALSL datasets"
    flush(io)
    @time ingest_data(ingest, ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"], ENV["DATA_INGEST_PATH"]; ingestion_id=ingestion_id, sqlite=true)

end

@info "Update metadata - instrument collection_cycle"
updatevalue(db, "instruments", "name", "collection_cycle", "Adult_eVA_Questionnaire-SL.pdf", "R1, R2")
updatevalue(db, "instruments", "name", "collection_cycle", "Neonate_eVA_Questionnaire-SL.pdf", "R1, R2")
updatevalue(db, "instruments", "name", "collection_cycle", "Child_eVA_Questionnaire-SL.pdf", "R1, R2")

d = now() - t
@info "===== Ingesting HEALSL into sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)




#"""
# Update metadata
#"""
t = now()

@info "Update NADA metadata for CHAMPS"

code_path = "/Users/chu.282/Dropbox/OSU/RDA_private/RDAORG/RDAIngest.jl/src/main.jl"
repository_ddi_path = "/Users/chu.282/Dropbox/OSU/RDA_private/RDAORG/RDANada.jl/test/Info_CHAMPS/Info_CHAMPS.xml"
repository_rdf_path = "/Users/chu.282/Dropbox/OSU/RDA_private/RDAORG/RDANada.jl/test/Info_CHAMPS/Info_CHAMPS.rdf"

datasets = ["CHAMPS_deid_verbal_autopsy","CHAMPS_deid_decode_results","CHAMPS_deid_basic_demographics",
            "CHAMPS_deid_tac_results","CHAMPS_deid_lab_results"]

for i in 1:length(datasets)
    meta = nadaMeta(dataset_name = datasets[i], 
                    doi = "https://doi.org/10.82901/Info_CHAMPS",
                    repository_id = "Info_CHAMPS",
                    repository_ddi_id = "Info_CHAMPS",
                    repository_ddi_path = repository_ddi_path,
                    repository_rdf_path = repository_rdf_path,
                    code_path = code_path,
                    transformation_status_id = 2
                    )

    updateMeta(db, meta)
end

d = now() - t
@info "===== Update CHAMPS metadata completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)


db = opendatabase(ENV["RDA_DATABASE_PATH"], ENV["RDA_DBNAME"]; sqlite=true)
@info "Ingest user data product into RDA sqlite with meta data"

submission_folder = "/Users/chu.282/Dropbox/OSU/RDA_private/RDAORG/RDANada.jl/test/"
dataset_path = "SCI_CHAMPS/SCI_CHAMPS.csv" #relative path in submission folder
dictionary_path = "SCI_CHAMPS/SCI_CHAMPS_dictionary2.csv"
code_path = "SCI_CHAMPS.R"

ingest = userIngest(
    id_col = "cod",
    domain_name = "BADWOLF",
    domain_description = "Domain for demo",

    submission_folder = submission_folder,

    dataset_path = dataset_path,
    dataset_name = "Demo - SCI",
    dataset_desc = "Symptom-cause Information matrix based on CHAMPS DeCoDe results and verbal autospy in InSilicoVA format.",
    dictionary_path = dictionary_path,

    # Metadata for dataset
    doi = "doi placeholder",
    repository_id = "Demo1",
    
    # Metadata for ingestion 
    ingest_desc = "Ingestion of user generated product - SCI for CHAMPS.",
    ingest_date = today(),

    # Metadata for transformation
    input_datasets = [1,5], #id of datasets used as input for producing data product
    transform_desc = "The dataset crosstabulates cause of death in DeCoDe and symptoms in verbal autopsy",
    code_reference = code_path,
    author = "John Smith",

    # Metadata file
    repository_ddi_id = "ddi_id",
    repository_ddi_path = "SCI_CHAMPS/SCI_CHAMPS.xml",
    repository_rdf_path = "SCI_CHAMPS/SCI_CHAMPS.rdf",

    # CSV format
    delim = ','
)
    



#"""
# Test SQL Server
#"""

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

