# TEST OUTPUT DATASETS

using ConfigEnv
using RDAIngest
using DBInterface
using Logging
#get environment variables
dotenv()

#region Setup Logging
l = open("log.log", "a+")
io = IOContext(l, :displaysize => (100, 100))
logger = SimpleLogger(io)
old_logger = global_logger(logger)
@info "Output dataset execution started $(Dates.format(now(), "yyyy-mm-dd HH:MM"))"
flush(io)
#endregion

t = now()
@info "============================== Using sqlite database: $(ENV["RDA_DATABASE_PATH"])"

db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")
try
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 4, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 5, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 6, ENV["DATA_INGEST_PATH"])
finally
    DBInterface.close!(db)
end
d = now() - t
@info "===== Outputting datasets from sqlite completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)

println("SQL Server")
t = now()
@info "============================== Using SQL Server database: $(ENV["RDA_SERVER"])"
db = opendatabase(ENV["RDA_SERVER"], "RDA", sqlite=false)
try
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 4, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 5, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 6, ENV["DATA_INGEST_PATH"])
finally
    DBInterface.close!(db)
end
d = now() - t
@info "===== Outputting datasets from SQL Server completed $(Dates.format(now(), "yyyy-mm-dd HH:MM")) duration $(round(d, Dates.Second))"
flush(io)
#region clean up
global_logger(old_logger)
close(io)
#endregion


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


