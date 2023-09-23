using ConfigEnv
using RDAIngest
using DBInterface

#get environment variables
dotenv()


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

println("SQL Server")
db = opendatabase(ENV["RDA_SERVER"], "RDA", sqlite = false)
try
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 4, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 5, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 8, ENV["DATA_INGEST_PATH"])
finally
    DBInterface.close!(db)
end