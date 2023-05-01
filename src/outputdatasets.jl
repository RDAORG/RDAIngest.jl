using ConfigEnv
using RDAIngest

#get environment variables
dotenv()


db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")
try
    @time dataset_to_arrow(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_arrow(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_arrow(db, 3, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"])
finally
    close(db)
end