using ConfigEnv
using DBInterface
using DataFrames
using Dates
using BenchmarkTools
using SQLite
#using RDAIngest #just run the codes in RDAIngest.jl 


#get environment variables
dotenv()

#outputdatasets
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


DBInterface.execute(db, "SELECT * FROM value_types";) |> DataFrame

