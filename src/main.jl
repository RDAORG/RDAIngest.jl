using ConfigEnv
using RDAIngest
using DBInterface
using DataFrames
using Dates
using BenchmarkTools

#get environment variables
dotenv()

@time createdatabase(ENV["RDA_DATABASE_PATH"], "RDA", replace=true)
@time ingest_champs(ENV["RDA_DATABASE_PATH"], "RDA", ENV["DATA_INGEST_PATH"], 
              "CHAMPS Level2 Data V4.10",
              "Ingest of CHAMPS de-identified data", "ingest_champs in RDAIngest.jl",
              "Kobus Herbst",
              "Raw data from CHAMPS level 2 release, categorical variables not encoded using vocabularies",
              ENV["DATA_DICTIONARY_PATH"])

