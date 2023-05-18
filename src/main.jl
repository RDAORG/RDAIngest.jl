using ConfigEnv
using RDAIngest
using DBInterface
using DuckDB

#get environment variables
dotenv()

createdatabase(ENV["RDA_DATABASE_PATH"], "RDA", replace=true, type = "duckdb")
# @time ingest_champs(ENV["RDA_DATABASE_PATH"], "RDA", ENV["DATA_INGEST_PATH"],
#     "CHAMPS Level2 Data V4.10",
#     "Ingest of CHAMPS de-identified data", "ingest_champs in RDAIngest.jl",
#     "Kobus Herbst",
#     "Raw data from CHAMPS level 2 release, categorical variables not encoded using vocabularies",
#     ENV["DATA_DICTIONARY_PATH"])

# db = DuckDB.open(".\\database\\t.duckdb")
# DBInterface.execute(db, "CREATE SEQUENCE seq_source_id START 1;")
# DBInterface.execute(db, """CREATE TABLE "sources" ("source_id" INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_source_id'),"name" TEXT NOT NULL);""")
# close(db)