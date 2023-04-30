using ConfigEnv
using RDAIngest
using DBInterface
using DataFrames
using Dates

#get environment variables
dotenv()

createdatabase(ENV["RDA_DATABASE_PATH"], "RDA", replace=true)

db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")
champs = addsource(db, "CHAMPS")
add_champs_sites(db, ENV["DATA_INGEST_PATH"])
add_champs_protocols(db, ENV["DATA_INGEST_PATH"])
ingest = add_dataingest(db, champs, Date(2023, 4, 1), "CHAMPS Level2 Data V4.10")
transformation = add_transformation(db, 1, 1, "Ingest of CHAMPS de-identified data", "RDAIngest.jl", today(), "Kobus Herbst")
deaths = ingest_champs_deaths(db, ingest, ENV["DATA_INGEST_PATH"])
save_CHAMPS_variables(db, ENV["DATA_INGEST_PATH"], "Format_CHAMPS_deid_basic_demographics")
save_CHAMPS_variables(db, ENV["DATA_INGEST_PATH"], "Format_CHAMPS_deid_verbal_autopsy")
save_CHAMPS_variables(db, ENV["DATA_INGEST_PATH"], "Format_CHAMPS_deid_decode_results")
import_champs_dataset(db, transformation, ingest, ENV["DATA_INGEST_PATH"], "CHAMPS_deid_basic_demographics", "Raw data from CHAMPS level 2 release, categorical variables not encoded using vocabularies")
import_champs_dataset(db, transformation, ingest, ENV["DATA_INGEST_PATH"], "CHAMPS_deid_verbal_autopsy", "Raw data from CHAMPS level 2 release, categorical variables not encoded using vocabularies")
import_champs_dataset(db, transformation, ingest, ENV["DATA_INGEST_PATH"], "CHAMPS_deid_decode_results", "Raw data from CHAMPS level 2 release, categorical variables not encoded using vocabularies")
close(db)
