using ConfigEnv
using RDAIngest
using DBInterface
using DataFrames

#get environment variables
dotenv()

# createdatabase(ENV["RDA_DATABASE_PATH"], "RDA", replace=true)

db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")
# x = addsource(db, "CHAMPS")

# df = read_champs_va(ENV["DATA_INGEST_PATH"])

# df = read_champs_basic_demographics(ENV["DATA_INGEST_PATH"])

# gdf = combine(groupby(df,:site_iso_code), nrow => :n)

# add_champs_sites(db, ENV["DATA_INGEST_PATH"])

add_champs_protocols(db, ENV["DATA_INGEST_PATH"])

close(db)