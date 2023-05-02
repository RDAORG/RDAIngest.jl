using ConfigEnv
using RDAIngest
using DBInterface
using DataFrames

#get environment variables
dotenv()

db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")

df = DataFrame(source = ["Source 2", "Source 3"])

savedataframe(db,df, "sources")

close(db)