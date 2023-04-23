using ConfigEnv
using RDAIngest
using DBInterface

#get environment variables
dotenv()

createdatabase(ENV["RDA_DATABASE_PATH"], "RDA", replace=true)

db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")
x = addsource(db, "CHAMPS")
DBInterface.close!(db)
