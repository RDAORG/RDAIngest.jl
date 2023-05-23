using RDAIngest
using ConfigEnv

#pwd()
#cd("./RDAIngest.jl")

dotenv()

###########TOY FUNCTION
#function hello(name)
#    println("Hello $name") 
# end
#hello("Kobus")


#db=opendatabase(ENV["RDA_DATABASE_PATH"],"RDA")
#a=getsource(db,"CHAMPS")
#close(db)


###########INGEST CHAMPS LAB AND TAC RESULTS

ingest_champs_labtac(ENV["RDA_DATABASE_PATH"], "RDA", ENV["DATA_INGEST_PATH"], 
              "CHAMPS Level2 Data V4.10",
              "Ingest of CHAMPS de-identified data", "ingest_champs2 - testing",
              "Yue Chu",
              "Raw lab and tac data from CHAMPS level 2 release",
              ENV["DATA_DICTIONARY_PATH"])