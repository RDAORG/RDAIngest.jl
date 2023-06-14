using RDAIngest
using ConfigEnv

dotenv()
#pwd()

# ingest_champs(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description, dictionarypath,labtaconly::String)
ingest_champs(ENV["RDA_DATABASE_PATH"], 
              "RDA",
              ENV["DATA_INGEST_PATH"], 
              "CHAMPS Level-2 Data accessed 20230518",
              "Ingest of CHAMPS de-identified data", "ingest_champs v2 testing",
              "Yue Chu",
              "Raw CHAMPS level 2 data from 2023 release",
              ENV["DATA_DICTIONARY_PATH"],
              "labtaconly")


# ingest_comsa(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description, dictionarypath)
ingest_comsa(ENV["RDA_DATABASE_PATH"], 
              "RDA", 
              ENV["DATA_INGEST_PATH"], 
              "COMSA Level2 Data Version 20230308",
              "Ingest of COMSA de-identified VA data", "ingest_comsa v2 testing",
              "Yue Chu",
              "Raw COMSA level 2 VA data from 20230308 release",
              ENV["DATA_DICTIONARY_PATH"])

add_comsa_dictionary(ENV["DATA_INGEST_PATH"],ENV["DATA_DICTIONARY_PATH"])