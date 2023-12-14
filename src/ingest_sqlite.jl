using RDAIngest
using ConfigEnv
using Logging
using DBInterface
using DataFrames
using Dates
using BenchmarkTools

#get environment variables
dotenv()
