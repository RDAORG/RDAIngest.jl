# Data quality check 
# Young 
# Last Updated: Aug 23 2023

using ConfigEnv
using DBInterface
using DataFrames
using Dates
using BenchmarkTools
using SQLite
using RDAIngest #just run the codes in RDAIngest.jl / rdadatabase.jl 
using Dates
using Arrow
using CSV

#Get environment variables #change the path in .env file
dotenv()

#Get the DB in SQL outputdatasets 
db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")

# Example codes to get the dataset to dataframe
DBInterface.execute(db, "SELECT * FROM value_types";) |> DataFrame
DBInterface.execute(db, "SELECT * FROM datasets";) |> DataFrame

##############################################
### STEP 0. Set up the [CHAMPS verbal autopsy] as dataframe -> dataset_id ==5
##############################################

# Query data for rows with dataset_id = 5
data_query = """
SELECT row_id, variable_id, value
FROM data
WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 5)
"""
champs_data = DBInterface.execute(db, data_query)|>DataFrame

# Query variables for variable information
variables_query = """
SELECT variable_id, name, value_type_id
FROM variables
WHERE variable_id IN (SELECT DISTINCT variable_id FROM data WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 5))
"""
champs_vars = DBInterface.execute(db, variables_query)|> DataFrame

# Merge data_df with variables_df based on variable_id
champs_df1 = innerjoin(champs_data, champs_vars, on = :variable_id)

# Long to Wide format based on unique row ID and variable name
champs_df2 = unstack(champs_df1, :row_id,  :name, :value)

names(champs_df2) # 457 variables 

# Save the DataFrame to a CSV file to explore the dataframe
CSV.write("champs_df2.csv", champs_df2)

# Check the unique values of each variable ()
unique(champs_df2[!, "Id10359"])

"""# Problematic variables from 457 variables
fix type: ["Id10024", "Id10248","Id10250","Id10262","Id10266","Id10352"] 
Surely need to clean values: ["Id10106", "Id10108", "Id10161_0","Id10161_1","Id10162","Id10202","Id10221",
               "Id10285","Id10358","Id10359","Id10367","Id10379","Id10380","Id10382","Id10392","Id10394"
               ] 
"""
#######################################
### STEP 2. Documentation of data quality of raw data in Markdown 
###(CHAMPS_VA_Quality_Check.jmd)
#######################################

## 0. Set up the CHAMPS VA dataset
using CSV
using DataFrames
using Pkg
"FreqTables" ∉ keys(Pkg.project().dependencies) && Pkg.add("FreqTables")
"StatsBase" ∉ keys(Pkg.project().dependencies) && Pkg.add("StatsBase")
using FreqTables
using StatsBase
filename = "champs_df2"
path = "/Users/young/Documents/GitHub/RDAIngest.jl/"
file = joinpath(path, "$filename.csv")
champs_raw = CSV.File(file; delim=',', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame


## 1. Describe the dataset 

### 1) Data Size
size(champs_raw)

### 2) Data types 
column_types = Dict{Symbol, Type}()
for col in names(champs_raw)
    column_types[Symbol(col)] = eltype(champs_raw[!, col])
end

unique_types = Set(values(column_types))

type_counts = Dict{Type, Int}()
for col_type in values(column_types)
    type_counts[col_type] = get(type_counts, col_type, 0) + 1
end

println("Summary of variable types:")
for (col_type, count) in type_counts
    println("$col_type: $count variables")
end

## 2. Looking closely into each variable type

### 
println("Variables with type Missing:")
for col_name in names(champs_raw)
    if eltype(champs_raw[!, col_name]) == Missing
        println(col_name)
    end
end

### 1) Categorical Variables (String)
##### Most variables are categorical which is typed in String. 

#### example Q. At any time during the final illness was there blood in the stools?
freqtable(champs_raw, :"Id10186")

freqtable(champs_raw, :"Id10193")




#############################################
### STEP 3. CLEAN THE DATA (not processed yet)
#############################################

# 1. Every string variable should be in the lowercase.

# Create a copy of the original DataFrame
champs_df3 = copy(champs_df2)

# Check the datatypes for each column
for col_name in names(champs_df3)
    col = champs_df3[!, col_name]
    println("$col_name: $(eltype(col))")
end

# Loop through each column in the copy
for col_name in names(champs_df3)
    col = champs_df3[!, col_name]
    col_type = eltype(col)
    if col_type == Union{Missing, String}
        champs_df3[!, col_name] .= map(x -> x isa Missing ? x : lowercase(x), col)
    end
end

# Save the DataFrame to a CSV file to check the dataframe
CSV.write("champs_df3.csv", champs_df3)

## STEP 2: Make the "DK" mising data consistent
using Statistics
using Pkg
#Pkg.add("FreqTables")
using FreqTables

# Check the frequency of each variable
freqtable(champs_df3, :"Id10186")

# Define a function to replace "doesn't know" and "does not know" with "dk"
replace_dk(text) = ismissing(text) ? missing : (text == "doesn't know" || text == "does not know" ? "dk" : text)

# Apply the replace_dk function to the entire DataFrame
for col_name in names(champs_df3)
    col = champs_df3[!, col_name]
    if eltype(col) <: Union{Missing, String}
        champs_df3[!, col_name] .= replace_dk.(col)
    end
end

# Check again for sanity
freqtable(champs_df3, :"Id10186")

# Save the DataFrame to a CSV file to check the dataframe
CSV.write("champs_df3.csv", champs_df3)


