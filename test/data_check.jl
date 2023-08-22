# Data quality check 
# Young 
# Last Updated: Aug 21 2023

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

#get environment variables
dotenv()

#Get the DB in SQL outputdatasets 
db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")

# Example codes to get the dataset to dataframe
DBInterface.execute(db, "SELECT * FROM value_types";) |> DataFrame

# Set up the [CHAMPS verbal autopsy] as dataframe

# Query data for rows with dataset_id = 2
data_query = """
SELECT row_id, variable_id, value
FROM data
WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 2)
"""
champs_data = DBInterface.execute(db, data_query)|>DataFrame

# Query variables for variable information
variables_query = """
SELECT variable_id, name, value_type_id
FROM variables
WHERE variable_id IN (SELECT DISTINCT variable_id FROM data WHERE row_id IN (SELECT row_id FROM datarows WHERE dataset_id = 2))
"""
champs_vars = DBInterface.execute(db, variables_query)|> DataFrame

# Merge data_df with variables_df based on variable_id
champs_df1 = innerjoin(champs_data, champs_vars, on = :variable_id)

# Long to Wide format based on unique row ID and variable name
champs_df2 = unstack(champs_df1, :row_id,  :name, :value)

names(champs_df2) # 457 variables 

# Save the DataFrame to a CSV file to view the dataframe
CSV.write("champs_df2.csv", champs_df2)

# Check the unique values of each variable ()
unique(champs_df2[!, "Id10358"])

"# Problematic variables from 457 variables
fix type: ["Id10024", "Id10248","Id10250","Id10262","Id10266","Id10352"] 
Surely need to clean values: ["Id10106", "Id10108", "Id10161_0","Id10161_1","Id10162","Id10202","Id10221",
               "Id10285","Id10358","Id10359","Id10367","Id10379","Id10380","Id10382","Id10392","Id10394"
               ] 
"

# CLEAN THE DATA 

## STEP 1: Every string variable should be in the lowercase.

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

## STEP 3: make 











##Data type####################################
"""
    get_datatype(db::SQLite.DB, domain)::AbstractDataFrame

Get variable value types for dataset

"""

function get_datatype(db::SQLite.DB, dataset_id)::AbstractDataFrame

    var_ids = DBInterface.execute(db, "SELECT variable_id FROM dataset_variables WHERE dataset_id = $dataset_id;") |> DataFrame

    sql = """

          SELECT variable_id, name, value_type_id FROM variables
          WHERE variable_id IN ($(join(var_ids.variable_id, ",")));

      """

    return DBInterface.execute(db, sql) |> DataFrame

end

get_datatype(db, 2)



for col in var_type.name[(var_type.value_type_id .== 1)]

            if eltype(df[!, col]) == Union{Missing, String}

                df[!, col] = [ismissing(x) ? missing : parse.(Float64,x) for x in df[!, col]]

            end

            df[!, col] = [ismissing(x) ? missing : convert.(Int,x) for x in df[!, col]]

        end

        for col in var_type.name[(var_type.value_type_id .== 2)]

            df[!, col] = [ismissing(x) ? missing : convert.(Float64,x) for x in df[!, col]]

        end

        for col in var_type.name[(var_type.value_type_id .== 3)]

            df[!, col] = [ismissing(x) ? missing : lowercase(x) for x in df[!, col]]

        end

        for col in var_type.name[(var_type.value_type_id .== 4)]

            df[!, col] = [ismissing(x) ? missing : Date(x) for x in df[!, col]]

        end

        for col in var_type.name[(var_type.value_type_id .== 5)]

            df[!, col] = [ismissing(x) ? missing : Dates.format(x, "yyyy-mm-ddTHH:mm:ss.sss") for x in df[!, col]]

        end

        for col in var_type.name[(var_type.value_type_id .== 6)]

            df[!, col] = [ismissing(x) ? missing : Dates.format(x, "HH:mm:ss.sss") for x in df[!, col]]

        end