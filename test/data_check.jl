# Trials for data quality check 
# Young 
# Updated: Aug 21 2023

using ConfigEnv
using DBInterface
using DataFrames
using Dates
using BenchmarkTools
using SQLite
using RDAIngest #just run the codes in RDAIngest.jl / rdadatabase.jl 
using Dates
using Arrow

#get environment variables
dotenv()

#outputdatasets
db = opendatabase(ENV["RDA_DATABASE_PATH"], "RDA")
try
    @time dataset_to_arrow(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 1, ENV["DATA_INGEST_PATH"])
    @time dataset_to_arrow(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 2, ENV["DATA_INGEST_PATH"])
    @time dataset_to_arrow(db, 3, ENV["DATA_INGEST_PATH"])
    @time dataset_to_csv(db, 3, ENV["DATA_INGEST_PATH"])
finally
    close(db)
end

# Example codes to get the dataset to dataframe
DBInterface.execute(db, "SELECT * FROM value_types";) |> DataFrame

df1=DBInterface.execute(db, "SELECT * FROM vocabularies";) |> DataFrame

# Set up the CHAMPS verbal autopsy dataset
cv1=DBInterface.execute(db, "SELECT * FROM data WHERE dataset_id =2")
cv1

row_ids = DBInterface.execute(db, "SELECT row_id FROM datarows WHERE dataset_id = 2;") |> DataFrame

sql = """

      SELECT variable_id, value FROM data

      WHERE row_id IN ($(join(row_ids.row_id, ",")));

  """

cv1 = DBInterface.execute(db, sql) |> DataFrame

using DataFrames

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
champs_vars = DBInterface.execute(db, variables_query) |> DataFrame

# Merge data_df with variables_df based on variable_id
champs_df1 = innerjoin(champs_data, champs_vars, on = :variable_id)

champs_df2 = unstack(champs_df1, :value, :variable_id, allowmissing=true)

wide_dict = Dict{Symbol, Vector}()
for row in eachrow(champs_df1)
    row_id = row.row_id
    variable_id = row.variable_id
    value = row.value
    wide_dict[row_id] = get(wide_dict, row_id, Dict{Symbol, Any}())
    wide_dict[row_id][Symbol("variable_$variable_id")] = value
end

wide_df = DataFrame(wide_dict)

######################################
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