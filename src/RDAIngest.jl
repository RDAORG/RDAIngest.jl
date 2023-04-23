module RDAIngest

using DataFrames
using CSV
using SQLite
using DBInterface
using ConfigEnv

export opendatabase, get_table, addsource, getsource, createdatabase

"""
    opendatabase(path::String, name::String)::SQLite.DB

Open file on path as an SQLite database (assume .sqlite extension)
"""
function opendatabase(path::String, name::String)::SQLite.DB
    file = joinpath(path, "$name.sqlite")
    if isfile(file)
        return SQLite.DB(file)
    else
        error("File '$file' not found.")
    end
end

"""
    get_table(db::SQLite.DB, table::String)::AbstractDataFrame

Retrieve table `table` as a DataFrame from `db`
"""
function get_table(db::SQLite.DB, table::String)::AbstractDataFrame
    sql = "SELECT * FROM $(table)"
    df = DBInterface.execute(db, sql; iterate_rows=true) |> DataFrame
    return df
end

"""
    getsource(db::SQLite.DB, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function getsource(db::SQLite.DB, name)
    sql = "SELECT * FROM sources WHERE name = '?'"
    result = DBInterface.execute(db, sql, params=[name])
    if isempty(result)
        return missing
    else
        df = DataFrame(result)
        return df[1, :source_id]
    end
end

"""
    addsource(db::SQLite.DB, name)

Add source `name` to the sources table, and returns the `source_id`, if source exists, its `source_id` is returned
"""
function addsource(db::SQLite.DB, name)
    id = getsource(db, name)
    if ismissing(id)
        sql = "INSERT INTO sources (name) VALUES ('?')"
        DBInterface.execute(db, sql; params=[name])
    end
    return getsource(db, name)
end

include("rdadatabase.jl")

end