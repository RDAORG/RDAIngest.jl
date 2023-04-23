module RDAIngest

using DataFrames
using CSV
using SQLite
using DBInterface
using ConfigEnv
using CSV

export opendatabase, get_table, addsource, getsource, createdatabase,
    read_champs_va, read_champs_basic_demographics, add_champs_sites, add_champs_protocols


"""
    getsource(db::SQLite.DB, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function getsource(db::SQLite.DB, name)
    sql = "SELECT * FROM sources WHERE name = @name"
    stmt = DBInterface.prepare(db, sql)
    result = DBInterface.execute(stmt, (name = name))
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
        sql = "INSERT INTO sources (name) VALUES (@name)"
        stmt = DBInterface.prepare(db, sql)
        DBInterface.execute(stmt, (name = name))
    end
    return getsource(db, name)
end

function read_champs_va(path)::AbstractDataFrame
    file = joinpath(path, "CHAMPS", "CHAMPS_de_identified_data", "CHAMPS_deid_verbal_autopsy.csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=',', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame
        return df
    end
end

function read_champs_basic_demographics(path)::AbstractDataFrame
    file = joinpath(path, "CHAMPS", "CHAMPS_de_identified_data", "CHAMPS_deid_basic_demographics.csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=',', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame
        return df
    end
end

function add_champs_sites(db::SQLite.DB, datapath)
    df = read_champs_basic_demographics(datapath)
    sites = combine(groupby(df, :site_iso_code), nrow => :n)
    source = getsource(db, "CHAMPS")
    insertcols!(sites, 1, :source_id => source)
    sites.site_id = 1:nrow(sites)
    select!(sites, :site_id, :site_iso_code => ByRow(x -> x) => :name, :site_iso_code, :source_id)
    #SQLite.load!(sites, db, "sites")
    sql = "INSERT INTO sites (name, site_iso_code, source_id) VALUES (@name, @site_iso_code, @source_id)"
    stmt = DBInterface.prepare(db, sql)
    for row in eachrow(sites)
        DBInterface.execute(stmt, (name=row.name, site_iso_code=row.site_iso_code, source_id=row.source_id))
    end
end

function add_champs_protocols(db::SQLite.DB, datapath)
    sql = raw"""
    INSERT INTO protocols (name) VALUES (@name)
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (name = "CHAMPS-Mortality-Surveillance-Protocol-v1.3"))
    DBInterface.execute(stmt, (name = "CHAMPS-Social-Behavioral-Science-Protocol-v1.0"))
    #insert document
    sql = raw"""
    INSERT INTO protocol_documents (protocol_id, name, document) VALUES (@protocol_id, @name, @document)
    """
    stmt = DBInterface.prepare(db, sql)
    sql = raw"""
    INSERT INTO site_protocols (site_id, protocol_id) VALUES (@site_id, @protocol_id)
    """
    stmt2 = DBInterface.prepare(db, sql)
    protocols = DBInterface.execute(db, "SELECT * FROM protocols") |> DataFrame
    sites = DBInterface.execute(db, "SELECT * FROM sites") |> DataFrame
    for row in eachrow(protocols)
        file = joinpath(datapath, "CHAMPS", "Protocols", "$(row.name).pdf")
        if isfile(file)
            document = read(file)
            DBInterface.execute(stmt, (protocol_id=row.protocol_id, name="$(row.name).pdf", document=document))
            for site in eachrow(sites)
                DBInterface.execute(stmt2,(site_id = site.site_id, protocol_id=row.protocol_id))
            end
        end
    end
end

include("rdadatabase.jl")

end