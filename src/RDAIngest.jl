module RDAIngest

using DataFrames
using CSV
using SQLite
using DBInterface
using ConfigEnv
using CSV
using Dates

export opendatabase, get_table, addsource, getsource, createdatabase, getnamedkey,
    read_champs_data, add_champs_sites, add_champs_protocols, read_champs_variables,
    add_dataingest, add_transformation, ingest_champs_deaths, save_CHAMPS_variables, import_champs_dataset

struct VocabularyItem
    value::Int64
    code::String
    description::Union{String,Missing}
end
struct Vocabulary
    name::String
    description::String
    items::Vector{VocabularyItem}
end


"""
    getnamedkey(db, table, key, keycol)

 Return the integer key from table `table` in column `keycol` for key with name `key`
"""
function getnamedkey(db, table, key, keycol)
    sql = "SELECT * FROM $table WHERE name = @name"
    stmt = DBInterface.prepare(db, sql)
    result = DBInterface.execute(stmt, (name = key))
    if isempty(result)
        return missing
    else
        df = DataFrame(result)
        return df[1, keycol]
    end
end
"""
    getsource(db::SQLite.DB, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function getsource(db::SQLite.DB, name)
    return getnamedkey(db, "sources", name, Symbol("source_id"))
end

"""
    addsource(db::SQLite.DB, name)

Add source `name` to the sources table, and returns the `source_id`
"""
function addsource(db::SQLite.DB, name)
    id = getsource(db, name)
    if ismissing(id)  # insert CHAMPS domain
        stmt = DBInterface.prepare(db, "INSERT INTO sources (name) VALUES (@name)")
        id = DBInterface.lastrowid(DBInterface.execute(stmt, (name = name)))
    end
    return id
end
"""
    read_champs_data(path, name)::AbstractDataFrame

Returns a DataFrame with the CHAMPS data, from the Level 2 de-identified CHAMPS data collection
"""
function read_champs_data(path, name)::AbstractDataFrame
    file = joinpath(path, "CHAMPS", "CHAMPS_de_identified_data", "$name.csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=',', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame
        return df
    end
end

"""
    add_champs_sites(db::SQLite.DB, datapath)

Add the CHAMPS sites - note, actual CHAMP sites not know - sites are just the countries the sites are in
"""
function add_champs_sites(db::SQLite.DB, datapath)
    df = read_champs_data(datapath, "CHAMPS_deid_basic_demographics")
    sites = combine(groupby(df, :site_iso_code), nrow => :n)
    source = getsource(db, "CHAMPS")
    insertcols!(sites, 1, :source_id => source)
    sites.site_id = 1:nrow(sites)
    select!(sites, :site_id, :site_iso_code => ByRow(x -> x) => :name, :site_iso_code, :source_id)
    sql = "INSERT INTO sites (name, site_iso_code, source_id) VALUES (@name, @site_iso_code, @source_id)"
    stmt = DBInterface.prepare(db, sql)
    for row in eachrow(sites)
        DBInterface.execute(stmt, (name=row.name, site_iso_code=row.site_iso_code, source_id=row.source_id))
    end
end

"""
    add_champs_protocols(db::SQLite.DB, datapath)

Add the CHAMPS Mortality Surveillance and Social Behavioural Science protocols
"""
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
                DBInterface.execute(stmt2, (site_id=site.site_id, protocol_id=row.protocol_id))
            end
        end
    end
end
"""
    lines(str)

Returns an array of lines in `str` 
"""
lines(str) = split(str, '\n')

"""
    read_variables(path, file)

Read a csv file listing variables variables in a CHAMPS dataset, the files are:
  1. 'Format_CHAMPS_deid_basic_demographics.csv' variables in the basic demographic dataset
  2. 'Format_CHAMPS_deid_decode_results.csv' variables in the cuase of death dataset
  3. 'Format_CHAMPS_deid_verbal_autopsy.csv' variables in the verbal autopsy dataset

These files are manually created from the 'CHAMPS De-Identified Data Set Description v4.2.pdf' file distributed with the CHAMPS de-identified dataset by exporting the pdf to an Excel spreadsheet and manually extracting the variable lists as csv files.
"""
function read_champs_variables(path, file)
    file = joinpath(path, "CHAMPS", "CHAMPS_de_identified_data", "$file.csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=';', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame
        vocabularies = Vector{Union{Vocabulary,Missing}}()
        for row in eachrow(df)
            l = lines(row.Description)
            if length(l) > 1
                push!(vocabularies, get_champs_vocabulary(row.Column_Name, l))
                row.Description = l[1]
            else
                push!(vocabularies, missing)
            end
        end
        df.Vocabulary = vocabularies
        return df
    end
end

"""
    get_champs_vocabulary(variable, l)::Vocabulary

Get a vocabulary, name of vocabulary in line 1 of l, vocabulary items (code and description) in subsequent lines, comma-separated
"""
function get_champs_vocabulary(name, l)::Vocabulary
    items = Vector{VocabularyItem}()
    description = ""
    for i in eachindex(l)
        if i == 1
            description = l[i]
        else
            item = split(l[i], ',')
            push!(items, length(item) > 1 ? VocabularyItem(i - 1, item[1], item[2]) : VocabularyItem(i - 1, item[1], missing))
        end
    end
    return Vocabulary(name, description, items)
end

"""
    add_dataingest(db::SQLite.DB, source_id::Int64, date::Date, description::String)::Int64

Insert a data ingestion into the data_ingestions table and return the data_ingestion_id
"""
function add_dataingest(db::SQLite.DB, source_id::Int64, date::Date, description::String)::Int64
    sql = """
    INSERT INTO data_ingestions (source_id, date_received, description)
    VALUES (@source_id, @date, @description)
    RETURNING *;
    """
    stmt = DBInterface.prepare(db, sql)
    ingest = DBInterface.execute(stmt, (source_id=source_id, date=Dates.format(date, "yyyy-mm-dd"), description=description)) |> DataFrame
    if nrow(ingest) > 0
        return ingest[1, :data_ingestion_id]
    else
        error("Unable to insert dataingest")
    end
end
"""
    add_transformation(db::SQLite.DB, type::Int64, status::Int64, description::String, code_reference::String, date_created::Date, created_by::String)

Add a transformation to the transformations table
"""
function add_transformation(db::SQLite.DB, type::Int64, status::Int64, description::String, code_reference::String, date_created::Date, created_by::String)
    sql = """
    INSERT INTO transformations (transformation_type_id, transformation_status_id, description, code_reference, date_created, created_by)
    VALUES (@type, @status, @description, @code_reference, @date_created, @created_by)
    RETURNING *;
    """
    stmt = DBInterface.prepare(db, sql)
    transformation = DBInterface.execute(stmt, (type=type, status=status, description=description, code_reference=code_reference, date_created=Dates.format(date_created, "yyyy-mm-dd"), created_by=created_by)) |> DataFrame
    if nrow(transformation) > 0
        return transformation[1, :transformation_id]
    else
        error("Unable to insert transformation")
    end
end
"""P
    ingest_champs_deaths(db::SQLite.DB, ingest::Int64, path::String)

INSERT CHAMPS deaths into the deaths table, for a specified data ingest. Returns a Dataframe suitable for lloking up the death_id from the CHAMPS deid.
"""
function ingest_champs_deaths(db::SQLite.DB, ingest::Int64, path::String)::AbstractDataFrame
    deaths = read_champs_data(path, "CHAMPS_deid_basic_demographics")
    sites = DBInterface.execute(db, "SELECT * FROM sites WHERE source_id = $(getsource(db, "CHAMPS"));") |> DataFrame
    sitedeaths = innerjoin(deaths, sites, on=:site_iso_code, matchmissing=:notequal)
    sql = """
      INSERT INTO deaths (site_id, external_id, data_ingestion_id)
      VALUES (@site_id, @external_id, @ingest)
    """
    stmt = DBInterface.prepare(db, sql)
    for row in eachrow(sitedeaths)
        DBInterface.execute(stmt, (site_id=row.site_id, external_id=row.champs_deid, ingest=ingest))
    end
    return DBInterface.execute(db, "SELECT death_id, external_id FROM deaths WHERE data_ingestion_id = $(ingest);") |> DataFrame
end
"""
    getdomain(db::SQLite.DB, domainname)

Return the domain_id for domain named `domainname`
"""
function getdomain(db::SQLite.DB, domainname)
    return getnamedkey(db, "domains", domainname, Symbol("domain_id"))
end

"""
    save_CHAMPS_variables(db::SQLite.DB, path::String)

Save the CHAMPS variables, including vocabularies for categorical variables.
!!! note
    Ingested data is not checked to ensure that categorical variable values conform to the vocabulary, in fect in the provided data thre are deviations, mostly in letter case. Common categries, such as the verbal autopsy indicators are also not converted to categorical values.
"""
function save_CHAMPS_variables(db::SQLite.DB, path::String, dictionary::String)
    domain = getdomain(db, "CHAMPS")
    if ismissing(domain)  # insert CHAMPS domain
        domain = DBInterface.lastrowid(DBInterface.execute(db, "INSERT INTO domains(name,description) VALUES('CHAMPS','CHAMPS Level2 Data')"))
    end
    # start with basic demographic data
    variables = read_champs_variables(path, dictionary)
    insertcols!(variables, 1, :domain_id => domain)
    #variable insert SQL
    sql = """
    INSERT INTO variables (domain_id, name, value_type_id, vocabulary_id, description, note)
    VALUES (@domain_id, @name, @value_type_id, @vocabulary_id, @description, @note)
    ON CONFLICT DO UPDATE
      SET vocabulary_id = excluded.vocabulary_id,
          description = excluded.description,
          note = excluded.note 
      WHERE variables.vocabulary_id IS NULL OR variables.description IS NULL OR variables.note IS NULL;
    """
    stmt = DBInterface.prepare(db, sql)
    for row in eachrow(variables)
        id = missing
        if !ismissing(row.Vocabulary)
            id = add_vocabulary(db, row.Vocabulary)
        end
        DBInterface.execute(stmt, (domain_id=row.domain_id, name=row.Column_Name, value_type_id=row.DataType, vocabulary_id=id, description=row.Description, note=row.Note))
    end
    return nothing
end
"""
    add_vocabulary(db::SQLite.DB, vocabulary::Vocabulary)

Insert a vocabulary and its items into a RDA database, returns the vocabulary_id of the inserted vocabulary
"""
function add_vocabulary(db::SQLite.DB, vocabulary::Vocabulary)
    id = getnamedkey(db, "vocabularies", vocabulary.name, "vocabulary_id")
    if !ismissing(id)
        return id
    end
    #vocabulary insert SQL
    sql = """
    INSERT INTO vocabularies (name, description) VAlUES (@name, @description)
    RETURNING *;
    """
    stmt = DBInterface.prepare(db, sql)
    v = DBInterface.execute(stmt, (name=vocabulary.name, description=vocabulary.description)) |> DataFrame
    if nrow(v) > 0
        id = v[1, :vocabulary_id]
    else
        error("Unable to insert vocabulary '$(vocabulary.name)'")
    end
    #vocabulary item insert SQL
    sql = """
    INSERT INTO vocabulary_items(vocabulary_id, value, code, description)
    VALUES (@vocabulary_id, @value, @code, @description)
    """
    stmt = DBInterface.prepare(db, sql)
    for item in vocabulary.items
        DBInterface.execute(stmt, (vocabulary_id=id, value=item.value, code=item.code, description=item.description))
    end
    return id
end
"""
    import_champs_dataset(db::SQLite.DB, transformation, ingest, path, dataset_name)

Insert dataset, datarows, and data into SQLite db
"""
function import_champs_dataset(db::SQLite.DB, transformation, ingest, path, dataset_name, description)
    try
        SQLite.transaction(db)
        data = read_champs_data(path, dataset_name)
        variables = lookup_variables(db, names(data), getnamedkey(db, "domains", "CHAMPS", "domain_id"))
        var_lookup = Dict{String,Int64}(zip(variables.name, variables.variable_id))
        sql = """
        INSERT INTO datasets(name, date_created, description) 
        VALUES (@name, @date_created, @description);
        """
        stmt = DBInterface.prepare(db, sql)
        dataset_id = DBInterface.lastrowid(DBInterface.execute(stmt, (name=dataset_name, date_created=Dates.format(today(), "yyyy-mm-dd"), description=description)))
        add_dataset_ingest(db, dataset_id, transformation, ingest)
        add_transformation_output(db, dataset_id, transformation)
        add_dataset_variables(db, variables, dataset_id)
        #store datarows
        stmt = DBInterface.prepare(db, "INSERT INTO datarows (dataset_id) VALUES(@dataset_id);")
        for i = 1:nrow(data)
            DBInterface.execute(stmt, (dataset_id = dataset_id))
        end
        #prepare data for storage
        datarows = DBInterface.execute(db, "SELECT row_id FROM datarows WHERE dataset_id = $dataset_id;") |> DataFrame
        d = hcat(datarows, data, makeunique=true, copycols=false) #add the row_id to each row of data
        #store whole column at a time
        for col in propertynames(data)
            variable_id = var_lookup[string(col)]
            coldata = select(d, :row_id, col => :value; copycols=false)
            add_data_column(db, variable_id, coldata)
        end
        SQLite.commit(db)
    catch e
        SQLite.rollback(db)
        throw(e)
    end
    return nothing
end
"""
    add_data_column(db::SQLite.DB, variable_id, coldata)

Insert data for a column of the source dataset
"""
function add_data_column(db::SQLite.DB, variable_id, coldata)
    sql = """
        INSERT INTO data (row_id, variable_id, value)
        VALUES (@row_id, @variable_id, @value)
    """
    stmt = DBInterface.prepare(db, sql)
    isdate = eltype(coldata.value) >: Date
    for row in eachrow(coldata)
        DBInterface.execute(stmt, (row_id=row.row_id, variable_id=variable_id, value=(isdate && !ismissing(row.value) ? Dates.format(row.value,"yyyy-mm-dd") : row.value)))
    end
end
"""
    add_dataset_variables(db::SQLite.DB, variables, dataset_id)

Insert dataset variables into table dataset_variables
"""
function add_dataset_variables(db::SQLite.DB, variables, dataset_id)
    sql = """
        INSERT INTO dataset_variables (dataset_id, variable_id)
        VALUES (@dataset_id, @variable_id);
    """
    stmt = DBInterface.prepare(db, sql)
    for row in eachrow(variables)
        DBInterface.execute(stmt, (dataset_id=dataset_id, variable_id=row.variable_id))
    end
end
"""
    lookup_variables(db::SQLite.DB, variable_names, domain)

Returns a DataFrame with dataset variable names and ids
"""
function lookup_variables(db::SQLite.DB, variable_names, domain)
    names = DataFrame(:name => variable_names)
    sql = """
    SELECT name, variable_id FROM variables
    WHERE domain_id = $domain
    """
    variables = DBInterface.execute(db, sql) |> DataFrame
    v = innerjoin(variables, names, on=:name) #just the variables in this dataset
    return v
end
"""
    add_dataset_ingest(db, dataset_id, transformation, ingest)

Record a dataset ingest into ingest_datasets
"""
function add_dataset_ingest(db::SQLite.DB, dataset_id, transformation, ingest)
    sql = """
    INSERT INTO ingest_datasets (data_ingestion_id, transformation_id, dataset_id)
    VALUES (@data_ingestion_id, @transformation_id, @dataset_id);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (data_ingestion_id=ingest, transformation_id=transformation, dataset_id=dataset_id))
end
"""
    add_transformation_output(db, dataset_id, transformation)

Add a transformation output dataset
"""
function add_transformation_output(db::SQLite.DB, dataset_id, transformation)
    sql = """
    INSERT INTO transformation_outputs (transformation_id, dataset_id)
    VALUES (@transformation_id, @dataset_id);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (transformation_id=transformation, dataset_id=dataset_id))
end

include("rdadatabase.jl")

end