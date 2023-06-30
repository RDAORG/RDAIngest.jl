module RDAIngest

using DataFrames
using CSV
using SQLite
using DBInterface
using ConfigEnv
using CSV
using Dates
using Arrow
using XLSX 
using DelimitedFiles

export opendatabase, get_table, addsource, getsource, createdatabase, getnamedkey,
    add_dataingest, add_transformation, link_deathrows, get_variable, getdomain, 
    dataset_to_dataframe, dataset_to_arrow, dataset_to_csv, savedataframe,
    read_data, add_sites, ingest_deaths, add_variables, read_variables, get_vocabulary, import_dataset, 
    add_protocols, 
    vec_to_df, join_wona, 
    #create_champs_dictionary, create_comsa_dictionary, 
    ingest_champs, ingest_comsa
    
    
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
   ingest_comsa(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description)

This is the main function to ingest a COMSA Level 2 data distribution. 
    The current version only ingest VA results version 20230308, accessed 20230522.

## Parameters
  * `dbpath        `: The file path to the database.
  * `dbname        `: The name of the database, .sqlite extension assumed.
  * `datapath      `: The file path to the CHAMPS data distribution, assumes the distribution is extracyed into folder `CHAMPS_de_identified_data`.
  * `ingest        `: The description of the data ingest.
  * `transformation`: The description of the transformation for the data ingest.
  * `code-reference`: The reference to the code used for the transformation `function` in `package`
  * `author        `: The transformation author
  * `description   `: The dataset description
  * `dictionarypath`: The path to the data dictionaries

"""

function ingest_comsa(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description, dictionarypath)
    db = opendatabase(dbpath, dbname)
    sourcename = "COMSA"

    try
        comsa = addsource(db, sourcename)
        
        add_sites(db, datapath, sourcename, "Comsa_WHO_VA_20230308", "provincia",site_iso_code="MZ")
        println("Completed adding sites")

        add_protocols(db, datapath, sourcename)
        println("Completed adding protocols")

        #add_comsa_dictionary(datapath,dictionarypath)
        #println("Completed adding data dictionaries")

        ingest = add_dataingest(db, comsa, today(), ingest)
        transformation = add_transformation(db, 1, 1, transformation, code_reference, today(), author)
        
        ingest_deaths(db, ingest, datapath, sourcename, "Comsa_WHO_VA_20230308",
                        sitevar="provincia", idvar = "comsa_id")
        println("Completed ingesting deaths")
        
        add_variables(db, sourcename, dictionarypath, "Format_Comsa_WHO_VA_20230308")
        println("Completed adding variables")
        
        va_ds = import_dataset(db, datapath, sourcename, "COMSA_WHO_VA_20230308", transformation, ingest, description)
        println("Completed importing COMSA VA")
        
        domain = getnamedkey(db, "domains", "COMSA", Symbol("domain_id"))
        death_idvar = get_variable(db, domain, "comsa_id")
        link_deathrows(db, ingest, va_ds, death_idvar) 
        println("Completed linking death rows")

        return nothing
    finally
        close(db)
    end
end


"""
   ingest_champs(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description)

!!! note
    The database should exist and be in the RDA format created by the [`createdatabase`](@ref) function

This is the main function to ingest a CHAMPS Level 2 data distribution, including demographics, VA, decode, lab, tac data

## Parameters
  * `dbpath        `: The file path to the database.
  * `dbname        `: The name of the database, .sqlite extension assumed.
  * `datapath      `: The file path to the CHAMPS data distribution, assumes the distribution is extracyed into folder `CHAMPS_de_identified_data`.
  * `ingest        `: The description of the data ingest.
  * `transformation`: The description of the transformation for the data ingest.
  * `code-reference`: The reference to the code used for the transformation `function` in `package`
  * `author        `: The transformation author
  * `description   `: The dataset description
  * `dictionarypath`: The path to the data dictionaries
  * `labtaconly    `: The indicator for adding lab tac data only - 'labtaconly' if only adding lab and tac.

## Method

1. A **CHAMPS** source is created if it doesn't exist, using function [`addsource`](@ref).
2. The CHAMPS sites are extracted from the `CHAMPS_deid_basic_demographics` dataset and saved using the [`add_sites`](@ref) function.
3. The CHAMPS protocol are added as pdfs from a sub-directory `CHAMPS\\Protocols` in `datapath` using the [`add_protocols`](@ref) function.
4. A data ingest is created using the [`add_dataingest`](@ref) function.
5. A transformation reprenting the complete data ingest is created using the [`add_transformation`](@ref) function.
6. A entry for each death is inserted in the `deaths` table, for each row the `CHAMPS_deid_basic_demographics` dataset using the [`ingest_deaths`](@ref) function.
7. The CHAMPS dataset variables are imported from a manually created data dictionary file as described in the [`add_variables`](@ref) function.
8. The CHAMPS datasets are imported using the [`import_dataset`](@ref) function.
9. The CHAMPS deaths are linked to the dataset rows containing the detail data about each death in the CHAMPS data distribution, using the function [`link_deathrows`](@ref)

"""

function ingest_champs(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description, dictionarypath,labtaconly::String)
    db = opendatabase(dbpath, dbname)
    sourcename = "CHAMPS"

    try
        champs = addsource(db, sourcename)

        if labtaconly!="labtaconly"
            add_sites(db, datapath, sourcename, "CHAMPS_deid_basic_demographics", "site_iso_code")
            println("Completed adding sites")

            add_protocols(db, datapath, sourcename)
            println("Completed adding protocols")
        end
        
        ingest = add_dataingest(db, champs, today(), ingest)
        transformation = add_transformation(db, 1, 1, transformation, code_reference, today(), author)
        
        if labtaconly!="labtaconly"
        # Ingest deaths
            ingest_deaths(db, ingest, datapath, sourcename, "CHAMPS_deid_basic_demographics",
                            sitevar="site_iso_code", idvar = "champs_deid")
            println("Completed ingesting deaths")

        # Add variables from each dataset
            add_variables(db, sourcename, dictionarypath, "Format_CHAMPS_deid_basic_demographics")
            add_variables(db, sourcename, dictionarypath, "Format_CHAMPS_deid_verbal_autopsy")
            add_variables(db, sourcename, dictionarypath, "Format_CHAMPS_deid_decode_results")
        end 
        add_variables(db, sourcename, dictionarypath, "Format_CHAMPS_deid_tac_results")
        add_variables(db, sourcename, dictionarypath, "Format_CHAMPS_deid_lab_results")
        println("Completed adding variables")
        
        if labtaconly!="labtaconly"
        # Import datasets
            basic_ds = import_dataset(db, datapath, sourcename, "CHAMPS_deid_basic_demographics", transformation, ingest, description)
            println("Completed importing CHAMPS basic demographics")
            va_ds = import_dataset(db, datapath, sourcename, "CHAMPS_deid_verbal_autopsy", transformation, ingest, description)
            println("Completed importing CHAMPS VA")
            decode_ds = import_dataset(db, datapath, sourcename, "CHAMPS_deid_decode_results", transformation, ingest, description)
            println("Completed importing CHAMPS decode resutls")
        end
        tac_ds = import_dataset(db, datapath, sourcename, "CHAMPS_deid_tac_results", transformation, ingest, description)
        println("Completed importing CHAMPS TAC results")
        lab_ds = import_dataset(db, datapath, sourcename, "CHAMPS_deid_lab_results", transformation, ingest, description)
        println("Completed importing CHAMPS LAB results")
        
        domain = getnamedkey(db, "domains", sourcename, Symbol("domain_id"))
        death_idvar = get_variable(db, domain, "champs_deid")

        if labtaconly!="labtaconly"
        # Insert records into deathrows table
            link_deathrows(db, ingest, basic_ds, death_idvar)
            link_deathrows(db, ingest, va_ds, death_idvar)
            link_deathrows(db, ingest, decode_ds, death_idvar)
        end
        link_deathrows(db, ingest, tac_ds, death_idvar)
        link_deathrows(db, ingest, lab_ds, death_idvar)
        println("Completed linking death rows")
        
        return nothing
    finally
        close(db)
    end
end


"""
ingest_deaths(db::SQLite.DB, ingest::Int64, datapath::String, sourcename, filename, sitevar, idvar)

INSERT deaths into the deaths table, for a specified data ingest. 

## Parameters
  * `ingest        `: The description of the data ingest.  
  * `datapath  `: The path to the raw de-identified data, assumes the data is extracted into "De_identified_data".
  * `sourcename    `: The name of data source, either "CHAMPS" or "COMSA".
  * `filename      `: The name of the raw deaths data file.

  * `sitevar       `: The name of the site name variable in raw deaths data. (built-in for now) 
  * `idvar         `: The name of the unique identifier variable in raw deaths data. (built-in for now)
"""
function ingest_deaths(db::SQLite.DB, ingest::Int64, datapath::String, sourcename, filename, sitevar, idvar)
    deaths = read_data(datapath, sourcename, filename)
    sites = DBInterface.execute(db, "SELECT * FROM sites WHERE source_id = $(getsource(db, sourcename));") |> DataFrame
    
    deaths[!,:name] = deaths[!,sitevar] # match on name
    sitedeaths = innerjoin(deaths, sites, on=:name, matchmissing=:notequal)
    savedataframe(db, select(sitedeaths, :site_id, idvar => :external_id, [] => Returns(ingest) => :data_ingestion_id, copycols=false), "deaths")
    return nothing
end

"""
    add_sites(db::SQLite.DB, datapath::String, sourcename::String, filename::String, sitevar::String, iso_code)

Add the CHAMPS/COMSA sites. 
CHAMPS: only with country iso2 codes: site_iso_code
COMSA: Mozambique provinces:

## Parameters
  * `datapath  `: The path to the raw de-identified data, assumes the data is extracted into "De_identified_data".
  * `sourcename    `: The name of data source, either "CHAMPS" or "COMSA".
  * `filename      `: The name of the raw data file with site name variables.
  * `sitevar       `: The name of the site name variable.
  * `iso_code      `: The site iso code if not provided in the dataset.
  
"""
function add_sites(db::SQLite.DB, datapath::String, sourcename::String, filename::String, sitevar::String, iso_code::String)
    df = read_data(datapath, sourcename, filename)
    source = getsource(db, sourcename)

    sites = combine(groupby(df, sitevar), nrow => :n)

    insertcols!(sites, 1, :source_id => source)

    if !("site_iso_code" in names(df))
        sites[!, :site_iso_code] .= iso_code
    end
    
    select!(sites, sitevar => ByRow(x -> x) => :name, :site_iso_code, :source_id)

    savedataframe(db, sites, "sites")
    return nothing
end

"""
    add_variables(db::SQLite.DB, sourcename, path::String, dictionary)

Add the variables, including vocabularies for categorical variables.

* `db            `: SQlite database
* `sourcename    `: The name of data source, either "CHAMPS" or "COMSA".
* `path          `: The path to the raw de-identified data, assumes the data is extracted into "De_identified_data"
* `dictionary    `: The name of data dictionary.

"""
function add_variables(db::SQLite.DB, sourcename, path::String, dictionary)
    sourcename = uppercase(sourcename) # force source name to be upper cases
    domain = getdomain(db, sourcename)
    
    if ismissing(domain)  
        name="INSERT INTO domains(name,description) VALUES('"*sourcename*"','"*sourcename*" Level2 Data')"
        domain = DBInterface.lastrowid(DBInterface.execute(db, name))
    end
    # start with basic demographic data
    variables = read_variables(joinpath(path, sourcename), dictionary)
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
    read_variables(path, dictionary_filename)

Read a csv file listing variables, variable descriptions and data types in a dataset.

"""
function read_variables(dictionarypath, dictionary)
    file = joinpath(dictionarypath, "$dictionary.csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=';', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame
        vocabularies = Vector{Union{Vocabulary,Missing}}()
        for row in eachrow(df)
            l = lines(row.Description)
            if length(l) > 1
                push!(vocabularies, get_vocabulary(row.Column_Name, l))
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
    get_vocabulary(variable, l)::Vocabulary

Get a vocabulary, name of vocabulary in line 1 of l, vocabulary items (code and description) in subsequent lines, comma-separated
"""
function get_vocabulary(name, l)::Vocabulary
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
    import_dataset(db::SQLite.DB, datapath, sourcename, filename, transformation, ingest, description)

Insert dataset, datarows, and data into SQLite db and returns the datatset_id

## Parameters
  * `db            `: SQlite database
  * `datapath      `: The path to the raw de-identified data, assumes the data is extracted into "De_identified_data"
  * `sourcename    `: The name of data source, either "CHAMPS" or "COMSA".
  * `filename      `: The name of the raw data file to read.

  * `transformation`: The description of the transformation for the data ingest.
  * `ingest        `: The description of the data ingest.
  * `description   `: The dataset description
"""
function import_dataset(db::SQLite.DB, datapath, sourcename, filename, transformation, ingest, description)::Int64
    try
        SQLite.transaction(db)
        data = read_data(datapath, sourcename, filename)
        variables = lookup_variables(db, names(data), getnamedkey(db, "domains", sourcename, "domain_id"))
        var_lookup = Dict{String,Int64}(zip(variables.name, variables.variable_id))
        sql = """
        INSERT INTO datasets(name, date_created, description) 
        VALUES (@name, @date_created, @description);
        """
        stmt = DBInterface.prepare(db, sql)
        dataset_id = DBInterface.lastrowid(DBInterface.execute(stmt, (name=filename, date_created=Dates.format(today(), "yyyy-mm-dd"), description=description)))
        add_dataset_ingest(db, dataset_id, transformation, ingest)
        add_transformation_output(db, dataset_id, transformation)
        savedataframe(db, select(variables, [] => Returns(dataset_id) => :dataset_id, :variable_id), "dataset_variables")
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
        return dataset_id
    catch e
        SQLite.rollback(db)
        throw(e)
    end
end

"""
    read_data(path, sourcename, filename)::AbstractDataFrame

Returns a DataFrame, read raw de-identified COMSA/CHAMPS data

## Parameters
  
  * `sourcename    `: The name of data source, either "CHAMPS" or "COMSA".
  * `datapath      `: The path to the raw de-identified data, assumes the data is extracted into "De_identified_data"
  * `filename      `: The name of the raw data file to read.
"""

function read_data(datapath, sourcename, filename) #::AbstractDataFrame
    file = joinpath(datapath, sourcename, "De_identified_data", "$filename.csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=',', quotechar='"', dateformat="yyyy-mm-dd", decimal='.') |> DataFrame
        return df
    end
end

"""NEW UPDATES ABOVE THIS"""


"""
    lines(str)

Returns an array of lines in `str` 
"""
lines(str) = split(str, '\n')

"""
    getdomain(db::SQLite.DB, domainname)

Return the domain_id for domain named `domainname`
"""
function getdomain(db::SQLite.DB, domainname)
    return getnamedkey(db, "domains", domainname, Symbol("domain_id"))
end

"""
    dataset_to_dataframe(db::SQLite.DB, dataset)::AbstractDataFrame

Return a dataset with id `dataset` as a DataFrame in the wide format
"""
function dataset_to_dataframe(db::SQLite.DB, dataset)::AbstractDataFrame
    sql = """
    SELECT
        d.row_id,
        v.name variable,
        d.value
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
      JOIN variables v ON d.variable_id = v.variable_id
    WHERE r.dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    long = DBInterface.execute(stmt, (dataset = dataset)) |> DataFrame
    return unstack(long, :row_id, :variable, :value)
end
"""
    dataset_to_arrow(db, dataset, datapath)

Save a dataset in the arrow format
"""
function dataset_to_arrow(db, dataset, datapath)
    outputdir = joinpath(datapath, "arrowfiles")
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(db, dataset)
    Arrow.write(joinpath(outputdir, "$(datasetname(db,dataset)).arrow"), df, compress=:zstd)
end
"""
    dataset_to_csv(db, dataset, datapath)

Save a dataset in compressed csv format
"""
function dataset_to_csv(db, dataset, datapath)
    outputdir = joinpath(datapath, "csvfiles")
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(db, dataset)
    CSV.write(joinpath(outputdir, "$(datasetname(db,dataset)).gz"), df, compress=true)
end
"""
    datasetname(db, dataset)

Return dataset name, given the `dataset_id`
"""
function datasetname(db, dataset)
    sql = """
    SELECT
      name
    FROM datasets
    WHERE dataset_id = @dataset
    """
    stmt = DBInterface.prepare(db, sql)
    result = DBInterface.execute(stmt, (dataset = dataset))
    if isempty(result)
        return missing
    else
        df = DataFrame(result)
        return df[1, :name]
    end
end

"""
    getnamedkey(db, table, key, keycol)

 Return the integer key from table `table` in column `keycol` (`keycol` must be a `Symbol`) for key with name `key`
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
    get_variable(db, domain, name)

Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable(db, domain, name)
    sql = """
    SELECT
      variable_id id
    FROM variables
    WHERE domain_id = @domain
      AND name = @name
    """
    stmt = DBInterface.prepare(db, sql)
    result = DBInterface.execute(stmt, (domain=domain, name=name))
    if isempty(result)
        return missing
    else
        df = DataFrame(result)
        return df[1, :id]
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
    if ismissing(id)
        stmt = DBInterface.prepare(db, "INSERT INTO sources (name) VALUES (@name)")
        id = DBInterface.lastrowid(DBInterface.execute(stmt, (name = name)))
    end
    return id
end

"""
    add_protocols(db::SQLite.DB, datapath, sourcename)

Add CHAMPS and COMSA protocols
"""
function add_protocols(db::SQLite.DB, datapath, sourcename, protocolnames)
    sql = raw"""
    INSERT INTO protocols (name) VALUES (@name)
    """
    stmt = DBInterface.prepare(db, sql)

    for i in 1:length(protocolnames)
        DBInterface.execute(stmt, (name = protocolnames[i])
    end
    
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
        file = joinpath(datapath, sourcename, "Protocols", "$(row.name).pdf")
        if isfile(file)
            document = read(file)
            DBInterface.execute(stmt, (protocol_id=row.protocol_id, name="$(row.name).pdf", document=document))
            for site in eachrow(sites)
                DBInterface.execute(stmt2, (site_id=site.site_id, protocol_id=row.protocol_id))
            end
        end
    end
    return nothing
end

# convert a vector to data frame, fill with missing
function vec_to_df(x)
    y=[vcat(a, fill("", maximum(length.(x)) - length(a))) for a in x]
    df = DataFrame(mapreduce(permutedims, vcat, y), :auto)
    return df
end

# join ignore empty string
function join_wona(vec,sep)
    vec = filter((i) -> i !==missing, vec)
    vec = filter((i) -> i != "", vec)
    y = join(Array(vec),sep)
    return y
end


"""!!! NEW UPDATES ABOVE THIS LINE"""

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
        DBInterface.execute(stmt, (row_id=row.row_id, variable_id=variable_id, value=(isdate && !ismissing(row.value) ? Dates.format(row.value, "yyyy-mm-dd") : row.value)))
    end
    return nothing
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
    return nothing
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
    return nothing
end

"""
    link_deathrows(db::SQLite.DB, transformation, ingest, dataset_id, death_identifier, identifier_domain)

Insert records into `deathrows` table to link dataset `dataset_id` to `deaths` table. Limited to a specific ingest.
`death_identifier` is the variable in the dataset that corresponds to the `external_id` of the death.
"""
function link_deathrows(db::SQLite.DB, ingest, dataset, death_identifier)
    if !dataset_in_ingest(db, dataset, ingest)
        error("Dataset $dataset not part of data ingest $ingest")
    end
    sql = """
    INSERT OR IGNORE INTO death_rows (death_id, row_id)
    SELECT
        d.death_id,
        data.row_id
    FROM deaths d
        JOIN data ON d.external_id = data.value
        JOIN datarows r ON data.row_id = r.row_id
    WHERE d.data_ingestion_id = @ingest
       AND data.variable_id = @death_identifier
       AND r.dataset_id = @dataset
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (ingest=ingest, death_identifier=death_identifier, dataset=dataset))
    return nothing
end
function dataset_in_ingest(db, dataset, ingest)
    sql = """
        SELECT COUNT(*) n FROM ingest_datasets
        WHERE data_ingestion_id = @ingest
          AND dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (ingest=ingest, dataset=dataset)) |> DataFrame
    return nrow(df) > 0 && df[1, :n] > 0
end

makeparam(s) = "@" * s

"""
    savedataframe(db::SQLite.DB, df::AbstractDataFrame, table)

Save a DataFrame into an SQLite database, the names of the dataframe columns should be identical to the table column names in the database
"""
function savedataframe(db::SQLite.DB, df::AbstractDataFrame, table)
    colnames = names(df)
    paramnames = map(makeparam, colnames) #add @ to column names
    sql = "INSERT INTO $table ($(join(colnames, ", "))) VALUES ($(join(paramnames, ", ")));"
    stmt = DBInterface.prepare(db, sql)
    for row in eachrow(df)
        DBInterface.execute(stmt, NamedTuple(row))
    end
end

include("rdadatabase.jl")

end