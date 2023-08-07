module RDAIngest

using DataFrames
using CSV
using SQLite
using DBInterface
using ConfigEnv
using CSV
using Dates
using Arrow

export opendatabase, get_table, addsource, getsource, createdatabase, getnamedkey,
    ingest_source,
    add_champs_sites, add_champs_protocols, read_champs_variables,
    add_dataingest, add_transformation, ingest_champs_deaths, add_champs_variables, import_champs_dataset,
    link_deathrows, ingest_champs, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv, savedataframe,
    ingest_champs_labtac


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

abstract type AbstractSource end
Base.@kwdef struct CHAMPSSource <: AbstractSource
    name::String = "CHAMPS"
    datafolder::String = "De_identified_data"
    site_data::String = "CHAMPS_deid_basic_demographics"
    site_col::String = "site_iso_code"
    protocolfolder::String = "Protocols"
    protocols::Dict{String, String} = Dict("CHAMPS Mortality Surveillance Protocol" => "CHAMPS-Mortality-Surveillance-Protocol-v1.3",
                                 "CHAMPS Social Behavioral Science Protocol" => "CHAMPS-Social-Behavioral-Science-Protocol-v1.0")
    instrumentfolder::String = "Instruments"
    instruments::Dict{String,String} = Dict("CHAMPS Verbal Autopsy Questionnaire" => "cdc_93759_DS9.pdf")
    variables::Vector{String} = ["Format_CHAMPS_deid_basic_demographics", 
                                 "Format_CHAMPS_deid_verbal_autopsy", 
                                 "Format_CHAMPS_deid_decode_results",
                                 "Format_CHAMPS_deid_tac_results", 
                                 "Format_CHAMPS_deid_lab_results"]
    datasets::Vector{String} = ["CHAMPS_deid_basic_demographics", 
                                "CHAMPS_deid_verbal_autopsy", 
                                "CHAMPS_deid_decode_results",
                                "CHAMPS_deid_tac_results", 
                                "CHAMPS_deid_lab_results"]
    deaths::String = "CHAMPS_deid_basic_demographics"
    death_idvar::String = "champs_deid"
    extension::String = "csv"
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'
end
Base.@kwdef struct COMSASource <: AbstractSource
    name::String = "COMSA"
    datafolder::String = "De_identified_data"
    site_data::String = "Comsa_death_20230308"
    site_col::String = "provincia"
    protocolfolder::String = "Protocols"
    protocols::Dict{String,String} = Dict("Countrywide Mortality Surveillance for Action (COMSA) Mozambique (Formative Research)" => 
                                          "COMSA-FR-protocol_version-1.0_05July2017.pdf",
                                          "Countrywide Mortality Surveillance for Action (COMSA) Mozambique" => 
                                          "COMSA-protocol_without-FR_version-1.1_15June2017_clean_REVISED.pdf")
    instrumentfolder::String = "Questionnaires"
    instruments::Dict{String,String} = Dict("Pregnancy Version 2, June 23, 2017" => "1.Pregnancy.pdf",
        "Pregnancy Outcome Version 2, June 23, 2017" => "2.Preg-outcome_2-23.pdf",
        "Death Version 2, June 23, 2017" => "3.Death_2-23.pdf",
        "Verbal and Social Autopsy - Adults" => "5a_2018_COMSA_VASA_ADULTS-EnglishOnly_01262019_clean.pdf",
        "Verbal and Social Autopsy - Child (4 weeks to 11 years)" => "5a_2018_COMSA_VASA_CHILD-EnglishOnly_12152018Clean.pdf",
        "Verbal and Social Autopsy - Stillbirth, Neonatal" => "5a_2018_COMSA_VASA_SB_NN-EnglishOnly_12152018Clean.pdf",
        "Verbal and Social Autopsy - General Information" => "5a_2018_COMSA_VASA-GenInfo_English_06272018_clean.pdf",
        "Household Members Version 2, June 23, 2017" => "Household-members_2-23.pdf")
    variables::Vector{String} = ["Format_Comsa_death_20230308",
        "Format_Comsa_WHO_VA_20230308"]
    datasets::Vector{String} = ["Comsa_death_20230308",
        "Comsa_WHO_VA_20230308.csv"]
    deaths::String = "Comsa_WHO_VA_20230308"
    death_idvar::String = "comsa_id"
    extension::String = "csv"
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "mmm dd, yyyy"
    decimal::Char = '.'
end

function ingest_source(source::AbstractSource, dbpath, dbname, datapath)
    db = opendatabase(dbpath, dbname)
    try
        source_id = addsource(db, source.name)
        add_sites(source, db, source_id, datapath)
        # Add instruments
        # Add Protocols
        # Add Ethics
        # Add variables
    finally
        close(db)
    end
end
function add_sites(source::CHAMPSSource, db::SQLite.DB, sourceid::Int64, datapath::String)
    sites = read_sitedata(source, sourceid, datapath)
    select!(sites, source.site_col => ByRow(x -> x) => :name, source.site_col, :source_id)
    savedataframe(db, sites, "sites")
    return nothing
end
function add_sites(source::COMSASource, db::SQLite.DB, sourceid::Int64, datapath::String)
    sites = read_sitedata(source, sourceid, datapath)
    select!(sites, source.site_col => ByRow(x -> x) => :name, [] => Returns("MW") => :site_iso_code, :source_id)
    savedataframe(db, sites, "sites")
    return nothing
end
function read_sitedata(source::AbstractSource, sourceid::Int64, datapath)
    df = read_data(joinpath(datapath,source.name,source.datafolder), source.site_data, 
                    extension=source.extension, delim=source.delim, quotechar=source.quotechar, 
                    dateformat=source.dateformat, decimal=source.decimal)
    sites = combine(groupby(df, source.site_col), nrow => :n)
    insertcols!(sites, 1, :source_id => sourceid)
    return sites    
end
"""
Adding CHAMPS lab and tac

ingest_champs_labtac(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description)

This is modified based on main ingest_champs function to ingest the laboratory or TAC results of CHAMPS Level 2 data distribution.

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
function ingest_champs_labtac(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description, dictionarypath)
    db = opendatabase(dbpath, dbname)
    try
        champs = addsource(db, "CHAMPS")
        ingest = add_dataingest(db, champs, today(), ingest)
        transformation = add_transformation(db, 1, 1, transformation, code_reference, today(), author)
        add_champs_variables(db, dictionarypath, "Format_CHAMPS_deid_tac_results")
        add_champs_variables(db, dictionarypath, "Format_CHAMPS_deid_lab_results")
        tac_ds = import_champs_dataset(db, transformation, ingest, datapath, "CHAMPS_deid_tac_results", description)
        lab_ds = import_champs_dataset(db, transformation, ingest, datapath, "CHAMPS_deid_lab_results", description)
        domain = getnamedkey(db, "domains", "CHAMPS", Symbol("domain_id"))
        death_idvar = get_variable(db, domain, "champs_deid")
        link_deathrows(db, ingest, tac_ds, death_idvar) #CHAMPS_deid_tac_results
        link_deathrows(db, ingest, lab_ds, death_idvar) #CHAMPS_deid_lab_results
        return nothing
    finally
        close(db)
    end
end

"""
   ingest_champs(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description)

!!! note
    The database should exist and be in the RDA format created by the [`createdatabase`](@ref) function

This is the main function to ingest a CHAMPS Level 2 data distribution. The current version does not ingest the laboratory or TAC results.

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

## Method

1. A **CHAMPS** source is created if it doesn't exist, using function [`addsource`](@ref).
2. The CHAMPS sites are extracted from the `CHAMPS_deid_basic_demographics` dataset and saved using the [`add_champs_sites`](@ref) function.
3. The CHAMPS protocol are added as pdfs from a sub-directory `CHAMPS\\Protocols` in `datapath` using the [`add_champs_protocols`](@ref) function.
4. A data ingest is created using the [`add_dataingest`](@ref) function.
5. A transformation reprenting the complete data ingest is created using the [`add_transformation`](@ref) function.
6. A entry for each death is inserted in the `deaths` table, for each row the `CHAMPS_deid_basic_demographics` dataset using the [`ingest_champs_deaths`](@ref) function.
7. The CHAMPS dataset variables are imported from a manually created data dictionary file as described in the [`add_champs_variables`](@ref) function.
8. The CHAMPS datasets are imported using the [`import_champs_dataset`](@ref) function.
9. The CHAMPS deaths are linked to the dataset rows containing the detail data about each death in the CHAMPS data distribution, using the function [`link_deathrows`](@ref)

"""
function ingest_champs(dbpath, dbname, datapath, ingest, transformation, code_reference, author, description, dictionarypath)
    db = opendatabase(dbpath, dbname)
    try
        champs = addsource(db, "CHAMPS")
        add_champs_sites(db, datapath)
        add_champs_protocols(db, datapath)
        ingest = add_dataingest(db, champs, today(), ingest)
        transformation = add_transformation(db, 1, 1, transformation, code_reference, today(), author)
        ingest_champs_deaths(db, ingest, datapath)
        add_champs_variables(db, dictionarypath, "Format_CHAMPS_deid_basic_demographics")
        add_champs_variables(db, dictionarypath, "Format_CHAMPS_deid_verbal_autopsy")
        add_champs_variables(db, dictionarypath, "Format_CHAMPS_deid_decode_results")
        basic_ds = import_champs_dataset(db, transformation, ingest, datapath, "CHAMPS_deid_basic_demographics", description)
        va_ds = import_champs_dataset(db, transformation, ingest, datapath, "CHAMPS_deid_verbal_autopsy", description)
        decode_ds = import_champs_dataset(db, transformation, ingest, datapath, "CHAMPS_deid_decode_results", description)
        domain = getnamedkey(db, "domains", "CHAMPS", Symbol("domain_id"))
        death_idvar = get_variable(db, domain, "champs_deid")
        link_deathrows(db, ingest, basic_ds, death_idvar) #CHAMPS_deid_basic_demographics
        link_deathrows(db, ingest, va_ds, death_idvar) #CHAMPS_deid_verbal_autopsy
        link_deathrows(db, ingest, decode_ds, death_idvar) #CHAMPS_deid_decode_results
        return nothing
    finally
        close(db)
    end
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
    if ismissing(id)  # insert source
        stmt = DBInterface.prepare(db, "INSERT INTO sources (name) VALUES (@name)")
        id = DBInterface.lastrowid(DBInterface.execute(stmt, (name = name)))
    end
    return id
end
"""
    read_data(path, name)::AbstractDataFrame

Returns a DataFrame with the data, from a data collection
path = path to the data collection
name = name of the data file, .csv extension assumed
"""
function read_data(path, name; extension="csv", delim=',', quotechar='"', dateformat="yyyy-mm-dd", decimal='.')::AbstractDataFrame
    #Nov 11, 2020
    file = joinpath(path, "$name.$extension")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=delim, quotechar=quotechar, dateformat=dateformat, decimal=decimal) |> DataFrame
        return df
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
    return nothing
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
    file = joinpath(path, "$file.csv")
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
"""
    ingest_champs_deaths(db::SQLite.DB, ingest::Int64, path::String)

INSERT CHAMPS deaths into the deaths table, for a specified data ingest. 
"""
function ingest_champs_deaths(db::SQLite.DB, ingest::Int64, path::String)
    deaths = read_champs_data(path, "CHAMPS_deid_basic_demographics")
    sites = DBInterface.execute(db, "SELECT * FROM sites WHERE source_id = $(getsource(db, "CHAMPS"));") |> DataFrame
    sitedeaths = innerjoin(deaths, sites, on=:site_iso_code, matchmissing=:notequal)
    savedataframe(db, select(sitedeaths, :site_id, :champs_deid => :external_id, [] => Returns(ingest) => :data_ingestion_id, copycols=false), "deaths")
    return nothing
end
"""
    getdomain(db::SQLite.DB, domainname)

Return the domain_id for domain named `domainname`
"""
function getdomain(db::SQLite.DB, domainname)
    return getnamedkey(db, "domains", domainname, Symbol("domain_id"))
end

"""
    add_champs_variables(db::SQLite.DB, path::String)

Save the CHAMPS variables, including vocabularies for categorical variables.
!!! note
    Ingested data is not checked to ensure that categorical variable values conform to the vocabulary, in fact in the provided data there are deviations, mostly in letter case. Common categories, such as the verbal autopsy indicators are also not converted to categorical values.
"""
function add_champs_variables(db::SQLite.DB, path::String, dictionary::String)
    domain = getdomain(db, "CHAMPS")
    if ismissing(domain)  # insert CHAMPS domain
        domain = DBInterface.lastrowid(DBInterface.execute(db, "INSERT INTO domains(name,description) VALUES('CHAMPS','CHAMPS Level2 Data')"))
    end
    # start with basic demographic data
    variables = read_champs_variables(joinpath(path, "CHAMPS"), dictionary)
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

Insert dataset, datarows, and data into SQLite db and returns the datatset_id
"""
function import_champs_dataset(db::SQLite.DB, transformation, ingest, path, dataset_name, description)::Int64
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