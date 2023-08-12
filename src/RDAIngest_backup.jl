module RDAIngest

using DataFrames
using CSV
using SQLite
using DBInterface
using ConfigEnv
using CSV
using Dates
using Arrow
using DataStructures
#using OrderedCollections
using XLSX

export ingest_source, ingest_data,
    add_source, get_source, get_namedkey, get_variable, get_domain, 
    add_sites, #read_sitedata, 
    add_protocols, add_instruments, add_ethics, 
    add_variables, add_vocabulary, read_variables, get_vocabulary,
    ingest_deaths, import_datasets,link_deathrows,
    add_ingestion, add_transformation, add_dataset_ingestion, add_transformation_output,
    add_data_column, lookup_variables, dataset_in_ingest,
    read_data, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv, get_datasetname,
    savedataframe,
    AbstractSource, CHAMPSSource, COMSASource,

    createdatabase, opendatabase #, 
    #get_table, createsources, createprotocols, createtransformations,
    #createvariables, createdatasets, createinstruments, createdeaths
            


"""
Structs
"""

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
        country_col::String = "site_iso_code"
        country_iso2::String = nothing
    
        # Protocol - assume file extension pdf
        protocolfolder::String = "Protocols"
        protocols::Dict{String, String} = Dict("CHAMPS Mortality Surveillance Protocol" => "CHAMPS-Mortality-Surveillance-Protocol-v1.3",
                                     "CHAMPS Social Behavioral Science Protocol" => "CHAMPS-Social-Behavioral-Science-Protocol-v1.0",
                                     "Determination of DeCoDe Diagnosis Standards" => "CHAMPS-Diagnosis-Standards",
                                     "CHAMPS Manual version 1.0" => "CHAMPS-Manual-v3",
                                     "CHAMPS Online De-Identified Data Transfer Agreement" => "CHAMPS Online De-Identified DTA")
    
        # Instrument - specify file extension in name
        instrumentfolder::String = "Instruments"
        instruments::Dict{String,String} = Dict("CHAMPS Verbal Autopsy Questionnaire" => "cdc_93759_DS9.pdf")
        
        # Ethics - assume file extension pdf
        # Document dictionaries need to match with comittee and reference
        ethicsfolder::String = "Ethics"
        #ethics::OrderedDict{String, Dict{String,String}} = OrderedDict("IRB 1" => Dict("IRB 1"=>"IRB1","IRB 2"=>"IRB2"),
        #                                                       "IRB 2" => Dict("IRB 3"=>"IRB1","IRB 4"=>"IRB4"))
        #ethics_committee::Vector{String} = ["Emory University","Country IRB"]
        #ethics_reference::Vector{String} = ["TBD","TBD"]

        ethics::Dict{String,Vector{String}} = Dict("Emory"=>["ref1","doc1"],"Emory"=>["ref2","doc2"],
                                                "Country" => ["ref3","doc3"])
        
        # Source-released data dictionary
        dictfolder::String = "De_identified_data"
        dict::Vector{String} = ["CHAMPS De-Identified Data Set Description v4.2"]
        dict_extension::String = "pdf"
    
        # Variables
        variables::Vector{String} = ["Format_CHAMPS_deid_basic_demographics", 
                                     "Format_CHAMPS_deid_verbal_autopsy", 
                                     "Format_CHAMPS_deid_decode_results",
                                     "Format_CHAMPS_deid_tac_results", 
                                     "Format_CHAMPS_deid_lab_results"]
        dic_delim::Char = ';'
        dic_quotechar::Char = '"'
        dic_dateformat::String = "yyyy-mm-dd"
        dic_decimal::Char = '.'
        
        # Data
        datasets::Vector{String} = ["CHAMPS_deid_basic_demographics", 
                                    "CHAMPS_deid_verbal_autopsy", 
                                    "CHAMPS_deid_decode_results",
                                    "CHAMPS_deid_tac_results", 
                                    "CHAMPS_deid_lab_results"]
        deaths::String = "CHAMPS_deid_basic_demographics"
        death_idcol::String = "champs_deid"
        extension::String = "csv"
        delim::Char = ','
        quotechar::Char = '"'
        dateformat::String = "yyyy-mm-dd"
        decimal::Char = '.'

        # Metadata
        #ingestion::String = "CHAMPS Level-2 Data accessed 20230518"
        #transformation::String = "Ingest of CHAMPS de-identified data"
        #code_reference::String = "Multiple dispatch testing"
        #author::String = "Yue Chu"
        #description::String = "Raw CHAMPS level 2 data release v4.2"
    
end

Base.@kwdef struct COMSASource <: AbstractSource
        name::String = "COMSA"
        datafolder::String = "De_identified_data"
        
        site_data::String = "Comsa_death_20230308"
        site_col::String = "provincia"
        country_col::String = nothing
        country_iso2::String = "MW"
        
        # Protocol - assume file extension pdf
        protocolfolder::String = "Protocols"
        protocols::Dict{String,String} = Dict("Countrywide Mortality Surveillance for Action (COMSA) Mozambique (Formative Research)" => 
                                              "COMSA-FR-protocol_version-1.0_05July2017",
                                              "Countrywide Mortality Surveillance for Action (COMSA) Mozambique" => 
                                              "COMSA-protocol_without-FR_version-1.1_15June2017_clean_REVISED",
                                              "COMSA Data Access Plan"=> "COMSA-Data-Access-Plan",
                                              "COMSA Data Use Agreement"=> "Data Use Agreement (DUA) - Comsa")
        
        # Instrument - specify file extension
        instrumentfolder::String = "Instruments"
        instruments::Dict{String,String} = Dict("Pregnancy Version 2, June 23, 2017" => "1.Pregnancy.pdf",
            "Pregnancy Outcome Version 2, June 23, 2017" => "2.Preg-outcome_2-23.pdf",
            "Death Version 2, June 23, 2017" => "3.Death_2-23.pdf",
            "Verbal and Social Autopsy - Adults" => "5a_2018_COMSA_VASA_ADULTS-EnglishOnly_01262019_clean.pdf",
            "Verbal and Social Autopsy - Child (4 weeks to 11 years)" => "5a_2018_COMSA_VASA_CHILD-EnglishOnly_12152018Clean.pdf",
            "Verbal and Social Autopsy - Stillbirth, Neonatal" => "5a_2018_COMSA_VASA_SB_NN-EnglishOnly_12152018Clean.pdf",
            "Verbal and Social Autopsy - General Information" => "5a_2018_COMSA_VASA-GenInfo_English_06272018_clean.pdf",
            "Household Members Version 2, June 23, 2017" => "Household-members_2-23.pdf")
    
        # Ethics - assume file extension pdf
        ethicsfolder::String = "Ethics"
        ethics::OrderedDict{String, Dict{String,String}} = OrderedDict("IRB 1" => Dict("IRB 1"=>"IRB1"),
                                                               "IRB 2" => Dict("IRB 3"=>"IRB1"))
        ethics_committee::Vector{String} = ["National Health Bioethics Committee of Mozambique",
                                            "Johns Hopkins Bloomberg School of Public Health"]
        ethics_reference::Vector{String} = ["REF 608/CNBS/17", 
                                            "IRB#7867"]
    
        # Source-released data dictionary
        dictfolder::String = "De_identified_data"
        dict::Vector{String} = ["Comsa_data_dictionary_20190909"]
        dict_extension::String = "xlsx"
    
        # Variables
        variables::Vector{String} = ["Format_Comsa_WHO_VA_20230308"]
        dic_delim::Char = ';'
        dic_quotechar::Char = '"'
        dic_dateformat::String = "yyyy-mm-dd"
        dic_decimal::Char = '.'
        
        # Data
        datasets::Vector{String} = ["Comsa_WHO_VA_20230308"]
        deaths::String = "Comsa_WHO_VA_20230308"
        death_idcol::String = "comsa_id"
        extension::String = "csv"
        delim::Char = ','
        quotechar::Char = '"'
        dateformat::String = "dd-u-yyyy" #"mmm dd, yyyy"
        decimal::Char = '.'

        #= # Metadata
        ingestion::String = "COMSA Level-2 Data accessed 20230518"
        transformation::String = "Ingest of COMSA de-identified VA data"
        code_reference::String = "Multiple dispatch testing"
        author::String = "Yue Chu"
        description::String = "Raw COMSA level 2 data release v20230308"
        =#
    

end



"""
Core functions
"""
   
"""
Step 1: 
Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 

    ingest_source(db::SQLite.DB, name)

datapath: root folder with data from all sources [DATA_INGEST_PATH]
"""

function ingest_source(source::AbstractSource, dbpath::String, dbname::String, 
                        datapath::String, dictionarypath::String)
    db = opendatabase(dbpath, dbname)
    try
        source_id = add_source(source,db)
        
        # Add sites and country iso2 codes
        add_sites(source, db, source_id, datapath)
        
        # Add instruments
        add_instruments(source, db, datapath)
        
        # Add Protocols
        add_protocols(source, db, datapath)

        # Add Ethics
        add_ethics(source, db, datapath)

        # Add variables
        add_variables(source, db, dictionarypath)

    finally
        close(db)
    end
end


"""
Step 2: 
Add ingestion and transformation info, import datasets, and link datasets to deaths

ingest_data(source::AbstractSource, dbpath, dbname, datapath,
            ingestion, transformation, code_reference, author, description)

"""

function ingest_data(source::AbstractSource, dbpath::String, dbname::String, datapath::String,
                     ingestion::String, transformation::String, 
                     code_reference::String, author::String, description::String
                     )
    db = opendatabase(dbpath, dbname)
    try
        source_id = get_source(db, source.name)
        
        # Add metadata
        ingestion_id = add_ingestion(db, source_id, today(), ingestion)
        transformation_id = add_transformation(db, 1, 1, transformation, code_reference, today(), author)
        #ingestion_id = add_ingestion(db, source_id, today(), source.ingestion)
        #transformation_id = add_transformation(db, 1, 1, source.transformation, source.code_reference, today(), source.author)

        # Import deaths
        ingest_deaths(source, db, datapath, ingestion_id)
        domain_id = get_namedkey(db, "domains", source.name, Symbol("domain_id"))
        death_idvar = get_variable(db, domain_id, source.death_idcol)
        
        # Import datasets and link to deaths
        for filename in source.datasets
            # Import datasets
            dateset_id = import_datasets(source, db, datapath, filename, 
                            transformation_id, ingestion_id, description #source.description
                            )
            
            # Link to deathrows
            link_deathrows(db, ingestion_id, dateset_id, death_idvar) 
        end
        
    finally
        close(db)
    end
end



"""
    add_source(source::AbstractSource, db::SQLite.DB)

Add source `name` to the sources table, and returns the `source_id`
"""
function add_source(source::AbstractSource, db::SQLite.DB)
    id = get_source(db, source.name)
    if ismissing(id)  # insert source
        stmt = DBInterface.prepare(db, "INSERT INTO sources (name) VALUES (@name)")
        id = DBInterface.lastrowid(DBInterface.execute(stmt, (name = source.name)))
    end
    return id
end

"""
    get_source(db::SQLite.DB, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function get_source(db::SQLite.DB, name)
    return get_namedkey(db, "sources", name, :source_id)
end

"""
    get_namedkey(db, table, key, keycol)

 Return the integer key from table `table` in column `keycol` (`keycol` must be a `Symbol`) for key with name `key`
"""
function get_namedkey(db, table, key, keycol)
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
    get_domain(db::SQLite.DB, domainname)

Return the domain_id for domain named `domainname`
"""
function get_domain(db::SQLite.DB, domainname)
    return get_namedkey(db, "domains", domainname, Symbol("domain_id"))
end

"""
    add_sites(source::AbstractSource, db::SQLite.DB, sourceid::Int64, datapath::String)

Add sites and country iso2 codes to sites table
"""

function add_sites(source::CHAMPSSource, db::SQLite.DB, sourceid::Int64, datapath::String)
    df = read_data(joinpath(datapath,source.name,source.datafolder), source.site_data, 
                    extension=source.extension, delim=source.delim, quotechar=source.quotechar, 
                    dateformat=source.dateformat, decimal=source.decimal)
    sites = combine(groupby(df, source.site_col), nrow => :n)
    insertcols!(sites, 1, :source_id => sourceid)

    select!(sites, 
            Symbol(source.site_col) => ByRow(x -> x) => :site_name, 
            Symbol(source.country_col) => ByRow(x -> x) => :country_iso2, 
            :source_id)
    savedataframe(db, sites, "sites")
    println("Site names and country iso2 codes ingested.")
    return nothing
end
function add_sites(source::COMSASource, db::SQLite.DB, sourceid::Int64, datapath::String)
    df = read_data(joinpath(datapath,source.name,source.datafolder), source.site_data, 
                    extension=source.extension, delim=source.delim, quotechar=source.quotechar, 
                    dateformat=source.dateformat, decimal=source.decimal)
    sites = combine(groupby(df, source.site_col), nrow => :n)
    insertcols!(sites, 1, :source_id => sourceid) 

    if (source.country_iso2=="" || !isdefined(source,Symbol("country_iso2")))
        error("Country iso2 code not provided in data and not specified.")
    else 
        select!(sites, 
                Symbol(source.site_col) => ByRow(x -> x) => :site_name, 
                [] => Returns(source.country_iso2) => :country_iso2, 
                :source_id)
        savedataframe(db, sites, "sites")
        println("Site names and country iso2 codes ingested.")
    end
    return nothing
end


"""
Alternative:
If country iso2 code doesn't exist for the study, specify in site source.

function add_sites(source::AbstractSource, db::SQLite.DB, sourceid::Int64, datapath::String)
    df = read_data(joinpath(datapath,source.name,source.datafolder), source.site_data, 
                    extension=source.extension, delim=source.delim, quotechar=source.quotechar, 
                    dateformat=source.dateformat, decimal=source.decimal)
    sites = combine(groupby(df, source.site_col), nrow => :n)
    insertcols!(sites, 1, :source_id => sourceid)

    if (!isdefined(source,Symbol("country_col")) || source.country_col=="")
        if (source.country_iso2=="" || !isdefined(source,Symbol("country_iso2"))
            error("Country iso2 code not provided in data and not specified.")
        else 
            select!(sites, 
                    Symbol(source.site_col) => ByRow(x -> x) => :site_name, 
                    [] => Returns(source.country_iso2) => :country_iso2, 
                    :source_id)
        end
    else
        select!(sites, 
        Symbol(source.site_col) => ByRow(x -> x) => :site_name, 
        Symbol(source.country_col) => ByRow(x -> x) => :country_iso2, 
        :source_id)
    end

    savedataframe(db, sites, "sites")
    println("Site names and country iso2 codes ingested.")
    return nothing
end
"""


"""
    add_protocols(source::AbstractSource, db::SQLite.DB, datapath::String)

Add protocols
"""

function add_protocols(source::AbstractSource, db::SQLite.DB, datapath::String)

    # Insert protocol names
    sql = raw"""
    INSERT INTO protocols (name, description) VALUES (@name, @description)
    """
    stmt_name = DBInterface.prepare(db, sql)

    # Insert protocol documents
    sql = raw"""
    INSERT INTO protocol_documents (protocol_id, name, document) VALUES (@protocol_id, @name, @document)
    """
    stmt_doc = DBInterface.prepare(db, sql)

    # Insert site protocols 
    sql = raw"""
    INSERT INTO site_protocols (site_id, protocol_id) VALUES (@site_id, @protocol_id)
    """
    stmt_site = DBInterface.prepare(db, sql)
    
    sites = DBInterface.execute(db, "SELECT * FROM sites") |> DataFrame

    for (key, value) in source.protocols
        
        # Add protocol
        DBInterface.execute(stmt_name, (name = "$value", description = "$key"))

        # Get protocol id
        row = DBInterface.execute(db, "SELECT * FROM protocols WHERE name = '$value'") |> DataFrame

        # Add site protocol
        for site in eachrow(sites)
            DBInterface.execute(stmt_site, (site_id=site.site_id, protocol_id=row.protocol_id))
        end
        
        # Add protocol documents
        file = joinpath(joinpath(datapath,source.name,source.protocolfolder), "$value.pdf")
        if !(isfile(file))
            println("Protocol document $value does not exist.")
        else
            document = read(file)
            DBInterface.execute(stmt_doc, 
                                (protocol_id=row.protocol_id, name="$value.pdf", document=document))
            println("Protocol document $value.pdf ingested.")
        end
    end
    return nothing
end


"""
    add_instruments(source::AbstractSource, db::SQLite.DB, datapath::String)

Add survey instruments
"""

function add_instruments(source::AbstractSource, db::SQLite.DB, datapath::String)

    # Insert instrument names
    sql = raw"""
    INSERT INTO instruments (name, description) VALUES (@name, @description)
    """
    stmt_name = DBInterface.prepare(db, sql)

    # Insert instrument documents
    sql = raw"""
    INSERT INTO instrument_documents (instrument_id, name, document) VALUES (@instrument_id, @name, @document)
    """
    stmt_doc = DBInterface.prepare(db, sql)

    for (key, value) in source.instruments

        # Add instruments
        DBInterface.execute(stmt_name, (name = "$value", description = "$key"))

        # Get instrument id
        row = DBInterface.execute(db, "SELECT * FROM instruments WHERE name = '$value'") |> DataFrame

        # Add instrument documents
        file = joinpath(joinpath(datapath,source.name,source.instrumentfolder), "$value")
        if !(isfile(file))
            println("Instrument document $value does not exist.")
        else
            document = read(file)
            DBInterface.execute(stmt_doc, 
                                (instrument_id=row.instrument_id, name="$value", document=document))
            println("Instrument document $value ingested.")
        end
    end
    return nothing
end


"""
    add_ethics(source::AbstractSource, db::SQLite.DB, datapath::String)

Ethics document, committee and reference need to be in matching order
"""

function add_ethics(source::AbstractSource, db::SQLite.DB, datapath::String)

    if !(length(source.ethics)==length(source.ethics_committee)==length(source.ethics_reference))
        error("Numbers of ethics document dictionaries, ethics committee, ethics reference does not match. They need to be of same length and in the matching orders.")
    end

    # Insert ethics names
    sql = raw"""
    INSERT INTO ethics (name, ethics_committee, ethics_reference) VALUES (@name, @ethics_committee, @ethics_reference)
    """
    stmt_name = DBInterface.prepare(db, sql)

    for i in 1:length(source.ethics)
        DBInterface.execute(stmt_name, (name = collect(keys(source.ethics))[i], 
                                        ethics_committee = source.ethics_committee[i],
                                        ethics_reference = source.ethics_reference[i]))
    end

    # Insert ethics documents
    sql = raw"""
    INSERT INTO ethics_documents (ethics_id, name, description, document) VALUES (@ethics_id, @name, @description, @document)
    """
    stmt_doc = DBInterface.prepare(db, sql)
    
    for (key, value) in source.ethics
        for (key2, value2) in value
        
        file = joinpath(joinpath(datapath,source.name,source.ethicsfolder), "$value2")
            if !(isfile(file))
                println("Ethics document $value2 does not exist.")
            else
                # Get ethics id
                row = DBInterface.execute(db, "SELECT * FROM ethics WHERE name = '$key'") |> DataFrame

                document = read(file)
                DBInterface.execute(stmt_doc, 
                                    (ethics_id=row.ethics_id, name="$value2", description = "$key2", document=document))
                println("Ethics document $value2 ingested.")
            end
        end
    end
    return nothing
end


"""
    add_variables(source::AbstractSource, db::SQLite.DB, dictionarypath::String)

"""
function add_variables(source::AbstractSource, db::SQLite.DB, dictionarypath::String)
    
    domain = get_domain(db, source.name)
    
    if ismissing(domain)  
        # Insert domain
        sql = raw"""
        INSERT INTO domains (name, description) VALUES (@name, @description)
        """
        stmt = DBInterface.prepare(db, sql)
        domain = DBInterface.lastrowid(DBInterface.execute(stmt, 
                                       (name = source.name, description = "$(source.name) Level2 Data")))
    end

    #variable insert SQL
    sql = raw"""
    INSERT INTO variables (domain_id, name, value_type_id, vocabulary_id, description, note)
    VALUES (@domain_id, @name, @value_type_id, @vocabulary_id, @description, @note)
    ON CONFLICT DO UPDATE
      SET vocabulary_id = excluded.vocabulary_id,
          description = excluded.description,
          note = excluded.note 
      WHERE variables.vocabulary_id IS NULL OR variables.description IS NULL OR variables.note IS NULL;
    """
    stmt = DBInterface.prepare(db, sql)

    # Add variables
    for i in 1:length(source.variables)
        variables = read_variables(source,dictionarypath,source.variables[i])
        insertcols!(variables, 1, :domain_id => domain)

        for row in eachrow(variables)
            id = missing
            if !ismissing(row.Vocabulary)
                id = add_vocabulary(db, row.Vocabulary)
            end
            DBInterface.execute(stmt, (domain_id=row.domain_id, name=row.Column_Name, value_type_id=row.DataType, vocabulary_id=id, description=row.Description, note=row.Note))
        end

        println("Variables from $(source.variables[i]) ingested.")
    end
    return nothing
end


"""
    add_vocabulary(db::SQLite.DB, vocabulary::Vocabulary)

Insert a vocabulary and its items into a RDA database, returns the vocabulary_id of the inserted vocabulary
"""

function add_vocabulary(db::SQLite.DB, vocabulary::Vocabulary)
    id = get_namedkey(db, "vocabularies", vocabulary.name, "vocabulary_id")
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
read_variables(dictionarypath::String, dictionary::String,
               delim::Char, quotechar::Char, dateformat::String,decimal::Char)

Read a csv file listing variables, variable descriptions and data types in a dataset.

"""

function read_variables(source::AbstractSource, dictionarypath::String, dictionary::String)
    file = joinpath(dictionarypath, source.name, "$dictionary.csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=source.dic_delim, quotechar=source.dic_quotechar, 
                            dateformat=source.dic_dateformat, decimal=source.dic_decimal) |> DataFrame
        vocabularies = Vector{Union{Vocabulary,Missing}}()
        for row in eachrow(df)
            #print(row)
            if !ismissing(row.Description) && length(lines(row.Description))>1
                l = lines(row.Description)
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
ingest_deaths(db::SQLite.DB, ingest::Int64, datapath::String, sourcename, filename, sitevar, idvar)

INSERT deaths into the deaths table, for a specified data ingest. 

"""

function ingest_deaths(source::AbstractSource, db::SQLite.DB, datapath::String, ingest_id::Int64)

    deaths = read_data(joinpath(datapath,source.name,source.datafolder), 
                        source.deaths, 
                        extension=source.extension, delim=source.delim, quotechar=source.quotechar, 
                        dateformat=source.dateformat, decimal=source.decimal)

    source_id = get_source(db, source.name)
    sites = DBInterface.execute(db, "SELECT * FROM sites WHERE source_id = $source_id") |> DataFrame
    
    sitedeaths = innerjoin(transform!(deaths, Symbol(source.site_col) => :site_name), 
                           sites, on=:site_name, matchmissing=:notequal)
    savedataframe(db, select(sitedeaths, :site_id, Symbol(source.death_idcol) => :external_id, 
                            [] => Returns(ingest_id) => :data_ingestion_id, copycols=false), 
                  "deaths")

    println("Death data $(source.deaths) ingested.")
    return nothing
end


"""
import_datasets(source::AbstractSource, db::SQLite.DB, datapath, transformation, ingest, description)::Int64

Insert datasets into SQLite db and returns the datatset_id

"""

function import_datasets(source::AbstractSource, db::SQLite.DB, datapath::String, filename::String, transformation_id::Int64, ingestion_id::Int64, description::String)::Int64
    try
        SQLite.transaction(db)
        data = read_data(joinpath(datapath,source.name,source.datafolder), 
                        filename, 
                        extension=source.extension, delim=source.delim, quotechar=source.quotechar, 
                        dateformat=source.dateformat, decimal=source.decimal)

        variables = lookup_variables(db, names(data), get_namedkey(db, "domains", source.name, "domain_id"))
        var_lookup = Dict{String,Int64}(zip(variables.name, variables.variable_id))
        sql = """
        INSERT INTO datasets(name, date_created, description) 
        VALUES (@name, @date_created, @description);
        """
        stmt = DBInterface.prepare(db, sql)
        dataset_id = DBInterface.lastrowid(DBInterface.execute(stmt, (name=filename, date_created=Dates.format(today(), "yyyy-mm-dd"), description=description)))
        add_dataset_ingestion(db, dataset_id, transformation_id, ingestion_id)
        add_transformation_output(db, dataset_id, transformation_id)
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

        println("Dataset $filename ingested.")

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
    add_ingestion(db::SQLite.DB, source_id::Int64, date::Date, description::String)::Int64

Insert a data ingestion into the data_ingestions table and return the data_ingestion_id
"""

function add_ingestion(db::SQLite.DB, source_id::Int64, date::Date, description::String)::Int64
    sql = """
    INSERT INTO data_ingestions (source_id, date_received, description)
    VALUES (@source_id, @date, @description)
    RETURNING *;
    """
    stmt = DBInterface.prepare(db, sql)
    ingest = DBInterface.execute(stmt, (source_id=source_id, 
                                        date=Dates.format(date, "yyyy-mm-dd"), 
                                        description=description)) |> DataFrame
    if nrow(ingest) > 0
        return ingest[1, :data_ingestion_id]
    else
        error("Unable to insert ingestion")
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
    add_dataset_ingestion(db::SQLite.DB, dataset_id, transformation_id, ingestion_id)

Record a dataset ingestion into ingest_datasets
"""
function add_dataset_ingestion(db::SQLite.DB, dataset_id, transformation_id::Int64, ingestion_id::Int64)
    sql = """
    INSERT INTO ingest_datasets (data_ingestion_id, transformation_id, dataset_id)
    VALUES (@data_ingestion_id, @transformation_id, @dataset_id);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (data_ingestion_id=ingestion_id, transformation_id=transformation_id, dataset_id=dataset_id))
    return nothing
end


"""
    add_transformation_output(db, dataset_id, transformation_id)

Add a transformation output dataset
"""
function add_transformation_output(db::SQLite.DB, dataset_id, transformation_id)
    sql = """
    INSERT INTO transformation_outputs (transformation_id, dataset_id)
    VALUES (@transformation_id, @dataset_id);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (transformation_id=transformation_id, dataset_id=dataset_id))
    return nothing
end

    

"""
    link_deathrows(db::SQLite.DB, transformation, ingest, dataset_id, death_identifier, identifier_domain)

Insert records into `deathrows` table to link dataset `dataset_id` to `deaths` table. Limited to a specific ingest.
`death_identifier` is the variable in the dataset that corresponds to the `external_id` of the death.
"""
function link_deathrows(db::SQLite.DB, ingestion_id, dataset_id, death_identifier)
    if !dataset_in_ingest(db, dataset_id, ingestion_id)
        error("Dataset $dataset_id not part of data ingest $ingestion_id")
    end
    sql = """
    INSERT OR IGNORE INTO death_rows (death_id, row_id)
    SELECT
        d.death_id,
        data.row_id
    FROM deaths d
        JOIN data ON d.external_id = data.value
        JOIN datarows r ON data.row_id = r.row_id
    WHERE d.data_ingestion_id = @ingestion_id
       AND data.variable_id = @death_identifier
       AND r.dataset_id = @dataset_id
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (ingestion_id=ingestion_id, death_identifier=death_identifier, dataset_id=dataset_id))
    return nothing
end

function dataset_in_ingest(db, dataset_id, ingestion_id)
    sql = """
        SELECT COUNT(*) n FROM ingest_datasets
        WHERE data_ingestion_id = @ingestion_id
          AND dataset_id = @dataset_id;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (ingestion_id=ingestion_id, dataset_id=dataset_id)) |> DataFrame
    return nrow(df) > 0 && df[1, :n] > 0
end



"""
 Miscellaneous Functions
"""

"""
read_data(path, name, extension, delim, quotechar, dateformat, decimal)

Returns a DataFrame with the data, from a data collection
path = path to the data collection
name = name of the data file, .csv extension assumed
"""
function read_data(path, name; extension="csv", delim, quotechar, dateformat, decimal)::AbstractDataFrame
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
    Arrow.write(joinpath(outputdir, "$(get_datasetname(db,dataset)).arrow"), df, compress=:zstd)
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
    CSV.write(joinpath(outputdir, "$(get_datasetname(db,dataset)).gz"), df, compress=true)
end

"""
    get_datasetname(db, dataset)

Return dataset name, given the `dataset_id`
"""
function get_datasetname(db, dataset)
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
    lines(str)

Returns an array of lines in `str` 
"""
lines(str) = split(str, '\n')


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