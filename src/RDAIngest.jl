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

export 
    Vocabulary, VocabularyItem,
    AbstractSource, CHAMPSSource, COMSASource, AbstractDictionary, Ingest,
    ingest_source, ingest_dictionary, ingest_deaths, ingest_data, ingest_voc_CHAMPSMITS,

    add_source, get_source, get_namedkey, get_variable, 
    add_domain, get_domain, 
    add_sites, read_sitedata, add_protocols, add_instruments, add_ethics, 
    add_variables, add_vocabulary, read_variables, get_vocabulary,
    import_datasets, link_instruments, link_deathrows, death_in_ingest, dataset_in_ingest, 
    add_ingestion, add_transformation, add_dataset_ingestion, add_transformation_output,
    
    add_data_column, lookup_variables, 
    read_data, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv, get_datasetname,
    savedataframe,

    add_datasets,add_datarows,

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
        country_iso2::String = "" #check - probably don't need this

        delim::Char = ','
        quotechar::Char = '"'
        dateformat::String = "yyyy-mm-dd"
        decimal::Char = '.'
    
        # Protocol - assume file extension pdf
        protocolfolder::String = "Protocols"
        protocols::Dict{String, String} = Dict("CHAMPS Mortality Surveillance Protocol" => "CHAMPS-Mortality-Surveillance-Protocol-v1.3.pdf",
                                     "CHAMPS Social Behavioral Science Protocol" => "CHAMPS-Social-Behavioral-Science-Protocol-v1.0.pdf",
                                     "Determination of DeCoDe Diagnosis Standards" => "CHAMPS-Diagnosis-Standards.pdf",
                                     "CHAMPS Manual version 1.0" => "CHAMPS-Manual-v3.pdf",
                                     "CHAMPS Online De-Identified Data Transfer Agreement" => "CHAMPS Online De-Identified DTA.pdf")
    
        # Instrument - specify file extension in name
        instrumentfolder::String = "Instruments"
        instruments::Dict{String,String} = Dict("CHAMPS Verbal Autopsy Questionnaire" => "cdc_93759_DS9.pdf")
        
        # Ethics - assume file extension pdf
        # Document dictionaries need to match with comittee and reference
        ethicsfolder::String = "Ethics"
        ethics::Dict{String,Vector{String}} = Dict("Emory"=>["ref1","IRB1.pdf"],"Emory"=>["ref2","IRB2.pdf"],
                                                "Country" => ["ref3","IRB3.pdf"])
end
   
Base.@kwdef struct COMSASource <: AbstractSource
        name::String = "COMSA"
        datafolder::String = "De_identified_data"
        
        site_data::String = "Comsa_WHO_VA_20230308"
        site_col::String = "provincia"
        country_col::String = ""
        country_iso2::String = "MW"
        delim::Char = ','
        quotechar::Char = '"'
        dateformat::String = "dd-u-yyyy" #"mmm dd, yyyy"
        decimal::Char = '.'
        
        # Protocol - assume file extension pdf
        protocolfolder::String = "Protocols"
        protocols::Dict{String,String} = Dict("Countrywide Mortality Surveillance for Action (COMSA) Mozambique (Formative Research)" => 
                                              "COMSA-FR-protocol_version-1.0_05July2017.pdf",
                                              "Countrywide Mortality Surveillance for Action (COMSA) Mozambique" => 
                                              "COMSA-protocol_without-FR_version-1.1_15June2017_clean_REVISED.pdf",
                                              "COMSA Data Access Plan"=> "COMSA-Data-Access-Plan.pdf",
                                              "COMSA Data Use Agreement"=> "Data Use Agreement (DUA) - Comsa.pdf")
        
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
        ethics::Dict{String,Vector{String}} = Dict("National Health Bioethics Committee of Mozambique"=>["REF 608/CNBS/17","IRB1.pdf"],
                                                   "Johns Hopkins Bloomberg School of Public Health"=>["IRB#7867","IRB2.pdf"])

end

# Ok to use a general struct if assume intermediate dictionaries Format_xx.csv given
Base.@kwdef struct AbstractDictionary
    domain_name::String = ""
    domain_description::String = domain_name
    
    dictionaries::Vector{String}
    delim::Char = ';'
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'

    id_col::String = ""
    site_col::String = ""
end

Base.@kwdef struct Ingest
    source_name::String 
    datafolder::String = "De_identified_data"    
    
    # Deaths data
    death_file::String = ""
    death_idcol::String = ""
    site_col::String = ""

    # Other datasets matching to deaths
    datasets::Dict{String,String} # Dictionary description => filename
    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'

    # Matching instruments for instrument filename => datasets filename
    datainstruments::Dict{String,String} 

    # Metadata for ingestion and transformation
    ingest_desc::String = "Ingest raw de-identified data"
    #transform_type::Int64 = 1
    #transform_status::Int64 = 1
    transform_desc::String = "Ingest raw de-identified data"
    code_reference::String = "RDAIngest"
    author::String = ""
    #date::Date = today()    
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
                        datapath::String)
    db = opendatabase(dbpath, dbname)
    try
        SQLite.transaction(db)

        source_id = add_source(source,db)
        
        # Add sites and country iso2 codes
        add_sites(source, db, source_id, datapath)
        
        # Add instruments
        add_instruments(source, db, datapath)
        
        # Add Protocols
        add_protocols(source, db, datapath)

        # Add Ethics
        add_ethics(source, db, datapath)

        SQLite.commit(db)

        return nothing
    finally
        close(db)
    end
end


"""
Step 2: 
Ingest data dictionaries, add variables and vocabularies
ingest_dictionary(dict::AbstractDictionary, dbpath::String, dbname::String, dictionarypath::String)

"""

function ingest_dictionary(dict::AbstractDictionary, dbpath::String, dbname::String, dictionarypath::String)
    db = opendatabase(dbpath, dbname)

    try
        SQLite.transaction(db)

        domain = add_domain(db, dict.domain_name, dict.domain_name)

        # Add variables
        for filename in dict.dictionaries
            variables = read_variables(dict,dictionarypath,filename)
            add_variables(variables, db, domain)
            println("Variables from $filename ingested.")
        end

        ##= 
        # Mark key fields for easier reference later
        row = lookup_variables(db,dict.id_col,domain)
        DBInterface.execute(db, "UPDATE variables SET key = 'id' WHERE domain_id = $domain AND variable_id = $(row.variable_id[1])")

        row = lookup_variables(db,dict.site_col,domain)
        DBInterface.execute(db, "UPDATE variables SET key = 'site_name' WHERE domain_id = $domain AND variable_id = $(row.variable_id[1])")
        
        ##=#

        SQLite.commit(db)

        return nothing
    finally
        close(db)
    end
end


"""
Step 3: 
Ingest deaths to deathrows, return transformation_id and ingestion_id
ingest_deaths(ingest::Ingest, db::SQLite.DB, datapath::String)

"""

function ingest_deaths(ingest::Ingest, dbpath::String, dbname::String, datapath::String)
    db = opendatabase(dbpath, dbname)
    
    try
        SQLite.transaction(db)

        source_id = get_source(db, ingest.source_name)

        # Add ingestion and transformation info
        ingestion_id = add_ingestion(db, source_id, today(), ingest.ingest_desc)
        transformation_id = add_transformation(db, 1, 1, ingest.transform_desc, #type=1, status=1
                                                ingest.code_reference, today(), ingest.author)

        # Ingest deaths
        deaths = read_data(DocCSV(joinpath(datapath,ingest.source_name,ingest.datafolder),
                            ingest.death_file,
                            ingest.delim, ingest.quotechar, ingest.dateformat, ingest.decimal))

        sites = DBInterface.execute(db, "SELECT * FROM sites WHERE source_id = $source_id") |> DataFrame
        sitedeaths = innerjoin(transform!(deaths, Symbol(ingest.site_col) => :site_name), 
                            sites, on=:site_name, matchmissing=:notequal)
                            
        savedataframe(db, select(sitedeaths, :site_id, Symbol(ingest.death_idcol) => :external_id, 
                                [] => Returns(ingestion_id) => :data_ingestion_id, copycols=false), 
                    "deaths")

        println("Death data $(ingest.death_file) ingested.")

        SQLite.commit(db)

        return Dict("ingestion_id" => ingestion_id ,
                    "transformation_id" => transformation_id)

    finally
        close(db)
    end
end


"""
Step 4: 
Import datasets, and link datasets to deaths

ingest_data(ingest::Ingest, dbpath::String, dbname::String, datapath::String,
                        transformation_id::Int64, ingestion_id::Int64)
# transformation_id and ingestion_id can be from step 3 outputs if ingesting both death and datasets at the same time.
# If only importing dataset without ingesting deaths, run add_ingestion() and add_transformation() to get new ids.
"""

function ingest_data(ingest::Ingest, dbpath::String, dbname::String, datapath::String,
                        transformation_id::Int64, ingestion_id::Int64, death_ingestion_id=nothing)
    db = opendatabase(dbpath, dbname)
    try
        source = get_source(db, ingest.source_name)
        domain = get_domain(db, ingest.source_name)
        death_idvar = get_variable(db, domain, ingest.death_idcol)

        for (key, value) in ingest.datasets

            dataset_name = "$value"
            dataset_desc = "$key"
            # Import datasets
            dataset_id = import_datasets(db, datapath, 
                    ingest, dataset_name, dataset_desc,
                    domain, transformation_id, ingestion_id)

            # Link to deathrows
            if !dataset_in_ingest(db, dataset_id, ingestion_id) #probably don't need this??
                error("Dataset $dataset_id not part of data ingest $ingestion_id")
            end

            if !death_in_ingest(db,ingestion_id)
                println("Death data is not part of currrent data ingest $ingestion_id")
                if death_ingestion_id===nothing
                    death_ingestion_id = get_last_deathingest(db,source)
                    println("Death ingestion id not specified. By default, use lastest ingested deaths from source $(ingest.source_name) from ingestion id $death_ingestion_id.")
                else
                    error("Death from source $(ingest.source_name) hasn't been ingested.")
                end
            else
                death_ingestion_id = ingestion_id
            end
            link_deathrows(db, death_ingestion_id, dataset_id, death_idvar)

            println("Dataset $dataset_name imported and linked to deathrows.")

        end

        # Link to instruments in instrument_datasets
        if !isempty(ingest.datainstruments) 
            for (key1, value1) in ingest.datainstruments #instrument name, dataset name
                link_instruments(db, "$key1","$value1") 
            end
        end    

    finally
    close(db)
    end
end


"""
Detailed functions
"""

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
    add_domain(db::SQLite.DB, domain_name::String, domain_description::String)

Add domain to the domain table if not exist, and returns the domain id
"""
function add_domain(db::SQLite.DB, domain_name::String, domain_description::String="")
    domain = get_domain(db, domain_name)
        
    if ismissing(domain)  
        # Insert domain
        sql = raw"""
        INSERT INTO domains (name, description) VALUES (@name, @description)
        """
        stmt = DBInterface.prepare(db, sql)
        domain = DBInterface.lastrowid(DBInterface.execute(stmt, 
                        (name = domain_name, description = domain_description)))
        
        println("Domain $domain_name added.")
    end

    return domain
end

"""
    get_domain(db::SQLite.DB, domainname)

Return the domain_id for domain named `domain_name`
"""
function get_domain(db::SQLite.DB,domain_name::String)
    return get_namedkey(db, "domains", domain_name, Symbol("domain_id"))
end

"""
    add_sites(source::AbstractSource, db::SQLite.DB, sourceid::Int64, datapath::String)

Add sites and country iso2 codes to sites table
"""

function add_sites(source::CHAMPSSource, db::SQLite.DB, sourceid::Int64, datapath::String)
    sites = read_sitedata(source,datapath,sourceid)

    select!(sites, 
            Symbol(source.site_col) => ByRow(x -> x) => :site_name, 
            Symbol(source.country_col) => ByRow(x -> x) => :country_iso2, 
            :source_id)
    savedataframe(db, sites, "sites")
    println("Site names and country iso2 codes ingested.")
    return nothing
end
function add_sites(source::COMSASource, db::SQLite.DB, sourceid::Int64, datapath::String)
    sites = read_sitedata(source,datapath,sourceid)

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

function read_sitedata(source::AbstractSource,datapath,sourceid)
    df = read_data(DocCSV(joinpath(datapath,source.name,source.datafolder),
                          source.site_data,
                          source.delim, source.quotechar, source.dateformat, source.decimal))
    sites = combine(groupby(df, source.site_col), nrow => :n)
    insertcols!(sites, 1, :source_id => sourceid) 
    return(sites)
end


"""
    add_protocols(source::AbstractSource, db::SQLite.DB, datapath::String)

Add protocols
Todo: how protocols link to enthics_id, need a mapping dictionary? 
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
        file = read_data(DocPDF(joinpath(datapath,source.name,source.protocolfolder),"$value"))
        DBInterface.execute(stmt_doc, 
                                (protocol_id=row.protocol_id, name="$value.pdf", document=file))
        println("Protocol document $value.pdf ingested.")
        
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
        file = read_data(DocPDF(joinpath(datapath,source.name,source.instrumentfolder),"$value"))
        DBInterface.execute(stmt_doc, 
                                (instrument_id=row.instrument_id, name="$value", document=file))
            println("Instrument document $value ingested.")
    end
    return nothing
end


"""
    add_ethics(source::AbstractSource, db::SQLite.DB, datapath::String)

Ethics document, committee and reference need to be in matching order
"""

function add_ethics(source::AbstractSource, db::SQLite.DB, datapath::String)

    # Insert ethics names
    sql = raw"""
    INSERT INTO ethics (name, ethics_committee, ethics_reference) VALUES (@name, @ethics_committee, @ethics_reference)
    """
    stmt_name = DBInterface.prepare(db, sql)

    for (key, value) in source.ethics
        DBInterface.execute(stmt_name, (name = "$(value[2])", 
                                        ethics_committee = "$key",
                                        ethics_reference = "$(value[1])"))
    end

    # Insert ethics documents
    sql = raw"""
    INSERT INTO ethics_documents (ethics_id, name, description, document) VALUES (@ethics_id, @name, @description, @document)
    """
    stmt_doc = DBInterface.prepare(db, sql)
    
    for (key, value) in source.ethics
        file = read_data(DocPDF(joinpath(datapath,source.name,source.ethicsfolder),"$(value[2])"))    
        
        # Get ethics id
        row = DBInterface.execute(db, "SELECT * FROM ethics WHERE name = '$(value[2])'") |> DataFrame
        
        DBInterface.execute(stmt_doc, 
                            (ethics_id=row.ethics_id, name="$(value[2])", description = "$key ($(value[1]))", document=file))
        println("Ethics document $(value[2]) ingested.")
    end
    return nothing
end


"""
add_variables(variables::AbstractDataFrame, db::SQLite.DB, domain_id::Int64)

Add variables from a variable dataframe to variables table
"""

function add_variables(variables::AbstractDataFrame, db::SQLite.DB, domain_id::Int64)
    
    # Check if variables dataframe has all required columns
    required_columns = ["Column_Name", "DataType","Description","Note","Vocabulary"]
    missing_columns = filter(col -> !(col in names(variables)), required_columns)
    if !isempty(missing_columns)
        error("Variables dataframe missing columns: ", join(missing_columns, ", "))
    end

        # Variable insert SQL
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
        if !("domain_id" in names(variables))
            insertcols!(variables, 1, :domain_id => domain_id)
        end

        for row in eachrow(variables)
            id = missing
            if !ismissing(row.Vocabulary)
                id = add_vocabulary(db, row.Vocabulary)
            end
            DBInterface.execute(stmt, (domain_id=row.domain_id, name=row.Column_Name, 
                                        value_type_id=row.DataType, vocabulary_id=id, 
                                        description=row.Description, note=row.Note))
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

    ingest_voc_CHAMPSMITS(datapath::String, source::String, datafolder::String, tac_vocabulary::String, domain_id::Int64)

This function ingets vocabulary for CHAMPS tac result 

tac_vocabulary: CHAMPS_deid_tac_vocabulary.xlsx created from CHAMPS data description, 
first sheet include pathogen and multi-gene result code, rest include assay pattern and corresponding pathogen result label.
"""

function ingest_voc_CHAMPSMITS(dbpath::String, dbname::String, datapath::String, source::String, datafolder::String, tac_vocabulary::String)

    db = opendatabase(dbpath, dbname)    
    domain_id = get_domain(db,source)

    # Read MITS vocabulary xlsx 
    file = joinpath(datapath,source,datafolder,tac_vocabulary)
    xf = XLSX.readxlsx(file)

    pathogencode = XLSX.readtable(file, XLSX.sheetnames(xf)[1])|> DataFrame

    # Get vocabularies
    sql = "SELECT vocabulary_id FROM vocabularies"
    last_row_id = DataFrame(DBInterface.execute(db,sql)).vocabulary_id[end]
    
    tac_voc = select(pathogencode,
                        [] => Returns((1:size(pathogencode,1)) .+ last_row_id) => :vocabulary_id,
                        :Pathogen => :name,
                        Symbol("Multi-target result code") => :description)

    #SQLite.transaction(db)

    # Add vocabulary ids to variables table    
    for row in eachrow(tac_voc)
        pathogen = string("_","$(row.name)")
        sql = """
                UPDATE variables
                SET vocabulary_id = IFNULL(vocabulary_id, $(row.vocabulary_id))
                WHERE name LIKE '%$(pathogen)%' AND domain_id = $domain_id;
                """
        DBInterface.execute(db, sql)
    end

    # Add vocabularies to vocabularies item
    sql = """
        INSERT INTO vocabularies (name, description)
        VALUES ( @name, @desc)
        """
        stmt = DBInterface.prepare(db, sql)
        for row in eachrow(tac_voc)
            DBInterface.execute(stmt, (
                                        name=row.name, desc=row.description))
        end

    #SQLite.commit(db)
    
    # Get vocabulary item, description reflects combined assay result
    tac_voc_item = DataFrame(vocabulary_id=Int64[], value = Int64[],
                            code=String[],description=String[])

    for row1 in eachrow(pathogencode)
        voc_id = tac_voc.vocabulary_id[tac_voc.name .==row1.Pathogen]
        ptg_xf = XLSX.readtable(file, row1.Pathogen)|> DataFrame
        value=0
        for row2 in eachrow(ptg_xf)
            desc = replace(join([join([names(row2)[i],row2[i]],":") for i in 1:(length(row2)-1)],";"), " " => "")
            code = row2.Interpretation
            
            # If code is new, add new row, otherwise update existing description
            if in(code, tac_voc_item.code[tac_voc_item.vocabulary_id .==voc_id])
                desc_old = tac_voc_item.description[(tac_voc_item.vocabulary_id .==voc_id) .&& (tac_voc_item.code .==code)][1]
                tac_voc_item.description[(tac_voc_item.vocabulary_id .==voc_id) .&& (tac_voc_item.code .==code)] .= join([desc_old,desc],"|")
            else
                value= value + 1
                tac_voc_item = vcat(tac_voc_item, DataFrame(vocabulary_id=voc_id, value=value, code=code, description=desc))
            end
        end
    end

    # Add vocabulary items to vocabulary_items table
    sql = """
            INSERT INTO vocabulary_items (vocabulary_id, value, code, description)
            VALUES (@vocabulary_id, @value, @code, @desc)
            """
            stmt = DBInterface.prepare(db, sql)
            for row in eachrow(tac_voc_item)
                DBInterface.execute(stmt, (vocabulary_id=row.vocabulary_id, 
                                            value=row.value, code = row.code, desc=row.description))
            end
            
    return nothing
end

"""
read_variables(dict::AbstractDictionary, dictionarypath::String, dictionaryname::String)

Read a csv file listing variables, variable descriptions and data types in a dataset.

"""

function read_variables(dict::AbstractDictionary, dictionarypath::String, dictionaryname::String)

    df = read_data(DocCSV(joinpath(dictionarypath, "$(dict.domain_name)"),dictionaryname,
                                dict.delim,dict.quotechar,dict.dateformat,dict.decimal))
    
    vocabularies = Vector{Union{Vocabulary,Missing}}()
    for row in eachrow(df)
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
import_datasets(db::SQLite.DB, datapath::String,
    ingest::Ingest, #delim::Char, quotechar::Char, dateformat::String, decimal::Char,
    filename::String, description::String,
    domain_id::Int64,transformation_id::Int64, ingestion_id::Int64)::Int64

Insert datasets into SQLite db and returns the datatset_id

"""

function import_datasets(db::SQLite.DB, datapath::String,
    ingest::Ingest, filename::String, description::String,
    domain_id::Int64,transformation_id::Int64, ingestion_id::Int64)::Int64
    try
        SQLite.transaction(db)

        data = read_data(DocCSV(joinpath(datapath,ingest.source_name,ingest.datafolder),filename,
                                ingest.delim,ingest.quotechar,ingest.dateformat,ingest.decimal))

        variables = lookup_variables(db, names(data), domain_id)
        var_lookup = Dict{String,Int64}(zip(variables.name, variables.variable_id))
        
        # Add dataset entry to datasets table
        dataset_id = add_datasets(db, filename, description)
        
        add_dataset_ingestion(db, dataset_id, transformation_id, ingestion_id)
        add_transformation_output(db, dataset_id, transformation_id)

        savedataframe(db, select(variables, [] => Returns(dataset_id) => :dataset_id, :variable_id), "dataset_variables")

        # Store datarows in datarows table and get row_ids 
        datarows = add_datarows(db::SQLite.DB, nrow(data), dataset_id)

        #prepare data for storage
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
    link_instruments(db::SQLite.DB, instrument_name, dataset_name)

Insert records into `instrument_datasets` table, linking datasets with instruments.
"""
function link_instruments(db::SQLite.DB, instrument_name::String, dataset_name::String)
    
    # get id for dataset and matching instrument
    dataset_id = get_namedkey(db, "datasets", dataset_name, :dataset_id)
    instrument_id = get_namedkey(db, "instruments", instrument_name, :instrument_id)

    if ismissing(dataset_id)
        error("Data file $dataset_name is not ingested.")
    end
    if ismissing(instrument_id)
        error("Instrument file $instrument_name is not ingested.")
    end

    # Insert into db
    sql = """
    INSERT INTO instrument_datasets(instrument_id, dataset_id) 
    VALUES (@instrument_id, @dataset_id);
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, (instrument_id=instrument_id, dataset_id=dataset_id))

    return println("Linked dataset $dataset_name to instrument $instrument_name")
end


"""
    link_deathrows(db::SQLite.DB, ingestion_id, dataset_id, death_identifier)

Insert records into `deathrows` table to link dataset `dataset_id` to `deaths` table. Limited to a specific ingest.
`death_identifier` is the variable in the dataset that corresponds to the `external_id` of the death.
"""
function link_deathrows(db::SQLite.DB, ingestion_id, dataset_id, death_identifier)

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

"""
death_in_ingest(db, ingestion_id)

If ingested deaths are part of ingestion_id
"""

function death_in_ingest(db, ingestion_id)
    sql = """
        SELECT COUNT(*) n FROM deaths
        WHERE data_ingestion_id = @ingestion_id;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, (ingestion_id=ingestion_id)) |> DataFrame
    return nrow(df) > 0 && df[1, :n] > 0
end


"""
dataset_in_ingest(db, dataset_id, ingestion_id)

If dataset_id is part of ingestion_id
"""

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
read_data(DocName)

Read file names and formatting parameters, returns a DataFrame with the data
"""

# Define an abstract document input
abstract type DocName end
# Define subtypes of Document - csv, xlsx, pdf
struct DocCSV
    path::String
    name::String
    delim::Char
    quotechar::Char
    dateformat::String
    decimal::Char
end
struct DocXLSX
    path::String
    name::String
    sheetname::String
    cellrange::String
end
struct DocPDF
    path::String
    name::String
end

# Define functions that calculate area for different shapes
function read_data(DocName::DocPDF)
    file = joinpath(DocName.path, "$(DocName.name)") #.pdf
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = read(file)
        return df
    end
end
function read_data(DocName::DocCSV)
    file = joinpath(DocName.path, "$(DocName.name).csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=DocName.delim, quotechar=DocName.quotechar, 
                        dateformat=DocName.dateformat, decimal=DocName.decimal) |> DataFrame
        return df
    end
end
function read_data(DocName::DocXLSX) 
    file = joinpath(DocName.path, "$(DocName.name)") #xlsx
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = DataFrame(XLSX.readdata(file,DocName.sheetname,DocName.cellrange),:auto)
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
    dataset_to_csv(db, dataset_id, datapath, compress)

Save a dataset in compressed csv format
"""
function dataset_to_csv(db, dataset_id, datapath, compress=false)
    outputdir = joinpath(datapath, "csvfiles")
    if !isdir(outputdir)
        mkpath(outputdir)
    end
    df = dataset_to_dataframe(db, dataset_id)
    if (compress)
        CSV.write(joinpath(outputdir, "$(get_datasetname(db,dataset_id)).gz"), df, compress=true) #have trouble opening on MacOS
    else
        CSV.write(joinpath(outputdir, "$(get_datasetname(db,dataset_id)).csv"), df)
    end
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

"""
    get_last_deathingest(source)

Get ingestion id for latest death ingestion for source
"""
function get_last_deathingest(db::SQLite.DB, source_name::String)
    
    source=get_source(db,source_name)
    
    source_ingests = DBInterface.execute(db, "SELECT data_ingestion_id FROM data_ingestions WHERE source_id = $source") |>DataFrame
                    
    sql = """
    SELECT data_ingestion_id, COUNT(*) As n 
    FROM deaths 
    WHERE data_ingestion_id IN ($(join(source_ingests.data_ingestion_id, ",")))
    GROUP BY data_ingestion_id
    """
    death_ingests = DBInterface.execute(db, sql) |>DataFrame 
    if nrow(death_ingests) > 0 && any(x -> x > 0, death_ingests.n)
        death_ingestion_id = death_ingests.data_ingestion_id[death_ingests.n .>0][end]
        return death_ingestion_id
    else
        error("Death from source $source_name hasn't been ingested.")
    end
end

"""
    add_datasets(db, new_dataset_name, new_dataset_description)

Add dataset entry in the datasets table
"""
function add_datasets(db::SQLite.DB, dataset_name::String, dataset_description::String)
    sql = """
    INSERT INTO datasets(name, date_created, description) 
    VALUES (@name, @date_created, @description);
    """
    stmt = DBInterface.prepare(db, sql)
    dataset_id = DBInterface.lastrowid(DBInterface.execute(stmt, 
                                            (name=dataset_name, 
                                            date_created=Dates.format(today(), "yyyy-mm-dd"), 
                                            description=dataset_description)))
    println("Entry for dataset $dataset_name added in the datasets table") 
    return dataset_id
end

"""
    add_datarows(db, nrow)

Define data rows in the datarows table
"""
function add_datarows(db::SQLite.DB, nrow::Int64, dataset_id::Int64)
    stmt = DBInterface.prepare(db, "INSERT INTO datarows (dataset_id) VALUES(@dataset_id);")
    for i = 1:nrow
        DBInterface.execute(stmt, (dataset_id = dataset_id))
    end

    datarows = DBInterface.execute(db, "SELECT row_id FROM datarows WHERE dataset_id = $dataset_id;") |> DataFrame

    return datarows
end

include("rdadatabase.jl")

end