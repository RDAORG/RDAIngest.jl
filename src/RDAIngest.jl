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
using ODBC
using XLSX

export
    Vocabulary, VocabularyItem,
    AbstractSource, CHAMPSSource, COMSASource, Ingest,
    ingest_source, ingest_dictionary, ingest_deaths, ingest_data,
    ingest_voc_CHAMPSMITS, add_source, get_source, get_namedkey, get_variable,
    add_domain, get_domain,
    add_sites, read_sitedata, add_protocols, add_instruments, add_ethics,
    add_variables, add_vocabulary, read_variables, get_vocabulary,
    import_datasets, link_instruments, link_deathrows, death_in_ingest, dataset_in_ingest,
    add_ingestion, add_transformation, add_dataset_ingestion, add_transformation_output,
    add_data_column, lookup_variables, add_datasets, add_datarows,
    get_last_deathingest, read_data, dataset_to_dataframe, dataset_to_arrow, dataset_to_csv, get_datasetname,
    savedataframe, createdatabase, opendatabase #, 
#get_table, createsources, createprotocols, createtransformations,
#createvariables, createdatasets, createinstruments, createdeaths

#ODBC.bindtypes(x::Vector{UInt8}) = ODBC.API.SQL_C_BINARY, ODBC.API.SQL_LONGVARBINARY

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

# Define an abstract document input
abstract type DataDocument end
# Define subtypes of Document - csv, xlsx, pdf
struct DocCSV <: DataDocument
    path::String
    name::String
    delim::Char
    quotechar::Char
    dateformat::String
    decimal::Char
end
struct DocXLSX <: DataDocument
    path::String
    name::String
    sheetname::String
    cellrange::String
end
struct DocPDF <: DataDocument
    path::String
    name::String
end

"""
Provide a source struct for source specific information
"""
abstract type AbstractSource end
"""
Provide CHAMPS specific information
"""
Base.@kwdef struct CHAMPSSource <: AbstractSource
    name::String = "CHAMPS"
    datafolder::String = "De_identified_data"

    site_data::String = "CHAMPS_deid_basic_demographics"
    site_col::String = "site_iso_code"
    country_col::String = "site_iso_code"
    country_iso2::String = "" #check - probably don't need this
    id_col::String = "champs_deid" #column that uniquely identifies a death

    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "yyyy-mm-dd"
    decimal::Char = '.'

    # Protocol - assume file extension pdf
    protocolfolder::String = "Protocols"
    protocols::Dict{String,String} = Dict("CHAMPS Mortality Surveillance Protocol" => "CHAMPS-Mortality-Surveillance-Protocol-v1.3.pdf",
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
    ethics::Dict{String,Vector{String}} = Dict("Emory" => ["ref1", "IRB1.pdf"], "Emory" => ["ref2", "IRB2.pdf"],
        "Country" => ["ref3", "IRB3.pdf"])
    # Data dictionaries
    domain_name::String = "CHAMPS"
    domain_description::String = "Raw CHAMPS level-2 deidentified data"
    datadictionaries::Vector{String} = ["Format_CHAMPS_deid_basic_demographics",
        "Format_CHAMPS_deid_verbal_autopsy",
        "Format_CHAMPS_deid_decode_results",
        "Format_CHAMPS_deid_tac_results",
        "Format_CHAMPS_deid_lab_results"]
    tac_vocabulary::String = "CHAMPS_deid_tac_vocabulary.xlsx"
end
"""
Provide CHAMPS specific information
"""
Base.@kwdef struct COMSASource <: AbstractSource
    name::String = "COMSA"
    datafolder::String = "De_identified_data"

    site_data::String = "Comsa_WHO_VA_20230308"
    site_col::String = "provincia"
    country_col::String = ""
    country_iso2::String = "MW"
    id_col::String = "comsa_id" #column that uniquely identifies a death

    delim::Char = ','
    quotechar::Char = '"'
    dateformat::String = "u dd, yyyy" #not "dd-u-yyyy" 
    decimal::Char = '.'

    # Protocol - assume file extension pdf
    protocolfolder::String = "Protocols"
    protocols::Dict{String,String} = Dict("Countrywide Mortality Surveillance for Action (COMSA) Mozambique (Formative Research)" => "COMSA-FR-protocol_version-1.0_05July2017.pdf",
        "Countrywide Mortality Surveillance for Action (COMSA) Mozambique" => "COMSA-protocol_without-FR_version-1.1_15June2017_clean_REVISED.pdf",
        "COMSA Data Access Plan" => "COMSA-Data-Access-Plan.pdf",
        "COMSA Data Use Agreement" => "Data Use Agreement (DUA) - Comsa.pdf")

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
    ethics::Dict{String,Vector{String}} = Dict("National Health Bioethics Committee of Mozambique" => ["REF 608/CNBS/17", "IRB1.pdf"],
        "Johns Hopkins Bloomberg School of Public Health" => ["IRB#7867", "IRB2.pdf"])

    # Data dictionaries
    domain_name::String = "COMSA"
    domain_description::String = "COMSA verbal autopsy dictionary"
    datadictionaries::Vector{String} = ["Format_Comsa_WHO_VA_20230308"]
    tac_vocabulary::String = ""
end
"""
Provide ingest specific information
"""
Base.@kwdef struct Ingest
    source::AbstractSource

    # Deaths data
    death_file::String = ""

    # Other datasets matching to deaths
    datasets::Dict{String,String} # Dataset name => description

    # Matching instruments to data sets Dataset name => instrument filename
    datainstruments::Dict{String,String}

    # Metadata for ingestion and transformation
    ingest_desc::String = "Ingest raw de-identified data"

    transform_desc::String = "Ingest raw de-identified data"
    code_reference::String = "RDAIngest"
    author::String = ""
end

"""
    ingest_source(source::AbstractSource, dbpath::String, dbname::String,
    datapath::String; sqlite=true)


Step 1: 
Ingest macro data of sources: sites, instruments, protocols, ethics, vocabularies 

datapath: root folder with data from all sources [DATA_INGEST_PATH]
"""
function ingest_source(source::AbstractSource, dbpath::String, dbname::String,
    datapath::String; sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)
    try
        DBInterface.transaction(db) do

            source_id = add_source(source, db)

            # Add sites and country iso2 codes
            add_sites(source, db, source_id, datapath)

            # Add instruments
            add_instruments(source, db, datapath)

            # Add Protocols
            add_protocols(source, db, datapath)

            # Add Ethics
            add_ethics(source, db, datapath; source_id)

        end

        return nothing
    finally
        DBInterface.close!(db)
    end
end

"""
    ingest_dictionary(source::AbstractSource, dbpath::String, dbname::String, dictionarypath::String, datapath::String; sqlite=true)

Step 2: 
Ingest data dictionaries, add variables and vocabularies
"""
function ingest_dictionary(source::AbstractSource, dbpath::String, dbname::String, dictionarypath::String, datapath::String; sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)

    try
        DBInterface.transaction(db) do
            @info "Ingest dictionaries for $(source.name). sqlite = $sqlite"
            domain = add_domain(db, source.domain_name, source.domain_description)

            # Add variables
            for filename in source.datadictionaries
                variables = read_variables(source, dictionarypath, filename)
                add_variables(variables, db, domain)
                @info "Variables from $filename ingested."
            end

            # Mark key fields for easier reference later
            row = lookup_variables(db, source.id_col, domain)
            DBInterface.execute(db, "UPDATE variables SET keyrole = 'id' WHERE domain_id = $domain AND variable_id = $(row.variable_id[1])")

            row = lookup_variables(db, source.site_col, domain)
            DBInterface.execute(db, "UPDATE variables SET keyrole = 'site_name' WHERE domain_id = $domain AND variable_id = $(row.variable_id[1])")

            #Add vocabularies for TAC results with multi-gene
            if source.tac_vocabulary != ""
                ingest_tac_vocabulary(source, db, datapath)               
            end
        end
        return nothing
    finally
        DBInterface.close!(db)
    end
end

"""
    ingest_tac_vocabulary(source::AbstractSource, db, datapath)

TBW
"""
function ingest_tac_vocabulary(source::AbstractSource, db, datapath)
    domain_id = get_domain(db, source.domain_name)
    xf = XLSX.readxlsx(joinpath(datapath, source.name, source.datafolder, source.tac_vocabulary))
    pathogens = pathogens = XLSX.gettable(xf[1]) |> DataFrame
    insertcols!(pathogens, 1, :vocabulary_id => 0) #to record saved vocabulary
    select!(pathogens, :vocabulary_id, :Pathogen => :name, Symbol("Multi-target result code") => :description)
    # Add vocabulary for each pathogen
    for row in eachrow(pathogens)
        vocab_id = insertwithidentity(db, "vocabularies", ["name", "description"], [row.name, row.description], "vocabulary_id")
        row.vocabulary_id = vocab_id
        updatevariable_vocabulary(db, "_" * row.name, domain_id, vocab_id)
    end
    # Add vocabulary items
    stmt = prepareinsertstatement(db, "vocabulary_items", ["vocabulary_id", "value", "code", "description"])
    for row in eachrow(pathogens)
        #get details for each pathogen in sheet with the pathogen name
        pathogen_details = XLSX.gettable(xf[row.name]) |> DataFrame
        #Group by Interpretation to get unique codes
        select!(pathogen_details, :Interpretation => ByRow(x -> strip(x)) => :code, AsTable(Not(:Interpretation)) =>
            ByRow(x -> replace(join([join([keys(x)[i], values(x)[i]], ":") for i in 1:length(x)], ";"), " " => "")) => :values)
        items = combine(groupby(pathogen_details, :code), groupindices => :value, :values => (x -> join(x, "|")) => :description)
        #convert values to string to keep ODBC happy
        transform!(items, :code => ByRow(x -> String(x)) => :code, :description => ByRow(x -> String(x)) => :description)
        for row1 in eachrow(items)
            DBInterface.execute(stmt, [row.vocabulary_id, row1.value, row1.code, row1.description])
        end
    end
end

"""
    ingest_deaths(ingest::Ingest, dbpath::String, dbname::String, datapath::String; sqlite=true)

Step 3: 
Ingest deaths to deathrows, return transformation_id and ingestion_id
"""
function ingest_deaths(ingest::Ingest, dbpath::String, dbname::String, datapath::String; sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)

    try
        DBInterface.transaction(db) do

            source_id = get_source(db, ingest.source.name)

            # Add ingestion and transformation info
            ingestion_id = insertwithidentity(db, "data_ingestions", ["source_id", "date_received", "description"], [source_id, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), ingest.ingest_desc], "data_ingestion_id")
            # transformation should not be created for the death ingestion

            # Ingest deaths
            deaths = read_data(DocCSV(joinpath(datapath, ingest.source.name, ingest.source.datafolder),
                ingest.death_file, ingest.source.delim, ingest.source.quotechar, ingest.source.dateformat, ingest.source.decimal))
            sites = selectdataframe(db, "sites", ["site_id", "site_name"], ["source_id"], [source_id])

            sitedeaths = innerjoin(transform!(deaths, Symbol(ingest.source.site_col) => :site_name),
                sites, on=:site_name, matchmissing=:notequal)

            savedataframe(db, select(sitedeaths, :site_id, Symbol(ingest.source.id_col) => :external_id,
                    [] => Returns(ingestion_id) => :data_ingestion_id, copycols=false), "deaths")


            return ingestion_id #, transformation_id
        end

    finally
        DBInterface.close!(db)
    end
end

"""
    ingest_data(ingest::Ingest, dbpath::String, dbname::String, datapath::String,
    transformation_id::Integer, ingestion_id::Integer, death_ingestion_id=nothing)


Step 4: 
Import datasets, and link datasets to deaths
* Ingestion_id can be from step 3 outputs if ingesting both death and datasets at the same time, if missing an ingestion will be created.
"""
function ingest_data(ingest::Ingest, dbpath::String, dbname::String, datapath::String; ingestion_id=0, sqlite=true)
    db = opendatabase(dbpath, dbname; sqlite)
    try
        source_id = get_source(db, ingest.source.name)
        domain_id = get_domain(db, ingest.source.name)
        death_idvar = get_variable(db, domain_id, ingest.source.id_col)

        DBInterface.transaction(db) do
            if ingestion_id == 0
                ingestion_id = insertwithidentity(db, "data_ingestions", ["source_id", "date_received", "description"], [source_id, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), ingest.ingest_desc], "data_ingestion_id")
            end
            transformation_id = insertwithidentity(db, "transformations", ["transformation_type_id", "transformation_status_id", "description", "code_reference", "date_created", "created_by"],
                [1, 1, ingest.transform_desc, ingest.code_reference, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), ingest.author], "transformation_id")

            for (dataset_desc, dataset_name) in ingest.datasets

                # Import datasets
                ds = read_data(DocCSV(joinpath(datapath, ingest.source.name, ingest.source.datafolder), dataset_name,
                    ingest.source.delim, ingest.source.quotechar, ingest.source.dateformat, ingest.source.decimal))

                dataset_id = save_dataset(db, ds, dataset_name, dataset_desc,
                    domain_id, transformation_id, ingestion_id)

                # Link to deathrows
                if !death_in_ingest(db, ingestion_id)
                    @info "Death data is not part of currrent data ingest $ingestion_id"
                    death_ingestion_id = get_last_deathingest(db, source_id)
                    @info "Death ingestion id not specified. By default, use lastest ingested deaths from source $(ingest.source.name) from ingestion id $death_ingestion_id."
                else
                    death_ingestion_id = ingestion_id
                end
                @info "Linking dataset $dataset_name to deathrows from ingestion id $death_ingestion_id, dataset_id = $dataset_id, death_idvar = $death_idvar"
                link_deathrows(db, death_ingestion_id, dataset_id, death_idvar)

                @info "Dataset $dataset_name imported and linked to deathrows."

            end

            # Link to instruments in instrument_datasets
            if !isempty(ingest.datainstruments)
                for (instrument_name, dataset_name) in ingest.datainstruments #instrument name, dataset name
                    link_instruments(db, instrument_name, dataset_name)
                end
            end
        end
    finally
        DBInterface.close!(db)
    end
end

"""
    add_source(source::AbstractSource, db::DBInterface.Connection)

Add source `name` to the sources table, and returns the `source_id`
"""
function add_source(source::AbstractSource, db::DBInterface.Connection)
    id = get_source(db, source.name)
    if ismissing(id)  # insert source
        id = RDAIngest.insertwithidentity(db, "sources", ["name"], [source.name], "source_id")
    end
    return id
end

"""
    get_source(db::DBInterface.Connection, name)

Return the `source_id` of source `name`, returns `missing` if source doesn't exist
"""
function get_source(db::DBInterface.Connection, name)
    return get_namedkey(db, "sources", name, :source_id)
end

"""
    get_namedkey(db::DBInterface.Connection, table, key, keycol)

 Return the integer key from table `table` in column `keycol` (`keycol` must be a `Symbol`) for key with name `key`
"""
function get_namedkey(db::DBInterface.Connection, table, key, keycol)
    stmt = prepareselectstatement(db, table, ["*"], ["name"])
    df = DBInterface.execute(stmt, [key]) |> DataFrame
    if nrow(df) == 0
        return missing
    else
        return df[1, keycol]
    end
end

"""
    get_variable(db::DBInterface.Connection, domain, name)

Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable(db::ODBC.Connection, domain, name)
    stmt = prepareselectstatement(db, "variables", ["variable_id"], ["domain_id", "name"])
    result = DBInterface.execute(stmt, [domain, name]; iterate_rows=true) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result[1, :variable_id]
    end
end
"""
    get_variable(db::SQLite.DB, domain, name)

Returns the `variable_id` of variable named `name` in domain with id `domain`
"""
function get_variable(db::SQLite.DB, domain, name)
    stmt = prepareselectstatement(db, "variables", ["variable_id"], ["domain_id", "name"])
    result = DBInterface.execute(stmt, [domain, name]) |> DataFrame
    if nrow(result) == 0
        return missing
    else
        return result[1, :variable_id]
    end
end

"""
    add_domain(db::DBInterface.Connection, domain_name::String, domain_description::String)

Add domain to the domain table if not exist, and returns the domain id
"""
function add_domain(db::DBInterface.Connection, domain_name::String, domain_description::String="")
    domain = get_domain(db, domain_name)
    if ismissing(domain)  # insert source
        domain = RDAIngest.insertwithidentity(db, "domains", ["name", "description"], [domain_name, domain_description], "domain_id")
    end
    return domain
end

"""
    get_domain(db::DBInterface.Connection, domain_name::String)

Return the domain_id for domain named `domain_name`
"""
get_domain(db::DBInterface.Connection, domain_name::String) = get_namedkey(db, "domains", domain_name, Symbol("domain_id"))

"""
    add_sites(source::CHAMPSSource, db::DBInterface.Connection, sourceid::Integer, datapath::String)

Add CHAMPS sites and country iso2 codes to sites table
"""
function add_sites(source::CHAMPSSource, db::DBInterface.Connection, sourceid::Integer, datapath::String)
    sites = read_sitedata(source, datapath, sourceid)

    select!(sites,
        Symbol(source.site_col) => ByRow(x -> x) => :site_name,
        Symbol(source.country_col) => ByRow(x -> x) => :country_iso2,
        :source_id)
    # ODBC can't deal with InlineStrings
    transform!(sites, :site_name => ByRow(x -> String(x)) => :site_name, :country_iso2 => ByRow(x -> String(x)) => :country_iso2)
    savedataframe(db, sites, "sites")
    @info "Site names and country iso2 codes ingested."
    return nothing
end
"""
    add_sites(source::COMSASource, db::DBInterface.Connection, sourceid::Integer, datapath::String)

Add COMSA sites and country iso2 codes to sites table
"""
function add_sites(source::COMSASource, db::DBInterface.Connection, sourceid::Integer, datapath::String)
    sites = read_sitedata(source, datapath, sourceid)

    if (source.country_iso2 == "" || !isdefined(source, Symbol("country_iso2")))
        error("Country iso2 code not provided in data and not specified.")
    else
        select!(sites,
            Symbol(source.site_col) => ByRow(x -> x) => :site_name,
            [] => Returns(source.country_iso2) => :country_iso2,
            :source_id)
        # ODBC can't deal with InlineStrings
        transform!(sites, :site_name => ByRow(x -> String(x)) => :site_name, :country_iso2 => ByRow(x -> String(x)) => :country_iso2)
        savedataframe(db, sites, "sites")
        @info "Site names and country iso2 codes ingested."
    end
    return nothing
end

"""
    read_sitedata(source::AbstractSource, datapath, sourceid)

Data are aggregated by site_col in order to identify unique sites
"""
function read_sitedata(source::AbstractSource, datapath, sourceid)
    df = read_data(DocCSV(joinpath(datapath, source.name, source.datafolder),
        source.site_data,
        source.delim, source.quotechar, source.dateformat, source.decimal))
    sites = combine(groupby(df, source.site_col), nrow => :n)
    insertcols!(sites, 1, :source_id => sourceid)
    return (sites)
end

"""
    add_protocols(source::AbstractSource, db::SQLite.DB, datapath::String)

Add protocols
Todo: how protocols link to enthics_id, need a mapping dictionary? 
"""
function add_protocols(source::AbstractSource, db, datapath::String)

    # Insert protocol names
    stmt_name = prepareinsertstatement(db, "protocols", ["name", "description"])

    # Insert protocol documents
    stmt_doc = prepareinsertstatement(db, "protocol_documents", ["protocol_id", "name", "document"])

    # Insert site protocols 
    stmt_site = prepareinsertstatement(db, "site_protocols", ["site_id", "protocol_id"])

    sites = selectsourcesites(db, source)

    for (key, value) in source.protocols

        # Add protocol
        DBInterface.execute(stmt_name, [value, key])

        # Get protocol id
        protocol_id = get_namedkey(db, "protocols", value, Symbol("protocol_id"))

        # Add site protocol
        for site in eachrow(sites)
            DBInterface.execute(stmt_site, [site.site_id, protocol_id])
        end

        # Add protocol documents
        file = read_data(DocPDF(joinpath(datapath, source.name, source.protocolfolder), "$value"))
        DBInterface.execute(stmt_doc, [protocol_id, value, file])
        @info "Protocol document $value ingested."
    end
    return nothing
end

"""
    add_instruments(source::AbstractSource, db::SQLite.DB, datapath::String)

Add survey instruments
"""
function add_instruments(source::AbstractSource, db, datapath::String)

    # Insert instrument names
    stmt_name = prepareinsertstatement(db, "instruments", ["name", "description"])

    # Insert instrument documents
    stmt_doc = prepareinsertstatement(db, "instrument_documents", ["instrument_id", "name", "document"])

    for (key, value) in source.instruments

        # Add instruments
        DBInterface.execute(stmt_name, [value, key])

        # Get instrument id
        instrument_id = get_namedkey(db, "instruments", value, Symbol("instrument_id"))

        # Add instrument documents
        file = read_data(DocPDF(joinpath(datapath, source.name, source.instrumentfolder), "$value"))
        DBInterface.execute(stmt_doc, [instrument_id, value, file])
        @info "Instrument document $value ingested."
    end
    return nothing
end

"""
    add_ethics(source::AbstractSource, db::DBInterface.Connection, datapath::String)

Ethics document, committee and reference need to be in matching order

"""
function add_ethics(source::AbstractSource, db::DBInterface.Connection, datapath::String; source_id=nothing)

    if isnothing(source_id)
        source_id = get_source(db, source.name)
    end
    # Insert ethics names
    stmt_name = prepareinsertstatement(db, "ethics", ["source_id", "name", "ethics_committee", "ethics_reference"])

    for (key, value) in source.ethics
        DBInterface.execute(stmt_name, [source_id, value[2], key, value[1]])
    end
    # get inserted ethics
    df_ethics = selectdataframe(db, "ethics", ["ethics_id", "name"], ["source_id"], [source_id])
    # Insert ethics documents
    stmt_doc = prepareinsertstatement(db, "ethics_documents", ["ethics_id", "name", "description", "document"])

    for (key, value) in source.ethics
        file = read_data(DocPDF(joinpath(datapath, source.name, source.ethicsfolder), "$(value[2])"))

        # Get ethics id
        ethics_id = df_ethics[df_ethics.name .== value[2], :ethics_id][1]

        DBInterface.execute(stmt_doc, [ethics_id, value[2], "$key ($(value[1]))", file])
        @info "Ethics document $(value[2]) ingested. Ethics id = $ethics_id."
    end
    return nothing
end

"""
    add_variables(variables::AbstractDataFrame, db::SQLite.DB, domain_id::Integer)

Add variables from a variable dataframe to variables table
"""
function add_variables(variables::AbstractDataFrame, db::SQLite.DB, domain_id::Integer)

    # Check if variables dataframe has all required columns
    required_columns = ["Column_Name", "DataType", "Description", "Note", "Vocabulary"]
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
    add_variables(variables::AbstractDataFrame, db::ODBC.Connection, domain_id::Integer)

Add variables from a variable dataframe to variables table
"""
function add_variables(variables::AbstractDataFrame, db::ODBC.Connection, domain_id::Integer)

    # Check if variables dataframe has all required columns
    required_columns = ["Column_Name", "DataType", "Description", "Note", "Vocabulary"]
    missing_columns = filter(col -> !(col in names(variables)), required_columns)
    if !isempty(missing_columns)
        error("Variables dataframe missing columns: ", join(missing_columns, ", "))
    end
    # ODBC can't deal with InlineStrings
    transform!(variables, :Column_Name => ByRow(x -> String(x)) => :Column_Name)

    # Check if variable exists
    sql = """
        SELECT 1 
        FROM variables 
        WHERE domain_id = ? AND name = ?
    """
    exist_stmt = DBInterface.prepare(db, sql)
    sql = """
        UPDATE variables
        SET 
            vocabulary_id = ?,
            description = ?,
            note = ?
        WHERE domain_id = ? AND name = ? 
    """
    update_stmt = DBInterface.prepare(db, sql)
    sql = """
    INSERT INTO variables (domain_id, name, value_type_id, vocabulary_id, description, note)
    VALUES (?, ?, ?, ?, ?, ?)
    """
    insert_stmt = DBInterface.prepare(db, sql)
    # Add variables
    if !("domain_id" in names(variables))
        insertcols!(variables, 1, :domain_id => domain_id)
    end

    for row in eachrow(variables)
        id = missing
        if !ismissing(row.Vocabulary)
            id = add_vocabulary(db, row.Vocabulary)
        end
        var_exist = !isempty(DBInterface.execute(exist_stmt, [row.domain_id, row.Column_Name]))
        if var_exist
            DBInterface.execute(update_stmt, [id, row.Description, row.Note, row.domain_id, row.Column_Name])
        else
            DBInterface.execute(insert_stmt, [row.domain_id, row.Column_Name, row.DataType, id, row.Description, row.Note])
        end
    end
    return nothing
end

"""
    add_vocabulary(db::SQLite.DB, vocabulary::Vocabulary)

Insert a vocabulary and its items into a RDA database, returns the vocabulary_id of the inserted vocabulary
"""
function add_vocabulary(db, vocabulary::Vocabulary)
    id = get_namedkey(db, "vocabularies", vocabulary.name, "vocabulary_id")
    if !ismissing(id)
        return id
    end
    id = insertwithidentity(db, "vocabularies", ["name", "description"], [vocabulary.name, vocabulary.description], "vocabulary_id")
    stmt = prepareinsertstatement(db, "vocabulary_items", ["vocabulary_id", "value", "code", "description"])
    for item in vocabulary.items
        DBInterface.execute(stmt, [id, item.value, item.code, item.description])
    end
    return id
end

"""
    read_variables(source::AbstractSource, dictionarypath::String, dictionaryname::String)

Read a csv file listing variables, variable descriptions and data types in a dataset.

"""
function read_variables(source::AbstractSource, dictionarypath::String, dictionaryname::String)

    df = read_data(DocCSV(joinpath(dictionarypath, "$(source.domain_name)"), dictionaryname,
        ';', source.quotechar, source.dateformat, source.decimal)) #dictionaries are delimited by semi-colons, not the default source delimeter
    vocabularies = Vector{Union{Vocabulary,Missing}}()
    for row in eachrow(df)
        if !ismissing(row.Description) && length(lines(row.Description)) > 1
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
    add_transformation(db::SQLite.DB, type::Integer, status::Integer, description::String, code_reference::String, date_created::Date, created_by::String)

Add a transformation to the transformations table
"""
function add_transformation(db::SQLite.DB, type::Integer, status::Integer, description::String, code_reference::String, date_created::Date, created_by::String)
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
    save_dataset(db::DBInterface.Connection, dataset::AbstractDataFrame, name::String, description::String,
    domain_id::Integer, transformation_id::Integer, ingestion_id::Integer)::Integer

Insert dataframe containing dataset into RDA database and returns the dataset_id
"""
function save_dataset(db::DBInterface.Connection, dataset::AbstractDataFrame, name::String, description::String,
    domain_id::Integer, transformation_id::Integer, ingestion_id::Integer)::Integer

    variables = lookup_variables(db, names(dataset), domain_id)
    transform!(variables, [:variable_id, :value_type_id] => ByRow((x, y) -> Tuple([x, y])) => :variable_id_type)
 
    var_lookup = Dict{String,Tuple{Integer,Integer}}(zip(variables.name, variables.variable_id_type))

    # Add dataset entry to datasets table
    dataset_id = insertwithidentity(db, "datasets", ["name", "date_created", "description"],
        [name, isa(db, SQLite.DB) ? Dates.format(today(), "yyyy-mm-dd") : today(), description], "dataset_id")

    insertdata(db, "ingest_datasets", ["data_ingestion_id", "transformation_id", "dataset_id"],
        [ingestion_id, transformation_id, dataset_id])
    insertdata(db, "transformation_outputs", ["transformation_id", "dataset_id"],
        [transformation_id, dataset_id])

    savedataframe(db, select(variables, [] => Returns(dataset_id) => :dataset_id, :variable_id), "dataset_variables")

    # Store datarows in datarows table and get row_ids 
    datarows = add_datarows(db, nrow(dataset), dataset_id)

    #prepare data for storage
    d = hcat(datarows, dataset, makeunique=true, copycols=false) #add the row_id to each row of data
    #store whole column at a time
    for col in propertynames(dataset)
        variable_id, value_type = var_lookup[string(col)]
        coldata = select(d, :row_id, col => :value; copycols=false)
        #println("Saving column $col : type=$(eltype(coldata.value)) : value_type=$value_type : $name.")
        add_data_column(db, variable_id, value_type, coldata)
    end
    @info "Dataset $name ingested."
    return dataset_id
end

"""
    add_data_column(db::SQLite.DB, variable_id, coldata)

Insert data for a column of the source dataset
"""
function add_data_column(db::SQLite.DB, variable_id, value_type, coldata)
    stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value"])
    if eltype(coldata.value) <: TimeType
        if value_type == RDA_TYPE_DATE
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "yyyy-mm-dd") : x) => :value)
        elseif value_type == RDA_TYPE_TIME
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "HH:MM:SS.sss") : x) => :value)
        elseif value_type == RDA_TYPE_DATETIME
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x, "yyyy-mm-ddTHH:MM:SS.sss") : x) => :value)
        else
            error("Variable $variable_id is not a date/time type. value_type = $value_type, eltype = $(eltype(coldata.value))")
        end
    end
    for row in eachrow(coldata)
        DBInterface.execute(stmt, [row.row_id, variable_id, row.value])
    end
    return nothing
end
"""
    add_data_column(db::ODBC.Connection, variable_id, value_type, coldata)

Insert data for a column of the source dataset
"""
function add_data_column(db::ODBC.Connection, variable_id, value_type, coldata)
    #println("Add data column variable_id = $variable_id, value_type = $value_type, eltype = $(eltype(coldata.value))")
    if value_type == RDA_TYPE_INTEGER
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_integer"])
    elseif value_type == RDA_TYPE_FLOAT
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_float"])
    elseif value_type == RDA_TYPE_STRING
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_string"])
        if eltype(coldata.value) <: Union{Missing,Number}
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? string(x) : x) => :value)
        elseif eltype(coldata.value) <: Union{Missing,TimeType}
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? Dates.format(x,"yyyy-mm-dd") : x) => :value)
        else
            transform!(coldata, :value => ByRow(x -> !ismissing(x) ? String(x) : x) => :value)
        end
    elseif value_type == RDA_TYPE_DATE || value_type == RDA_TYPE_TIME || value_type == RDA_TYPE_DATETIME
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_datetime"])
    elseif value_type == RDA_TYPE_CATEGORY && eltype(coldata.value) <: Union{Missing,Integer}
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_integer"])
    elseif value_type == RDA_TYPE_CATEGORY && eltype(coldata.value) <: Union{Missing,AbstractString}
        stmt = prepareinsertstatement(db, "data", ["row_id", "variable_id", "value_string"])
        transform!(coldata, :value => ByRow(x -> !ismissing(x) ? String(x) : x) => :value)
    else
        error("Variable $variable_id is not a valid type. value_type = $value_type, eltype = $(eltype(coldata.value))")
    end
    for row in eachrow(coldata)
        DBInterface.execute(stmt, [row.row_id, variable_id, row.value])
    end
    return nothing
end

"""
    lookup_variables(db, variable_names, domain)

Returns a DataFrame with dataset variable names and ids
"""
function lookup_variables(db, variable_names, domain)
    names = DataFrame(:name => variable_names)
    variables = selectdataframe(db, "variables", ["name", "variable_id", "value_type_id"], ["domain_id"], [domain]) |> DataFrame
    return innerjoin(variables, names, on=:name) #just the variables in this dataset
end

"""
    link_instruments(db::SQLite.DB, instrument_name, dataset_name)

Insert records into `instrument_datasets` table, linking datasets with instruments.
"""
function link_instruments(db::DBInterface.Connection, instrument_name::String, dataset_name::String)

    # get id for dataset and matching instrument
    dataset_id = get_namedkey(db, "datasets", dataset_name, :dataset_id)
    instrument_id = get_namedkey(db, "instruments", instrument_name, :instrument_id)

    if ismissing(dataset_id)
        error("Data file $dataset_name is not ingested.")
    end
    if ismissing(instrument_id)
        error("Instrument file $instrument_name is not ingested.")
    end

    insertdata(db, "instrument_datasets", ["instrument_id", "dataset_id"], [instrument_id, dataset_id])

    @info "Linked dataset $dataset_name to instrument $instrument_name"

    return nothing
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
    link_deathrows(db::ODBC.Connection, ingestion_id, dataset_id, death_identifier)

Insert records into `deathrows` table to link dataset `dataset_id` to `deaths` table. Limited to a specific ingest.
`death_identifier` is the variable in the dataset that corresponds to the `external_id` of the death.
"""
function link_deathrows(db::ODBC.Connection, ingestion_id, dataset_id, death_identifier)

    sql = """
        INSERT INTO death_rows (death_id, row_id)
        SELECT
            d.death_id,
            data.row_id
        FROM deaths d
        JOIN data ON d.external_id = data.value_string
        JOIN datarows r ON data.row_id = r.row_id
        WHERE d.data_ingestion_id = ?
        AND data.variable_id = ?
        AND r.dataset_id = ?
        AND NOT EXISTS (
            SELECT 1
            FROM death_rows dr
            WHERE dr.death_id = d.death_id AND dr.row_id = data.row_id
        )
    """
    stmt = DBInterface.prepare(db, sql)
    DBInterface.execute(stmt, [ingestion_id, death_identifier, dataset_id])

    return nothing
end

"""
    death_in_ingest(db, ingestion_id)

If ingested deaths are part of ingestion_id
"""
function death_in_ingest(db, ingestion_id)
    df = selectdataframe(db, "deaths", ["COUNT(*)"], ["data_ingestion_id"], [ingestion_id]) |> DataFrame
    return nrow(df) > 0 && df[1, 1] > 0
end

"""
dataset_in_ingest(db, dataset_id, ingestion_id)

If dataset_id is part of ingestion_id
"""
function dataset_in_ingest(db, dataset_id, ingestion_id)
    df = selectdataframe(db, "ingest_datasets", ["COUNT(*)"], ["data_ingestion_id", "dataset_id"], [ingestion_id, dataset_id]) |> DataFrame
    return nrow(df) > 0 && df[1, 1] > 0
end

"""
read_data(datadoc)

Read file names and formatting parameters, returns a DataFrame with the data
"""
function read_data(datadoc::DocPDF)
    file = joinpath(datadoc.path, "$(datadoc.name)") #.pdf
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = read(file)
        return df
    end
end
function read_data(datadoc::DocCSV)
    file = joinpath(datadoc.path, "$(datadoc.name).csv")
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = CSV.File(file; delim=datadoc.delim, quotechar=datadoc.quotechar,
            dateformat=datadoc.dateformat, decimal=datadoc.decimal) |> DataFrame
        return df
    end
end
function read_data(datadoc::DocXLSX)
    file = joinpath(datadoc.path, "$(datadoc.name)") #xlsx
    if !isfile(file)
        error("File '$file' not found.")
    else
        df = DataFrame(XLSX.readdata(file, datadoc.sheetname, datadoc.cellrange), :auto)
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
    dataset_variables(db::SQLite.DB, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::SQLite.DB, dataset)::AbstractDataFrame
    sql = """
    SELECT
        v.variable_id,
        v.name variable,
        v.value_type_id
    FROM dataset_variables dv
      JOIN variables v ON dv.variable_id = v.variable_id
    WHERE dv.dataset_id = @dataset;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (dataset = dataset)) |> DataFrame
end
"""
    dataset_variables(db::ODBC.Connection, dataset)::AbstractDataFrame

Return the list of variables in a dataset
"""
function dataset_variables(db::ODBC.Connection, dataset)::AbstractDataFrame
    sql = """
    SELECT
        v.variable_id,
        v.name variable,
        v.value_type_id
    FROM dataset_variables dv
      JOIN variables v ON dv.variable_id = v.variable_id
    WHERE dv.dataset_id = ?;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, [dataset]; iterate_rows=true) |> DataFrame
end
"""
    dataset_column(db::SQLite.DB, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame

Return one column of data in a dataset (representing a variable)
"""
function dataset_column(db::SQLite.DB, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame
    sql = """
    SELECT
        d.row_id,
        d.value as $variable_name
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
    WHERE r.dataset_id = @dataset_id
      AND d.variable_id = @variable_id;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, (dataset_id = dataset_id, variable_id=variable_id)) |> DataFrame
end
"""
    get_valuetype(db::ODBC.Connection, variable_id::Integer)

Get the data type of a variable
"""
function get_valuetype(db::ODBC.Connection, variable_id::Integer)
    sql = """
    SELECT
        value_type_id
    FROM variables
    WHERE variable_id = ?;
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, [variable_id]; iterate_rows=true) |> DataFrame
    if nrow(df) > 0
        if df[1, :value_type_id] == RDA_TYPE_INTEGER
            return "value_integer"
        elseif df[1, :value_type_id] == RDA_TYPE_FLOAT
            return "value_float"
        elseif df[1, :value_type_id] == RDA_TYPE_STRING
            return "value_string"
        elseif df[1, :value_type_id] == RDA_TYPE_DATE || df[1, :value_type_id] == RDA_TYPE_TIME || df[1, :value_type_id] == RDA_TYPE_DATETIME
            return "value_datetime"
        elseif df[1, :value_type_id] == RDA_TYPE_CATEGORY
            return "COALESCE(CAST(d.value_integer AS varchar), d.value_string)"
        else 
            error("Variable $variable_id has an invalid value type $(df[1, :value_type_id]).")
        end
    else
        error("Variable $variable_id not found.")
    end
end
"""
    dataset_column(db::ODBC.Connection, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame

Return one column of data in a dataset (representing a variable)
"""
function dataset_column(db::ODBC.Connection, dataset_id::Integer, variable_id::Integer, variable_name::String)::AbstractDataFrame
    value_column = get_valuetype(db, variable_id)
    sql = """
    SELECT
        d.row_id,
        $value_column as $variable_name
    FROM data d
      JOIN datarows r ON d.row_id = r.row_id
    WHERE r.dataset_id = ?
      AND d.variable_id = ?;
    """
    stmt = DBInterface.prepare(db, sql)
    return DBInterface.execute(stmt, [dataset_id, variable_id]; iterate_rows=true) |> DataFrame
end
"""
    dataset_to_dataframe(db::ODBC.Connection, dataset)::AbstractDataFrame

Extract a dataset into a DataFrame
"""
function dataset_to_dataframe(db::ODBC.Connection, dataset)::AbstractDataFrame
    variables = dataset_variables(db, dataset)
    df = DataFrame()
    col1 = true
    for variable in eachrow(variables)
       if col1
           df = dataset_column(db, dataset, variable.variable_id, variable.variable)
           col1 = false
       else
           df = outerjoin(df, dataset_column(db, dataset, variable.variable_id, variable.variable), on=:row_id)
       end 
    end
    return df
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
function get_datasetname(db::SQLite.DB, dataset)
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
    get_datasetname(db::ODBC.Connection, dataset)

Get the name of a dataset
"""
function get_datasetname(db::ODBC.Connection, dataset)
    sql = """
    SELECT
      name
    FROM datasets
    WHERE dataset_id = ?
    """
    stmt = DBInterface.prepare(db, sql)
    df = DBInterface.execute(stmt, [dataset], iterate_rows=true) |> DataFrame
    if nrow(df) == 0
        return missing
    else
        return df[1, :name]
    end
end

"""
    lines(str)

Returns an array of lines in `str` 
"""
lines(str) = split(str, '\n')


"""
    get_last_deathingest(source)

Get ingestion id for latest death ingestion for source
"""
function get_last_deathingest(db::SQLite.DB, source_id::Integer)
    sql = """
    SELECT 
        di.data_ingestion_id
    FROM data_ingestions di
    JOIN deaths d ON di.data_ingestion_id = d.data_ingestion_id
    WHERE source_id = @source_id
    ORDER BY di.date_received DESC
    LIMIT 1;
    """
    death_ingests = DBInterface.execute(db, sql, (source_id = source_id)) |> DataFrame
    if nrow(death_ingests) > 0
        return death_ingests[1, :data_ingestion_id]
    else
        error("Death from source $source_id hasn't been ingested.")
    end
end
"""
    get_last_deathingest(db::ODBC.Connection, source_id::Integer)

Get ingestion id for latest death ingestion for source
"""
function get_last_deathingest(db::ODBC.Connection, source_id::Integer)
    sql = """
    SELECT TOP 1
        di.data_ingestion_id
    FROM data_ingestions di
    JOIN deaths d ON di.data_ingestion_id = d.data_ingestion_id
    WHERE source_id = ?
    ORDER BY di.date_received DESC;
    """
    death_ingests = DBInterface.execute(db, sql, [source_id]) |> DataFrame
    if nrow(death_ingests) > 0
        return death_ingests[1, :data_ingestion_id]
    else
        error("Death from source $source_id hasn't been ingested.")
    end
end

"""
    add_datarows(db::SQLite.DB, nrow::Integer, dataset_id::Integer)

    Define data rows in the datarows table
"""
function add_datarows(db::DBInterface.Connection, nrow::Integer, dataset_id::Integer)
    stmt = prepareinsertstatement(db, "datarows", ["dataset_id"])
    #Create a row_id for every row in the dataset
    for i = 1:nrow
        DBInterface.execute(stmt, [dataset_id])
    end
    return selectdataframe(db, "datarows", ["row_id"], ["dataset_id"], [dataset_id]) |> DataFrame
end

include("constants.jl")
include("rdadatabase.jl")

end #module