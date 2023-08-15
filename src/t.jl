

using DataFrames
using CSV
using SQLite
using DBInterface
using ConfigEnv
using CSV
using Dates
using Arrow
using DataStructures

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
        ethics::OrderedDict{String, Dict{String,String}} = OrderedDict("IRB 1" => Dict("IRB 1"=>"IRB1","IRB 2"=>"IRB2"),
                                                               "IRB 2" => Dict("IRB 3"=>"IRB1","IRB 4"=>"IRB4"))
        ethics_committee::Vector{String} = ["Emory University","Country IRB"]
        ethics_reference::Vector{String} = ["TBD","TBD"]
    
        # Source-released data dictionary
        datadictfolder::String = "De_identified_data"
        datadict::Vector{String} = ["CHAMPS De-Identified Data Set Description v4.2"]
        datadict_extension::String = "pdf"
    
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

        #= # Metadata
        ingestion::String = "CHAMPS Level-2 Data accessed 20230518"
        transformation::String = "Ingest of CHAMPS de-identified data"
        code_reference::String = "Multiple dispatch testing"
        author::String = "Yue Chu"
        description::String = "Raw CHAMPS level 2 data release v4.2"
        =#
    
end

Base.@kwdef struct COMSASource <: AbstractSource
        name::String = "COMSA"
        datafolder::String = "De_identified_data"
        
        site_data::String = "Comsa_death_20230308"
        site_col::String = "provincia"
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
        datadictfolder::String = "De_identified_data"
        datadict::Vector{String} = ["Comsa_data_dictionary_20190909"]
        datadict_extension::String = "xlsx"
    
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
s = CHAMPSSource()

function tp(s::AbstractSource)
    println(s.site_data)
end
tp(s)

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

struct COMSASource <: AbstractSource
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
    
        
    

end

Dict{

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

}
 
        # Metadata
        #ingestion::String = "CHAMPS Level-2 Data accessed 20230518"
        #transformation::String = "Ingest of CHAMPS de-identified data"
        #code_reference::String = "Multiple dispatch testing"
        #author::String = "Yue Chu"
        #description::String = "Raw CHAMPS level 2 data release v4.2"
    