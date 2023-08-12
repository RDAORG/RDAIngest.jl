module RDATransform

"""
A Julia version of PyCrossVA
https://github.com/verbal-autopsy-software/pyCrossVA/tree/master/pycrossva

"""

using DataFrames
using SQLite
using DBInterface
using ConfigEnv
using CSV

using XLSX
using Dates
using Arrow
using DataStructures
#using OrderedCollections

# include RDAIngest.jl

export transform

"""
Structs
"""
const SUPPORTED_INPUTS = ["2016WHOv151", "2012WHO", "2022WHO"] #, "2016WHOv141", "PHRMCShort"
const SUPPORTED_OUTPUTS = ["InterVA5", "InSilicoVA"] #"InterVA4", 


abstract type AbstractInput end
Base.@kwdef struct CHAMPSVA <: AbstractInput
    dataset_name = "CHAMPS_deid_verbal_autopsy"
    input_type::String = "2016WHOv151"
    output_type::Vector{String} = ["InterVA5","InSilicoVA"]
end

Base.@kwdef struct COMSAVA <: AbstractInput
    dataset_name = "COMSA_WHO_VA_20230308"
    input_type::String = "2016WHOv151"
    output_type::Vector{String} = ["InterVA5","InSilicoVA"]
end

"""
Core functions
"""

function source_transform(input::AbstractInput,dbpath::String, dbname::String)

    db = opendatabase(dbpath, dbname)
    
    result = DBInterface.execute(db, "SELECT dataset_id FROM datasets WHERE name = '$(input.dataset_name)';") |> DataFrame

    raw_va = dataset_to_dataframe(db, result.dataset_id[1])

    for output_type in input.output_type
    transformed_va = va_transform(raw_va,
                                    input.intput_type, 
                                    output_type)
    # save output to dataset

    println("")
    end
    return nothing

end

function va_transform(raw_data, input_type, output_type, 
                   lower::Bool=false, preserve_na::Bool = true #format
                    )   
    
    mapping_name = 

                    expected_filename = joinpath(internal_path, "$(mapping[1])_to_$(mapping[2]).csv")
                    if isfile(expected_filename)
                        mapping_data = DataFrame(CSV.File(expected_filename))
                    else
                        throw(ArgumentError("No mapping supporting $(mapping[1]) to $(mapping[2]) currently exists."))
                    end
                else
                    throw(ArgumentError("Output not supported. Expected one of $(SUPPORTED_OUTPUTS), but received $(mapping[2])"))
                end
            else
                throw(ArgumentError("Input not supported. Expected one of $(SUPPORTED_INPUTS), but received $(mapping[1])"))
            end
        else
            throw(ArgumentError("If mapping is tuple, input should be of length two in the form (input type, output type)"))
        end
    else
        mapping_data = CSV.read(mapping)
    end

    if isempty(mapping_data)  # this shouldn't happen; if it does, throw
        throw(ArgumentError("No valid mapping data provided to transform. Should be either a tuple in form (input, output), a path to csv or a DataFrame."))
    end

    # init configuration obj from given mapping data
    config = Configuration(config_data=mapping_data, verbose=verbose, process_strings=false)
    if lower
        config.config_data[!, "Source Column ID"] = lowercase.(config.config_data[!, "Source Column ID"])
        config.source_columns = lowercase.(config.source_columns)
    end

    # if the configuration isn't valid, or if the data isn't valid for the config file, then throw error
    if !validate(config, verbose=verbose)
        throw(ArgumentError("Configuration from mapping file must be valid before transform."))
    end

    # TODO adds args to init based on data type?
    input_data = flexible_read(raw_data) |> DataFrame
    if lower
        rename!(input_data, Symbol.(lowercase.(names(input_data))))
    end
    cross_va = CrossVA(input_data, config)
    if !validate(cross_va, verbose=verbose)
        return
        # throw(ArgumentError("Cannot transform if provided raw data is not valid for configuration file."))
    end
    final_data = process(cross_va)
    # if result values have been changed, then map as directed, otherwise
    # leave alone - the default values are what we actually have, so we don't
    # need to do any mapping if they have not specified an alternative.

    defaults = Dict("Present"=>1, "Absent"=>0, "NA"=>missing)
    if result_values != defaults
        actual_mapping = Dict(value=>result_values[key] for (key, value) in defaults)
        final_data = replace(final_data, actual_mapping)
    end

    if !isnothing(raw_data_id)
        try
            if lower
                insert!(final_data, 1, raw_data_id => input_data[Symbol(raw_data_id)])
            else
                insert!(final_data, 1, raw_data_id => input_data[raw_data_id])
            end
        catch e
            throw(ArgumentError("Could not find column named $raw_data_id in raw_data."))
        end
    else
        final_data[!, "ID"] = axes(final_data, 1) .+ 1
    end

    if preserve_na
        return final_data
    end
    return coalesce.(final_data, 0)
end

function transform(mapping::Tuple, raw_data::String; kwargs...)
    return transform(mapping, raw_data; kwargs...)
end

function transform(mapping::String, raw_data::String; kwargs...)
    return transform(mapping, raw_data; kwargs...)
end

function transform(mapping::DataFrame, raw_data::String; kwargs...)
    return transform(mapping, raw_data; kwargs...)
end

function transform(raw_data::String, mapping::String; kwargs...)
    return transform(mapping, raw_data; kwargs...)
end

function transform(raw_data::String, mapping::DataFrame; kwargs...)
    return transform(mapping, raw_data; kwargs...)
end

function transform(raw_data::String, mapping::Tuple; kwargs...)
    return transform(mapping, raw_data; kwargs...)
end

const SUPPORTED_OUTPUTS = ["InterVA5", "InterVA4", "InSilicoVA"]

function validate(args...; verbose::Int=2)
    println("Not implemented in the translation as it refers to external functions.")
    return true
end

function process(args...; verbose::Int=2)
    println("Not implemented in the translation as it refers to external functions.")
end

const input_data = "resources/sample_data/2016WHO_mock_data_1.csv"
const mapping = ("2016WHOv151", "InterVA4")

transform_result = transform(mapping, input_data)
println(transform_result[1:5, ["ACUTE", "CHRONIC", "TUBER"]])

# Note: The remaining parts of the Python script are for testing purposes using doctest
# They are not directly related to the main function's translation, so I have omitted them in the Julia translation.


############

function read_sitedata(source::AbstractSource, sourceid::Int64, datapath::string)
    df = read_data(joinpath(datapath,source.name,source.datafolder), source.site_data, 
                    extension=source.extension, delim=source.delim, quotechar=source.quotechar, 
                    dateformat=source.dateformat, decimal=source.decimal)
    sites = combine(groupby(df, source.site_col), nrow => :n)
    insertcols!(sites, 1, :source_id => sourceid)
    return sites    
end

function add_sites(source::CHAMPSSource, db::SQLite.DB, sourceid::Int64, datapath::String)
    sites = read_sitedata(source, sourceid, datapath)
    select!(sites, 
            source.site_col => ByRow(x -> x) => :site, 
            source.country_col => ByRow(x -> x) => :country_iso, 
            :source_id)
    savedataframe(db, sites, "sites")
    return nothing
end
function add_sites(source::COMSASource, db::SQLite.DB, sourceid::Int64, datapath::String)
    sites = read_sitedata(source, sourceid, datapath)
    select!(sites, source.site_col => ByRow(x -> x) => :name, [] => Returns("MW") => :site_iso_code, :source_id)
    savedataframe(db, sites, "sites")
    return nothing
end

end