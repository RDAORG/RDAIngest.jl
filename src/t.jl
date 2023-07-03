abstract type AbstractSource end
Base.@kwdef struct CHAMPSSource <: AbstractSource
    name::String = "CHAMPS"
    site_data::String = "CHAMPS_deid_basic_demographics"
    site_col::String = "site_iso_code"
    protocols::Vector{String} = ["CHAMPS-Mortality-Surveillance-Protocol-v1.3", 
                                 "CHAMPS-Social-Behavioral-Science-Protocol-v1.0"]
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
end
s = CHAMPSSource()
function tp(s::AbstractSource)
    println(s.site_data)
end
tp(s)