using Pkg; Pkg.add.(["Plots", "DSP"])
Pkg.add("GR")
Pkg.add("Weave")
using Plots
using DSP
using Weave
using Gadfly
Pkg.add("Mustache")
using(Mustache)
Pkg.add("Gadfly")
using libGR.so


filename = normpath("/Users/young/Documents/GitHub/RDAIngest.jl/test", "CHAMPS_VA_Quality_Check.jmd")

# Julia markdown to HTML
weave(filename; doctype = "md2html", out_path = :pwd)
