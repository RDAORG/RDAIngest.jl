using Pkg
Pkg.add("Weave")
using Weave


filename = normpath("/Users/young/Documents/GitHub/RDAIngest.jl/CHAMPS", "CHAMPS_VA_Quality_Report.jmd")

# Julia markdown to HTML
weave(filename; doctype = "md2html", out_path = :pwd)
