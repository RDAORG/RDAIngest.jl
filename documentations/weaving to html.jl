#Pkg.add("Weave")
using Weave


filename = normpath("/Users/young/Documents/GitHub/RDAIngest.jl/documentations", "CHAMPS_VA_Quality_Check.jmd")

# Julia markdown to HTML
weave(filename; doctype = "md2html", out_path = :pwd)
