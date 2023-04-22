using RDAIngest
using Documenter

DocMeta.setdocmeta!(RDAIngest, :DocTestSetup, :(using RDAIngest); recursive=true)

makedocs(;
    modules=[RDAIngest],
    authors="Kobus Herbst<kobus.herbst@ahri.org>",
    repo="https://github.com/kobusherbst/RDAIngest.jl/blob/{commit}{path}#{line}",
    sitename="RDAIngest.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://kobusherbst.github.io/RDAIngest.jl",
        edit_link="master",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
    ],
)

deploydocs(;
    repo="github.com/kobusherbst/RDAIngest.jl",
    devbranch="master",
)
