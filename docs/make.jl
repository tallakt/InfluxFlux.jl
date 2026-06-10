using Documenter
using InfluxFlux

makedocs(
    sitename = "InfluxFlux.jl",
    modules  = [InfluxFlux],
    format   = Documenter.HTML(
        canonical = "https://tallakt.github.io/InfluxFlux.jl"
    ),
    pages = [
        "Home"      => "index.md",
    ],
)

deploydocs(
    repo = "github.com/tallakt/InfluxFlux.jl",
    devbranch = "main",
)
