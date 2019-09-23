using Pkg
Pkg.activate(".")
println("Loading SpineBasedRecordLinkage.jl")
using SpineBasedRecordLinkage
configfile = ARGS[1]
run_linkage(configfile)