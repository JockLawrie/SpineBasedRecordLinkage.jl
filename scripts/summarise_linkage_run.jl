using Pkg
Pkg.activate(".")
println("Loading SpineBasedRecordLinkage.jl")
using SpineBasedRecordLinkage
if length(ARGS) != 2
    error("The number of arguments is $(length(ARGS)), should be 2.")
end
directory1 = ARGS[1]
outfile    = ARGS[2]
summarise_linkage_run(directory1, outfile)