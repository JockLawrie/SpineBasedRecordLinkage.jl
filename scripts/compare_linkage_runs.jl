using Pkg
Pkg.activate(".")
println("Loading SpineBasedRecordLinkage.jl")
using SpineBasedRecordLinkage
directory1 = ARGS[1]
directory2 = ARGS[2]
outfile    = ARGS[3]
compare_linkage_runs(directory1, directory2, outfile)