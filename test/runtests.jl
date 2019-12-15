using Test
using SpineBasedRecordLinkage

using CSV
using DataFrames
using Schemata

# NOTE: pwd is /path/to/SpineBasedRecordLinkage.jl/test/
const outdir = joinpath(pwd(), "output")

if !isdir(outdir)
    mkdir(outdir)
end

# Support functions
function cleanup()
    contents = readdir(outdir)
    for x in contents
        rm(joinpath(outdir, x); recursive=true)
    end
end

# Test sets
cleanup()
include("testset1.jl")  # Construct spine from 1 data set
include("testset2.jl")  # Construct spine from multiple data sets
include("testset3.jl")  # Compare the results from test sets 1 and 2
cleanup()
