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
"Returns: A set of Dict{Symbol, Any}, with each element being a row of the table in the supplied datafile."
function table_to_set_of_dicts(datafile::String)
    result   = Set{Dict{Symbol, Any}}()
    data     = DataFrame(CSV.File(datafile))
    colnames = names(data)
    for row in eachrow(data)
        d = Dict{Symbol, Any}(colname => getproperty(row, colname) for colname in colnames)
        push!(result, d)
    end
    result
end

function cleanup()
    contents = readdir(outdir)
    for x in contents
        rm(joinpath(outdir, x); recursive=true)
    end
end

# Test sets
cleanup()
include("testset1.jl")  # Construct spine from 1 table (influenza cases)
include("testset2.jl")  # Construct spine from multiple data sets
include("testset3.jl")  # Compare the results from test sets 1 and 2
cleanup()
