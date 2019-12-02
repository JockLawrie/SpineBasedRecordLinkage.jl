using Test
using SpineBasedRecordLinkage

using CSV
using DataFrames

# NOTE: pwd is /path/to/SpineBasedRecordLinkage.jl/test/

# Support functions
function cleanup()
    contents = readdir("output")
    for x in contents
        rm(joinpath(pwd(), "output", x); recursive=true)
    end
end

# Input data
emergencies   = DataFrame(CSV.File(joinpath("data", "emergency_presentations.csv")))
admissions    = DataFrame(CSV.File(joinpath("data", "hospital_admissions.csv")))
notifications = DataFrame(CSV.File(joinpath("data", "notifiable_disease_reports.csv")))

# Test sets
#include("testset1.jl")
include("testset2.jl")
