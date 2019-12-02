using Test
using SpineBasedRecordLinkage

using CSV
using DataFrames

# NOTE: pwd is /path/to/SpineBasedRecordLinkage.jl/test/

# Input data
emergencies   = DataFrame(CSV.File(joinpath("data", "emergency_presentations.csv")))
admissions    = DataFrame(CSV.File(joinpath("data", "hospital_admissions.csv")))
notifications = DataFrame(CSV.File(joinpath("data", "notifiable_disease_reports.csv")))

# Construct spine
outdir = construct_spine(joinpath("config", "construct_spine.yml"))
spine  = DataFrame(CSV.File(joinpath(outdir, "output", "spine.tsv"); delim='\t'))
@test size(spine, 1) == 3

# Linkage
cp(joinpath(outdir, "output", "spine.tsv"), joinpath("output", "spine.tsv"))
outdir     = run_linkage(joinpath("config", "linkagerun1.yml"))
ep_linked  = DataFrame(CSV.File(joinpath(outdir, "output", "emergency_presentations_linked.tsv"); delim='\t'))
ha_linked  = DataFrame(CSV.File(joinpath(outdir, "output", "hospital_admissions_linked.tsv"); delim='\t'))
ndr_linked = DataFrame(CSV.File(joinpath(outdir, "output", "notifiable_disease_reports_linked.tsv"); delim='\t'))
@test size(ep_linked, 1)  == size(emergencies, 1)  # All records linked because the spine was constructed from the emergencies table
@test size(ha_linked, 1)  == 3  # 3 of 5 admissions, 2 of 4 people admitted also had emergency presentations
@test size(ndr_linked, 1) == 4  # 4 of 8 reports, 2 of 5 people with disease reports also had emergency presentations

# Reporting

#=
# Clean up
for x in readdir("output")
    rm(x, recursive=true)
end
=#