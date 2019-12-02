#=
  Test set 2.

  1. Construct a spine from each of the 3 tables.
  2. Stack into a single table.
  3. Construct a spine from the stacked table.
  4. Link the 3 tables to the spine.
=#

################################################################################
# Construct a spine from each of the 3 tables
outdir1 = construct_spine(joinpath("config", "constructspine_emergencies.yml"))
spine1  = DataFrame(CSV.File(joinpath(outdir1, "output", "spine.tsv"); delim='\t'))
@test size(spine1, 1) == 3

outdir2 = construct_spine(joinpath("config", "constructspine_admissions.yml"))
spine2  = DataFrame(CSV.File(joinpath(outdir2, "output", "spine.tsv"); delim='\t'))
@test size(spine2, 1) == 4

outdir3 = construct_spine(joinpath("config", "constructspine_diseases.yml"))
spine3  = DataFrame(CSV.File(joinpath(outdir3, "output", "spine.tsv"); delim='\t'))
@test size(spine3, 1) == 4

################################################################################
# Linkage
#=
cp(joinpath(outdir, "output", "spine.tsv"), joinpath("output", "spine.tsv"); force=true)
outdir = run_linkage(joinpath("config", "linkagerun1.yml"))
ep     = DataFrame(CSV.File(joinpath(outdir, "output", "emergency_presentations_linked.tsv"); delim='\t'))
ha     = DataFrame(CSV.File(joinpath(outdir, "output", "hospital_admissions_linked.tsv"); delim='\t'))
ndr    = DataFrame(CSV.File(joinpath(outdir, "output", "notifiable_disease_reports_linked.tsv"); delim='\t'))

ep_linked  = view(ep,  .!ismissing.(ep[!,  :spineID]), :)
ha_linked  = view(ha,  .!ismissing.(ha[!,  :spineID]), :)
ndr_linked = view(ndr, .!ismissing.(ndr[!, :spineID]), :)

@test size(ep_linked, 1) == size(emergencies, 1)  # All records linked because the spine was constructed from the emergencies table
@test size(view(ep_linked, ep_linked[!, :criteriaID] .== 1, :), 1) == 4    # 4 of 5 links made with criteria 1
@test size(view(ep_linked, ep_linked[!, :criteriaID] .== 2, :), 1) == 1    # 1 of 5 links made with criteria 2

@test size(ha_linked, 1) == 3   # 3 of 5 admissions, 2 of 4 people admitted also had emergency presentations
@test size(view(ha_linked, ha_linked[!, :criteriaID] .== 3, :), 1) == 3    # 3 of 3 links made with criteria 3

@test size(ndr_linked, 1) == 4  # 4 of 8 reports, 2 of 5 people with disease reports also had emergency presentations
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 4, :), 1) == 1  # 1 of 4 links made with criteria 4
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 5, :), 1) == 1  # 1 of 4 links made with criteria 5
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 6, :), 1) == 1  # 1 of 4 links made with criteria 6
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 7, :), 1) == 1  # 1 of 4 links made with criteria 7

# Reporting
outfile = joinpath("output", "linkage_report.csv")
summarise_linkage_run(outdir, outfile)
report = DataFrame(CSV.File(outfile))
result_set = Set{NamedTuple{(:tablename, :status1, :nrecords), Tuple{String, String, Union{Missing, Int}}}}()
for r in eachrow(report)
    push!(result_set, (tablename=r[:tablename], status1=r[:status1], nrecords=r[:nrecords]))
end

@test size(report, 1) == 11
@test in((tablename="LINKAGE RUNS",               status1=outdir,                      nrecords=missing), result_set)
@test in((tablename="spine",                      status1="existent",                  nrecords=3), result_set)
@test in((tablename="emergency_presentations",    status1="linked with criteria ID 1", nrecords=4), result_set)
@test in((tablename="emergency_presentations",    status1="linked with criteria ID 2", nrecords=1), result_set)
@test in((tablename="hospital_admissions",        status1="linked with criteria ID 3", nrecords=3), result_set)
@test in((tablename="hospital_admissions",        status1="unlinked",                  nrecords=2), result_set)
@test in((tablename="notifiable_disease_reports", status1="linked with criteria ID 4", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status1="linked with criteria ID 5", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status1="linked with criteria ID 6", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status1="linked with criteria ID 7", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status1="unlinked",                  nrecords=4), result_set)

# Clean up
contents = readdir("output")
for x in contents
    rm(joinpath(pwd(), "output", x); recursive=true)
end
=#
