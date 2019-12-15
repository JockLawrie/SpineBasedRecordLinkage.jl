#=
  Test set 1: Construct a spine from 1 table and link where possible.
=#

# Construct spine
outdir1a = run_linkage(joinpath("config", "constructspine_emergencies.yml"))
spine    = DataFrame(CSV.File(joinpath(outdir1a, "output", "spine.tsv"); delim='\t'))
@test size(spine, 1) == 3

# Linkage
cp(joinpath(outdir1a, "output", "spine.tsv"), joinpath(outdir, "spine.tsv"); force=true)
outdir1b = run_linkage(joinpath("config", "link_to_emergencies.yml"))
ep       = DataFrame(CSV.File(joinpath(outdir1b, "output", "emergency_presentations_linked.tsv"); delim='\t'))
ha       = DataFrame(CSV.File(joinpath(outdir1b, "output", "hospital_admissions_linked.tsv"); delim='\t'))
ndr      = DataFrame(CSV.File(joinpath(outdir1b, "output", "notifiable_disease_reports_linked.tsv"); delim='\t'))

ep_linked  = view(ep,  .!ismissing.(ep[!,  :spineID]), :)
ha_linked  = view(ha,  .!ismissing.(ha[!,  :spineID]), :)
ndr_linked = view(ndr, .!ismissing.(ndr[!, :spineID]), :)

@test size(ep_linked, 1) == size(ep, 1)  # All records linked because the spine was constructed from the emergencies table
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
outfile = joinpath(outdir, "linkage_report.csv")
summarise_linkage_run(outdir1b, outfile)
report = DataFrame(CSV.File(outfile))
result_set = Set{NamedTuple{(:tablename, :status, :nrecords), Tuple{String, String, Union{Missing, Int}}}}()
for r in eachrow(report)
    push!(result_set, (tablename=r[:tablename], status=r[:status], nrecords=r[:nrecords]))
end

@test size(report, 1) == 11
@test in((tablename="LINKAGE RUNS",               status=outdir1b,                     nrecords=missing), result_set)
@test in((tablename="spine",                      status="existent",                  nrecords=3), result_set)
@test in((tablename="emergency_presentations",    status="linked with criteria ID 1", nrecords=4), result_set)
@test in((tablename="emergency_presentations",    status="linked with criteria ID 2", nrecords=1), result_set)
@test in((tablename="hospital_admissions",        status="linked with criteria ID 3", nrecords=3), result_set)
@test in((tablename="hospital_admissions",        status="unlinked",                  nrecords=2), result_set)
@test in((tablename="notifiable_disease_reports", status="linked with criteria ID 4", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status="linked with criteria ID 5", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status="linked with criteria ID 6", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status="linked with criteria ID 7", nrecords=1), result_set)
@test in((tablename="notifiable_disease_reports", status="unlinked",                  nrecords=4), result_set)
