#=
  Test set 2: Construct a spine from all 3 tables and link all records.
=#

println("\nTEST SET 2")

# Linkage
outdir2 = run_linkage(joinpath("config", "link_all_tables.yml"))

spine = DataFrame(CSV.File(joinpath(outdir2, "output", "spine.tsv"); delim='\t'))
ha    = DataFrame(CSV.File(joinpath(outdir2, "output", "hospital_admissions_linked.tsv"); delim='\t'))
ep    = DataFrame(CSV.File(joinpath(outdir2, "output", "emergency_presentations_linked.tsv"); delim='\t'))
ndr   = DataFrame(CSV.File(joinpath(outdir2, "output", "notifiable_disease_reports_linked.tsv"); delim='\t'))

ha_linked  = view(ha,  .!ismissing.(ha[!,  :spineID]), :)
ep_linked  = view(ep,  .!ismissing.(ep[!,  :spineID]), :)
ndr_linked = view(ndr, .!ismissing.(ndr[!, :spineID]), :)

@test size(spine, 1) == 6

@test size(ha_linked, 1) == size(ha, 1)  # All records were linked
@test size(view(ha_linked, ha_linked[!, :criteriaID] .== 1, :), 1) == 5    # 5 of 5 links made with criteria 1

@test size(ep_linked, 1) == size(ep, 1)  # All records were linked
@test size(view(ep_linked, ep_linked[!, :criteriaID] .== 2, :), 1) == 4    # 4 of 5 links made with criteria 2
@test size(view(ep_linked, ep_linked[!, :criteriaID] .== 3, :), 1) == 1    # 1 of 5 links made with criteria 3

@test size(ndr_linked, 1) == size(ndr, 1)  # All records were linked
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 4, :), 1) == 3  # 3 of 8 links made with criteria 4
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 5, :), 1) == 2  # 2 of 8 links made with criteria 5
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 6, :), 1) == 1  # 1 of 8 links made with criteria 6
@test size(view(ndr_linked, ndr_linked[!, :criteriaID] .== 7, :), 1) == 2  # 2 of 8 links made with criteria 7


# Reporting
outfile = joinpath(outdir, "linkage_report.csv")
summarise_linkage_run(outdir2, outfile)
result  = table_to_set_of_dicts(outfile)

@test length(result) == 9
@test in(Dict(:tablename => "LINKAGE RUNS",               :status => outdir2,                     :nrecords => missing), result)
@test in(Dict(:tablename => "spine",                      :status => "existent",                  :nrecords => 6), result)
@test in(Dict(:tablename => "hospital_admissions",        :status => "linked with criteria ID 1", :nrecords => 5), result)
@test in(Dict(:tablename => "emergency_presentations",    :status => "linked with criteria ID 2", :nrecords => 4), result)
@test in(Dict(:tablename => "emergency_presentations",    :status => "linked with criteria ID 3", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status => "linked with criteria ID 4", :nrecords => 3), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status => "linked with criteria ID 5", :nrecords => 2), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status => "linked with criteria ID 6", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status => "linked with criteria ID 7", :nrecords => 2), result)
