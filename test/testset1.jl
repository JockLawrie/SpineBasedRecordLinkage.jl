#=
  Test set 1: Construct a spine from 1 table (influenza cases) and link events where possible.
=#

println("\nTEST SET 1")

println("Construct spine from influenza cases")
outdir1a = run_linkage(joinpath("config", "construct_spine_from_influenza_cases.yml"))
spine    = DataFrame(CSV.File(joinpath(outdir1a, "output", "spine.tsv"); delim='\t'))

@test size(spine, 1) == 4

println("Link influenza cases to events")
cp(joinpath(outdir1a, "output", "spine.tsv"), joinpath(outdir, "spine.tsv"); force=true)
outdir1b = run_linkage(joinpath("config", "link_influenza_cases_to_events.yml"))
ep       = DataFrame(CSV.File(joinpath(outdir1b, "output", "emergency_presentations_linked.tsv"); delim='\t'))
ha       = DataFrame(CSV.File(joinpath(outdir1b, "output", "hospital_admissions_linked.tsv"); delim='\t'))
ic       = DataFrame(CSV.File(joinpath(outdir1b, "output", "influenza_cases_linked.tsv"); delim='\t'))

ep_linked = view(ep, .!ismissing.(ep[!, :spineID]), :)
ha_linked = view(ha, .!ismissing.(ha[!, :spineID]), :)
ic_linked = view(ic, .!ismissing.(ic[!, :spineID]), :)

@test size(ep_linked, 1) == 3  # 3 of 5 emergency presentations were also influenza cases
@test size(view(ep_linked, ep_linked[!, :criteriaID] .== 1, :), 1) == 2  # 1 of 3 links made with criteria 1
@test size(view(ep_linked, ep_linked[!, :criteriaID] .== 2, :), 1) == 1  # 1 of 3 links made with criteria 2

@test size(ha_linked, 1) == 3  # 3 of 5 admissions were also influenza cases
@test size(view(ha_linked, ha_linked[!, :criteriaID] .== 3, :), 1) == 3  # 3 of 3 links made with criteria 3

@test size(ic_linked, 1) == size(ic, 1)  # All records linked because the spine was constructed from the influenza cases table
@test size(view(ic_linked, ic_linked[!, :criteriaID] .== 4, :), 1) == 4  # 4 of 8 links made with criteria 4
@test size(view(ic_linked, ic_linked[!, :criteriaID] .== 5, :), 1) == 2  # 2 of 8 links made with criteria 5
@test size(view(ic_linked, ic_linked[!, :criteriaID] .== 6, :), 1) == 1  # 1 of 8 links made with criteria 6
@test size(view(ic_linked, ic_linked[!, :criteriaID] .== 7, :), 1) == 1  # 1 of 8 links made with criteria 7

println("Reporting")
outfile = joinpath(outdir, "linkage_report.csv")
summarise_linkage_run(outdir1b, outfile)
result  = table_to_set_of_dicts(outfile)

@test length(result) == 11
@test in(Dict(:tablename => "LINKAGE RUNS",            :status => outdir1b,                    :nrecords => missing), result)
@test in(Dict(:tablename => "spine",                   :status => "existent",                  :nrecords => 4), result)
@test in(Dict(:tablename => "emergency_presentations", :status => "linked with criteria ID 1", :nrecords => 2), result)
@test in(Dict(:tablename => "emergency_presentations", :status => "linked with criteria ID 2", :nrecords => 1), result)
@test in(Dict(:tablename => "emergency_presentations", :status => "unlinked",                  :nrecords => 2), result)
@test in(Dict(:tablename => "hospital_admissions",     :status => "linked with criteria ID 3", :nrecords => 3), result)
@test in(Dict(:tablename => "hospital_admissions",     :status => "unlinked",                  :nrecords => 2), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 4", :nrecords => 4), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 5", :nrecords => 2), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 6", :nrecords => 1), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 7", :nrecords => 1), result)
