#=
  Test set 2: Construct a spine from all 3 tables and link all records.
=#

println("\nTEST SET 2")

println("Link all health service events")
outdir2 = run_linkage(joinpath("config", "link_all_health_service_events.toml"))

println("Reporting")
outfile = joinpath(outdir, "linkage_report.csv")
summarise_linkage_run(outdir2, outfile)
result  = table_to_set_of_dicts(outfile)

@test length(result) == 9
@test in(Dict(:tablename => "LINKAGE RUNS",            :status => outdir2,                     :nrecords => missing), result)
@test in(Dict(:tablename => "spine",                   :status => "existent",                  :nrecords => 6), result)
@test in(Dict(:tablename => "hospital_admissions",     :status => "linked with criteria ID 1", :nrecords => 5), result)
@test in(Dict(:tablename => "emergency_presentations", :status => "linked with criteria ID 2", :nrecords => 4), result)
@test in(Dict(:tablename => "emergency_presentations", :status => "linked with criteria ID 3", :nrecords => 1), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 4", :nrecords => 3), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 5", :nrecords => 2), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 6", :nrecords => 1), result)
@test in(Dict(:tablename => "influenza_cases",         :status => "linked with criteria ID 7", :nrecords => 2), result)
