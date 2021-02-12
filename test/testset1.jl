#=
  Test set 1: Construct a spine from 1 table (influenza cases) and link events where possible.
=#

println("\nTEST SET 1")

println("Write LinkageConfig")
lc1 = SpineBasedRecordLinkage.config.LinkageConfig(joinpath("config", "construct_spine_from_influenza_cases.toml"))
SpineBasedRecordLinkage.config.write_config(joinpath("output", "test_linkage_config.toml"), lc1)

println("Construct spine from influenza cases")
outdir1a = run_linkage(joinpath("config", "construct_spine_from_influenza_cases.toml"))
spine    = DataFrame(CSV.File(joinpath(outdir1a, "output", "spine.tsv"); delim='\t'))

@test size(spine, 1) == 4

println("Link influenza cases to events")
cp(joinpath(outdir1a, "output", "spine.tsv"), joinpath(outdir, "spine.tsv"); force=true)
outdir1b = run_linkage(joinpath("config", "link_influenza_cases_to_events.toml"))

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