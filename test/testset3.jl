#=
  Test set 3: Compare the results from test sets 1 and 2.
=#

println("\nTEST SET 3")

outfile = joinpath(outdir, "linkage_comparison.csv")
compare_linkage_runs(outdir1b, outdir2, outfile)
result  = table_to_set_of_dicts(outfile)

@test length(result) == 14
@test in(Dict(:tablename => "LINKAGE RUNS",               :status1 => outdir1b,                    :status2 => outdir2,                     :nrecords => missing), result)
@test in(Dict(:tablename => "spine",                      :status1 => "existent",                  :status2 => "nonexistent",               :nrecords => 3), result)
@test in(Dict(:tablename => "spine",                      :status1 => "nonexistent",               :status2 => "existent",                  :nrecords => 6), result)
@test in(Dict(:tablename => "hospital_admissions",        :status1 => "linked with criteria ID 3", :status2 => "linked with criteria ID 1", :nrecords => 3), result)
@test in(Dict(:tablename => "hospital_admissions",        :status1 => "unlinked",                  :status2 => "linked with criteria ID 1", :nrecords => 2), result)
@test in(Dict(:tablename => "emergency_presentations",    :status1 => "linked with criteria ID 1", :status2 => "linked with criteria ID 2", :nrecords => 4), result)
@test in(Dict(:tablename => "emergency_presentations",    :status1 => "linked with criteria ID 2", :status2 => "linked with criteria ID 3", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status1 => "linked with criteria ID 4", :status2 => "linked with criteria ID 4", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status1 => "linked with criteria ID 5", :status2 => "linked with criteria ID 5", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status1 => "linked with criteria ID 6", :status2 => "linked with criteria ID 6", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status1 => "linked with criteria ID 7", :status2 => "linked with criteria ID 7", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status1 => "unlinked",                  :status2 => "linked with criteria ID 4", :nrecords => 2), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status1 => "unlinked",                  :status2 => "linked with criteria ID 5", :nrecords => 1), result)
@test in(Dict(:tablename => "notifiable_disease_reports", :status1 => "unlinked",                  :status2 => "linked with criteria ID 7", :nrecords => 1), result)