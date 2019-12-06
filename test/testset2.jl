#=
  Test set 2.

  1. Construct a spine from each of the 3 tables.
  2. Stack the 3 spines into a single table with the columns being the intersection of the sets of columns.
  3. Construct a schema for the stacked table by combining the schemata of the 3 tables.
  4. Construct a construct_spine config for the stacked table by combining the construct_spine configs of the 3 tables.
  5. Construct a spine from the stacked table.
  6. Construct a linkage config file from the inidividual spine construction config files.
  7. Link the 3 tables to the spine.
=#

################################################################################
# Step 1: Construct a spine from each of the 3 tables
outdir1 = construct_spine(joinpath("config", "constructspine_emergencies.yml"))
spine   = DataFrame(CSV.File(joinpath(outdir1, "output", "spine.tsv"); delim='\t'))
@test size(spine, 1) == 3

outdir2 = construct_spine(joinpath("config", "constructspine_admissions.yml"))
spine   = DataFrame(CSV.File(joinpath(outdir2, "output", "spine.tsv"); delim='\t'))
@test size(spine, 1) == 4

outdir3 = construct_spine(joinpath("config", "constructspine_diseases.yml"))
spine   = DataFrame(CSV.File(joinpath(outdir3, "output", "spine.tsv"); delim='\t'))
@test size(spine, 1) == 4

######################
#=
Link admissions to the spine constructed from constructspine_admissions.yml.

TODO:
This linkage results in all admissions being linked to the spine constructed from constructspine_admissions.yml, which is good.
Yet the final linkage below results in 1 admission not being linked to the final spine.
Investigate how this happens.

Notes:
1. We have a method for collapsing a table to a spine and linking all records in the table to the spine.
2. We want a method for combining spines and still being able to link all records.
3. Should be able to use the method in Note 1 applied to the tables, then stack the resulting spines and apply the method to the stacked spines.
   - We have a 2-level hierarchy of linkages, namely tables -> small spines and small spines -> big spine.
=#
cfg         = SpineBasedRecordLinkage.LinkageConfig(joinpath("config", "constructspine_admissions.yml"))
spineconfig = SpineBasedRecordLinkage.config.TableConfig(joinpath(outdir2, "output", "spine.tsv"), cfg.spine.schema)
newconfig   = SpineBasedRecordLinkage.LinkageConfig(cfg.projectname, cfg.output_directory, spineconfig, cfg.tables, cfg.criteria)
outdir2 = run_linkage(newconfig, joinpath("config", "constructspine_admissions.yml"))
@test 1 == 2  # Deliberate fail
######################

################################################################################
# Step 2: Stack the 3 spines using the intersection of the columns

spine_datafile = joinpath(outdir, "stackedtable_intersection.tsv")  # Store result here
infile1 = joinpath(outdir1, "output", "spine.tsv")
infile2 = joinpath(outdir2, "output", "spine.tsv")
infile3 = joinpath(outdir3, "output", "spine.tsv")
stack_tables(spine_datafile, infile1, infile2, infile3; replace_outfile=true, columns=:intersection)
stacked = DataFrame(CSV.File(spine_datafile; delim='\t'))

@test Set(names(stacked)) == Set([:spineID, :firstname, :middlename, :lastname, :birthdate])
@test size(stacked) == (11, 5)

################################################################################
# Step 3. Construct a schema for the stacked table by combining the schemata of the 3 tables.

spine_schemafile = joinpath(outdir, "combined_spine_schema.yml")  # Store result here
infile1 = joinpath(pwd(), "schema", "emergency_presentations.yml")
infile2 = joinpath(pwd(), "schema", "hospital_admissions.yml")
infile3 = joinpath(pwd(), "schema", "notifiable_disease_reports.yml")
combine_schemata(spine_schemafile, infile1, infile2, infile3; replace_outfile=true, columns=:intersection)

################################################################################
# Step 4: Construct a construct_spine config for the stacked table by combining the construct_spine configs of the 3 tables.

projectname = "combined"
spine_linkagefile = joinpath(outdir, "combined_constructspine.yml")  # Store result here
infile1 = joinpath("config", "constructspine_emergencies.yml")
infile2 = joinpath("config", "constructspine_admissions.yml")
infile3 = joinpath("config", "constructspine_diseases.yml")
combine_spine_construction_configs(projectname, outdir, spine_datafile, spine_schemafile, spine_linkagefile,
                                   infile1, infile2, infile3; replace_outfile=true)

################################################################################
# Step 5: Construct a spine from the stacked table (intersection of columns).

outdir1 = construct_spine(spine_linkagefile)
spine   = DataFrame(CSV.File(joinpath(outdir1, "output", "spine.tsv"); delim='\t'))
@test size(spine, 1) == 6  # 6 people across 18 events

################################################################################
# Step 6: Construct a linkage config file from the inidividual spine construction config files.

projectname    = "combined"
outfile        = joinpath(outdir, "combined_linkage.yml")  # Store result here
spine_datafile = joinpath(outdir1, "output", "spine.tsv")
infile1 = joinpath("config", "constructspine_emergencies.yml")
infile2 = joinpath("config", "constructspine_admissions.yml")
infile3 = joinpath("config", "constructspine_diseases.yml")
combine_linkage_configs(projectname, outdir, spine_datafile, spine_schemafile, outfile,
                        infile1, infile2, infile3; replace_outfile=true)

################################################################################
# Linkage

outdir1 = run_linkage(joinpath(outdir, outfile))
spine   = DataFrame(CSV.File(joinpath(outdir1, "output", "spine_primarykey_and_spineid.tsv"); delim='\t'))
ep      = DataFrame(CSV.File(joinpath(outdir1, "output", "emergency_presentations_linked.tsv"); delim='\t'))
ha      = DataFrame(CSV.File(joinpath(outdir1, "output", "hospital_admissions_linked.tsv"); delim='\t'))
ndr     = DataFrame(CSV.File(joinpath(outdir1, "output", "notifiable_disease_reports_linked.tsv"); delim='\t'))

@test size(spine, 1) == 6
@test size(view(ep, .!ismissing.(ep[!, :spineID]), :), 1) == 5
@test size(view(ha, .!ismissing.(ha[!, :spineID]), :), 1) == 5
@test size(view(ndr, .!ismissing.(ndr[!, :spineID]), :), 1) == 8

# Reporting
outfile = joinpath(outdir, "linkage_report.csv")
summarise_linkage_run(outdir1, outfile)
#cleanup()