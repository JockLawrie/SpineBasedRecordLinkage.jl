#=
  Test set 3.

  1. Construct a spine from each of the 3 tables.
  2. Stack the 3 spines into a single table with the columns being the union of the sets of columns.
  3. Construct a schema for the stacked table by combining the schemata of the 3 tables.
  4. Construct a construct_spine config for the stacked table by combining the construct_spine configs of the 3 tables.
  5. Construct a spine from the stacked table.
  6. Link the 3 tables to the spine.
=#


# Stack tables using the union of the columns
outfile2 = joinpath(pwd(),   "output", "stackedtable_union.tsv")
stack_tables(outfile2, infile1, infile2, infile3; replace_outfile=true, columns=:union)
stacked = DataFrame(CSV.File(outfile2; delim='\t'))
nms = [:spineID, :firstname, :middlename, :lastname, :birthdate, :patient_postcode,
       :hospitalid, :campusid, :patientid, :admissiondate, :dischargedate, :presentationdate,
       :reportid, :reportdate]

@test Set(names(stacked)) == Set(nms)
@test size(stacked) == (11, 14)

