#=
  Test set 3: Compare the results from test sets 1 and 2.
=#

outfile = joinpath(outdir, "linkage_comparison.csv")
compare_linkage_runs(outdir1b, outdir2, outfile)
result  = table_to_set_of_dicts(outfile)
for x in result
    println(x)
end