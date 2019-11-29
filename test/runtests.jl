using Test
using SpineBasedRecordLinkage

# pwd is the test directory
construct_spine(joinpath("config", "construct_spine.yml"))

# Clean up
contents = readdir()
for x in contents
    !isdir(x) && continue
    if length(x) > 17 && x[1:17] == "spineconstruction"
        rm(x, recursive=true)
    elseif length(x) > 7 && x[1:7] == "linkage"
        rm(x, recursive=true)
    end
end