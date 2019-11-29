using Test
using SpineBasedRecordLinkage

# pwd is the test directory
construct_spine(joinpath("config", "construct_spine.yml"))

# Clean up
#=
contents = readdir()
for x in contents
    !isdir(x) && continue
    if x[1:17] == "spineconstruction"
        rm(x, recursive=true)
    elseif x[1:7] == "linkage"
        rm(x, recursive=true)
    end
end
=#