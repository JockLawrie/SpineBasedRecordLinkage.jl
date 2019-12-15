module SpineBasedRecordLinkage

export run_linkage, summarise_linkage_run, compare_linkage_runs

#=
TODO:
3. testset5 (to be named testset3) compares the 2 linkage runs.

Issue: Merging a row into the spine may alter the primary key and therefore the spineid.
       Update the spineid and all links involving this row.
       This means doing everything in memory and maintaining a lookup: spineid => [rowindices...]
=#

# Functions that aren't exported
include("unexported/distances.jl")     # Independent
include("unexported/TableIndexes.jl")  # Independent
include("unexported/config.jl")        # Depends on distances

using .distances
using .TableIndexes
using .config

# Exported functions
include("runlinkage.jl")
include("reporting.jl")

using .runlinkage
using .reporting

end
