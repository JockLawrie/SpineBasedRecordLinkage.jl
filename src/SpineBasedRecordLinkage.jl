module SpineBasedRecordLinkage

export run_linkage, summarise_linkage_run, compare_linkage_runs

include("unexported/distances.jl")     # Independent
include("unexported/TableIndexes.jl")  # Independent
include("unexported/config.jl")        # Depends on distances
include("runlinkage.jl")               # Exports: run_linkage
include("reporting.jl")                # Exports: summarise_linkage_run, compare_linkage_runs

using .distances
using .TableIndexes
using .config
using .runlinkage
using .reporting

end
