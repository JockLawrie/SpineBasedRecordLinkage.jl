module SpineBasedRecordLinkage

export construct_spine, stack_tables, run_linkage,  # Core functions
       summarise_linkage_run, compare_linkage_runs  # Reporting functions

# Functions that aren't exported
include("unexported/distances.jl")     # Independent
include("unexported/TableIndexes.jl")  # Independent
include("unexported/config.jl")        # Depends on distances
include("unexported/utils.jl")         # Depends on config, TableIndexes

using .distances
using .TableIndexes
using .config
using .utils

# Core functions
include("constructspine.jl")
include("stacktables.jl")
include("runlinkage.jl")

using .constructspine
using .stacktables
using .runlinkage

# Reporting functions
include("reporting.jl")

using .reporting

end
