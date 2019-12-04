module SpineBasedRecordLinkage

export construct_spine, run_linkage,                            # Core functions
       combine_schemata, combine_configurations, stack_tables,  # Combining functions
       summarise_linkage_run, compare_linkage_runs              # Reporting functions

# Functions that aren't exported
include("unexported/distances.jl")     # Independent
include("unexported/TableIndexes.jl")  # Independent
include("unexported/config.jl")        # Depends on distances
include("unexported/utils.jl")         # Depends on config, TableIndexes

using .distances
using .TableIndexes
using .config
using .utils

# Combining functions
include("combineschemata.jl")
#include("combineconfigurations.jl")
include("stacktables.jl")

using .combineschemata
#using .combineconfigurations
using .stacktables

# Core functions
include("constructspine.jl")
include("runlinkage.jl")

using .constructspine
using .runlinkage

# Reporting functions
include("reporting.jl")

using .reporting

end
