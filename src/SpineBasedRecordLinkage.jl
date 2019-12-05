module SpineBasedRecordLinkage

export construct_spine, run_linkage,                # Core functions
       stack_tables, combine_schemata, combine_spine_construction_configs,  # Combining functions
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

# Combining functions
include("stacktables.jl")
include("combineschemata.jl")
include("combine_linkage_configurations.jl")

using .stacktables
using .combineschemata
using .combine_linkage_configurations

# Core functions
include("constructspine.jl")
include("runlinkage.jl")

using .constructspine
using .runlinkage

# Reporting functions
include("reporting.jl")

using .reporting

end
