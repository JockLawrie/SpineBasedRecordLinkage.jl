module SpineBasedRecordLinkage

export run_linkage, summarise_linkage_run, compare_linkage_runs
#export construct_spine, run_linkage,                # Core functions
#       stack_tables, combine_schemata,              # Combining functions
#       combine_spine_construction_configs, combine_linkage_configs,  # More combining functions
#       summarise_linkage_run, compare_linkage_runs  # Reporting functions

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
#include("combine/stacktables.jl")
#include("combine/combineschemata.jl")
#include("combine/combine_spine_construction_configurations.jl")
#include("combine/combine_linkage_configurations.jl")

#using .stacktables
#using .combineschemata
#using .combine_spine_construction_configurations
#using .combine_linkage_configurations

# Core functions
include("runlinkage2.jl")
#include("constructspine.jl")

using .runlinkage
#using .constructspine

# Reporting functions
include("reporting.jl")

using .reporting

end
