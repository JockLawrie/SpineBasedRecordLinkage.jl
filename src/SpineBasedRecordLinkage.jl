module SpineBasedRecordLinkage

export run_linkage, summarise_linkage_run, compare_linkage_runs

#=
TODO:
1. Minimise code, exports and dependencies. Revert dependencies to their release versions.
2. rename testset3 to testset2, and remove old testset2.
3. Test a linkage with 2 tables and compare the linkage runs from the 2 test sets.

Issue: Merging a row into the spine may alter the primary key and therefore the spineid.
       Update the spineid and all links involving this row.
       This means doing everything in memory and maintaining a lookup: spineid => [rowindices...]
=#

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

# Exported functions
include("runlinkage2.jl")
include("reporting.jl")
#include("constructspine.jl")

using .runlinkage
using .reporting
#using .constructspine

end
