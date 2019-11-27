module SpineBasedRecordLinkage

export construct_spine, stack_tables, run_linkage,  # Core functions
       compare_linkage_runs                         # Reporting functions

# Non-exported functions
include("distances.jl")           # Independent
include("utils/TableIndexes.jl")  # Independent
include("config.jl")              # Depends on distances
include("utils/utils.jl")         # Depends on config, TableIndexes

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
include("compare_linkage_runs.jl")

using .comparelinkageruns

end
