module SpineBasedRecordLinkage

export construct_spine, run_linkage,  # Core functions
       compare_linkage_runs           # Convemience functions

# Non-exported functions
include("distances.jl")           # Independent
include("utils/TableIndexes.jl")  # Independent
include("config.jl")              # Depends on distances
include("utils/utils.jl")         # Depends on config, TableIndexes

# Core functions
include("constructspine.jl")
include("runlinkage.jl")

# Convenience functions
include("compare_linkage_runs.jl")

using .distances
using .TableIndexes
using .config
using .utils

using .constructspine
using .runlinkage

using .comparelinkageruns

end
