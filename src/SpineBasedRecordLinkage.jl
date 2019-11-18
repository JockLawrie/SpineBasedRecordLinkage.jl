module SpineBasedRecordLinkage

export construct_spine, run_linkage

include("distances.jl")           # Independent
include("utils/TableIndexes.jl")  # Independent
include("config.jl")              # Depends on distances
include("utils/utils.jl")         # Depends on config, TableIndexes
include("constructspine.jl")
include("runlinkage.jl")

using .distances
using .TableIndexes
using .config
using .utils
using .constructspine
using .runlinkage

end
