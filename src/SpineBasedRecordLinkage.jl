module SpineBasedRecordLinkage

export construct_spine, run_linkage

include("TableIndexes.jl")
include("distances.jl")
include("config.jl")
include("utils.jl")
include("link.jl")
include("constructspine.jl")
include("runlinkage.jl")

using .TableIndexes
using .distances
using .config
using .utils
using .link
using .constructspine
using .runlinkage

end
