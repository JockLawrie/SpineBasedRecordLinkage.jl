module RecordLinkage

export run_linkage

include("distances.jl")
include("config.jl")
include("utils.jl")
include("link.jl")
include("run.jl")

using .distances
using .config
using .utils
using .link
using .runlinkage

end