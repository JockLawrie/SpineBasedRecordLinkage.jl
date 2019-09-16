module RecordLinkage

export run_linkage

include("utils.jl")
include("distances.jl")
include("config.jl")
#include("persontable.jl")
#include("linkmap.jl")
include("run.jl")

using .utils
using .distances
using .config
#using .persontable
#using .linkmap
using .run

end
