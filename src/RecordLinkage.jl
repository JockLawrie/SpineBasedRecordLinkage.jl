module RecordLinkage

include("distances.jl")
include("config.jl")
include("persontable.jl")
include("linkmap.jl")
include("run.jl")

using .distances
using .config
using .persontable
using .linkmap
using .run

end
