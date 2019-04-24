module RecordLinkage

include("config.jl")
include("persontable.jl")
include("linkmap.jl")
include("run.jl")

using .config
using .persontable
using .linkmap
using .run

end
