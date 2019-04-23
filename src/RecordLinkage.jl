module RecordLinkage


include("persontable.jl")
include("linkmap.jl")
include("config.jl")
include("run.jl")

using .persontable
using .linkmap
using .config
using .run

end
