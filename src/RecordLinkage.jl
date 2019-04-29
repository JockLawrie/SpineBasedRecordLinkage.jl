module RecordLinkage

export run_linkage_pipeline

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
