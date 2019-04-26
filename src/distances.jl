module distances

export compute_distance, overalldistance

using StringDistances  # Scales distance to [0, 1], with 1 being equality

const metrics = Dict(:levenshtein => Levenshtein)


overalldistance(d::Vector{Float64}) = sum(d)

function compute_distance(metric::Symbol, val1, val2)
    ismissing(val1) && return 1.0
    ismissing(val2) && return 1.0
    1.0 - compare(metrics[metric](), val1, val2)
end


end
