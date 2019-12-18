module distances

export distance

using StringDistances

const metrics = Dict(:jaro => Jaro, :dameraulevenshtein => DamerauLevenshtein, :levenshtein => Levenshtein, :ratcliffobershelp => RatcliffObershelp)

"""
Returns: Distance between val1 and val2 as measured by the metric.

Distances are scaled to be in [0, 1], with 0 indicating that the values are equal and 1 indicating that the values are totally different.
"""
function distance(metric::Symbol, val1, val2)
    ismissing(val1) && return 1.0
    ismissing(val2) && return 1.0
    1.0 - compare(val1, val2, metrics[metric]())
end

end
