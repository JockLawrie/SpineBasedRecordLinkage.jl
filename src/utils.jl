module utils

export get_n_unique

using DataFrames

function get_n_unique(data)
    K      = NamedTuple{(:ntotal, :nmissing, :nuniq), Tuple{Int, Int, Int}}
    result = Dict{Symbol, K}()
    ntotal = size(data, 1)
    for colname in names(data)
        col             = data[colname]
        nmissing        = sum(ismissing.(col))
        nunique         = nmissing > 0 ? length(unique(col)) - 1 : length(unique(col))
        result[colname] = (ntotal=ntotal, nmissing=nmissing, nuniq=nunique)
    end
    result
end


"""
Returns: DataFrame containing the rows uniquely defined by colnames.
"""
function uniquerows(data, colnames::Vector{Symbol}, primarykey::Vector{Symbol}, allowmissing=false)
    size(unique(data[:, primarykey]), 1) != size(data, 1) && error("Invalid primary key: $(primarykey)")
    cnames = vcat(primarykey, colnames)
    d = allowmissing ? unique(data[:, cnames]) : unique(dropmissing(data[:, cnames]))
end

uniquerows(data, colnames, primarykey::Symbol,allowmissing=false) = uniquerows(data, colnames, [primarykey],allowmissing)


function iscomplete(d)
    n = length(d)
    for i = 1:n
        ismissing(d[i]) && return false
    end
    true
end


function createindex!(data, indexname::Symbol)
    haskey(data, indexname) && error("Cannot create index. Table already has a column called $(indexname).")
    data[indexname] = collect(1:size(data, 1))
end


#function creategroupindex(tbl1, tbl2, colname_pairs::Vector{Pair{Symbol, Symbol}})
"""
Appends a group index to data and returns a Dict(groupid => groupvalue,...)
"""
function creategroupindex!(data, indexname::Symbol, colnames::Vector{Symbol})
    haskey(data, indexname) && error("Cannot create group index. Table already has a column called $(indexname).")
    result = Dict{Tuple, Int}()
    data[indexname] = missings(Int, size(data, 1))
    idx = 0
    for subdata in groupby(data, colnames)
        k = Tuple(subdata[1, colnames])
        !iscomplete(k) && continue
        idx += 1
        subdata[indexname] .= idx
        result[k] = idx
    end
    result
end


function linkgroupindex!(data, indexname::Symbol, colnames::Vector{Symbol}, p)
    haskey(data, indexname) && error("Cannot link group index. Table already has a column called $(indexname).")
    data[indexname] = missings(Int, size(data, 1))
    for subdata in groupby(data, colnames)
        k = Tuple(subdata[1, colnames])
        !haskey(p, k) && continue
        idx = p[k]
        subdata[indexname] .= idx
    end
end


function compute_distances(data1, data2, comparisons, index1, index2, groupindex1, groupindex2)
    nd     = length(comparisons)
    result = Dict{Tuple{Int, Int}, Vector{Float64}}()  # (index1, index2) => [d1, d2, ...]
    for subdata1 in groupby(data1, groupindex1)
        idx      = subdata1[1, groupindex1]
        subdata2 = view(data2, data2[groupindex2] .== idx, :)
        n1       = size(subdata1, 1)
        n2       = size(subdata2, 1)
        for i = 1:n1
            k1 = subdata1[i, index1]
            for j = 1:n2
                k = (k1, subdata2[j, index2])
                result[k] = compare_2_rows(subdata1, i, subdata2, j, comparisons)
            end
        end
    end
    result
end


function compare_2_rows(data1, i1, data2, i2, comparisons)
    result = Float64[]
    for cmp in comparisons
        method = cmp[:method]
        colname1, colname2 = cmp[:colnames]
        dist = compare(method(), data1[i1, colname1], data2[i2, colname2])
        push!(result, dist)
    end
    result
end

end
