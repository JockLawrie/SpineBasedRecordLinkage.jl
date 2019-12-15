module TableIndexes

export TableIndex

using Tables

struct TableIndex{T1, T2}
    table::T1
    colnames::Vector{Symbol}      # Column names used to construct the index
    index::Dict{T2, Vector{Int}}  # (val1, val2, ...) => [rowindex1, ...], where val_j = value of colnames[j]
end

function TableIndex(table::T1, colnames::Vector{Symbol}) where {T1}
    # Determine T2
    schema = Tables.schema(table)
    colname2type = Dict{Symbol, Any}(colname => tp for (colname, tp) in zip(schema.names, schema.types))
    types = [colname2type[colname] for colname in colnames]
    T2    = string(Tuple(types))       # "(T1, T2, ...)"
    T2    = "Tuple{$(T2[2:(end-1)])}"  # "Tuple{T1, T2, ...}"
    T2    = eval(Meta.parse(T2))

    # Construct TableIndex
    index = Dict{T2, Vector{Int}}()
    rows  = Tables.rows(table)
    row1  = Vector{Any}(undef, length(colnames))  # A row of the table
    i     = 0
    for row in rows
        i += 1
        j  = 0
        for colname in colnames
            j += 1
            row1[j] = getproperty(row, colname)
        end
        k = Tuple(row1)
        update!(index, k, i)
    end
    TableIndex{T1, T2}(table, colnames, index)
end

function update!(index, k, i)
    if !haskey(index, k)
        index[k] = [i]
    else
        push!(index[k], i)
    end
end

end