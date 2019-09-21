module TableIndexes

export TableIndex, createindex,  get_rowindices, unsafe_get_rowindices

using Tables


struct TableIndex{T}
    colnames::Vector{Symbol}     # Column names used to construct the index
    index::Dict{T, Vector{Int}}  # (colname1=val1, ...) => [rowindex1, ...]
end


function createindex(table, colnames::Vector{Symbol})
    # Determine T
    schema = Tables.schema(table)
    colname2type = Dict{Symbol, Any}(colname => tp for (colname, tp) in zip(schema.names, schema.types))
    types = [colname2type[colname] for colname in colnames]
    T     = string(Tuple(types))      # "(T1, T2, ...)"
    T     = "Tuple{$(T[2:(end-1)])}"  # "Tuple{T1, T2, ...}"
    T     = eval(Meta.parse(T))

    # Construct TableIndex
    index = Dict{T, Vector{Int}}()
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
        if !haskey(index, k)
            index[k] = [i]
        else
            push!(index[k], i)
        end
    end
    TableIndex{T}(colnames, index)
end


"Returns: Vector of row indices for which table[result, colnames] == k"
function get_rowindices(tableindex, colnames, k)
    if colnames == tableindex.colnames && haskey(tableindex.index, k)
        tableindex.index[k]
    end
    nothing
end

"Aas per get_rowidindices, ASSUMING k corresponds to tableindex.colnames"
unsafe_get_rowindices(tableindex, k) = haskey(tableindex.index, k) ? tableindex.index[k] : nothing


end
