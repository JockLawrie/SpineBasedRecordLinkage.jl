module constructspine

export construct_spine

using CSV
using DataFrames
using Dates
using LightGraphs
using Logging
using Schemata

using ..TableIndexes
using ..distances
using ..config
using ..utils

"""
Construct a spine by linking a table to itself.
"""
function construct_spine(configfile::String)
    @info "$(now()) Configuring spine construction"
    cfg = spine_construction_config(configfile)

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    d = cfg.output_directory
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    cp(configfile, joinpath(d, "input", basename(configfile)))      # Copy config file to d/input
    software_versions = utils.construct_software_versions_table()
    CSV.write(joinpath(d, "input", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/input

    @info "$(now()) Importing data"
    data = DataFrame(CSV.File(cfg.spine.datafile; type=String))    # We only compare Strings...avoids parsing values (which should be done prior to linkage using Schemata.jl)

    @info "$(now()) Constructing groups of linked rows."
    mc = construct_maximal_cliques(cfg, data)

    @info "$(now()) Constructing the spine from the linked rows."
    spinerows = [rowindices[1] for rowindices in mc]  # Select the first row of each group for inclusion in the spine (arbitrary choice)
    spine     = data[spinerows, :]
    utils.append_spineid!(spine, cfg.spine.schema.primarykey)

    @info "$(now()) Writing spine to disk"
    colnames = vcat(:spineid, names(data))
    CSV.write(joinpath(cfg.output_directory, "output", "spine.tsv"), spine[!, colnames]; delim='\t')

    @info "$(now()) Finished spine construction"
end

"""
Return: A Vector{Vector{Int}}, where each inner vector is a list of row indices that denote a cluster of pairwise linked rows (maximal clique).

The algorithm is as follows:

1. Init g = SimpleGraph{Int}(0).
   - Each vertex represents a record in the data table.
   - Each edge (i,j) represents a link between record i in the data and record j in the data.
     Under the hood this is achieved as a link from record i in the data to record j in the spine.
2. For each data record (i)
       For each linkage iteration
           Get all rows from the spine which satisfy the exact match criteria
           For each candidate row from the spine (j)
               Record the link (i,j), using `add_edge!(g, i, j)`, if the candidate satisfies the fuzzy match criteria.
3. Calcluate maximal_cliques(g).
4. Remove singletons (vertices with 0 edges), which are singletons because they have incomplete data on all linkage criteria and thus don't even link to themselves.
"""
function construct_maximal_cliques(cfg::LinkageConfig, spine::DataFrame)
    # 1. Init graph
    n = size(spine, 1)
    g = SimpleGraph{Int}(n)

    # 2. Loop through data
    iterations         = cfg.iterations[1]  # Vector{LinkageIteration}. Each iteration refers to the data table.
    tablename          = iterations[1].tablename
    iterationid2index  = utils.construct_table_indexes(iterations, spine)  # iteration.id => TableIndex(spine, colnames, index)
    iterationid2key    = Dict(id => fill("", length(tableindex.colnames)) for (id, tableindex) in iterationid2index)  # Place-holder for lookup keys
    vertices_to_remove = Set{Int}()
    for i_data = 1:n
        datarow = spine[i_data, :]
        nlinks  = 0
        for iteration in iterations
            tableindex = iterationid2index[iteration.id]
            hasmissing = utils.constructkey!(iterationid2key[iteration.id], datarow, tableindex.colnames)
            hasmissing && continue                    # datarow[exactmatchcols] includes a missing value
            k = Tuple(iterationid2key[iteration.id])
            candidate_indices = tableindex.index[k]   # Indices of rows of the spine that satisfy iteration.exactmatchcols
            for i_spine in candidate_indices
                ok = candidate_satisfies_fuzzy_criteria(spine, i_spine, datarow, iteration.fuzzymatches)
                if ok
                    add_edge!(g, i_data, i_spine)   # Returns false if the edge or its reverse (i_spine, i_data) already exists
                    nlinks += 1
                end
            end
        end
        if nlinks == 0
            push!(vertices_to_remove, i_data)  # A row links to istelf unless it satifies none of the linkage criteria due to missing data
        end
    end

    # 3. Get maximal cliques
    mc = maximal_cliques(g)

    # 4. Remove singletons with 0 links (due to incomplete data on all linkage criteria)
    result = Vector{Int}[]
    for maximalclique in mc
        length(maximalclique) == 1 && in(maximalclique[1], vertices_to_remove) && continue
        push!(result, maximalclique)
    end
    result
end

"Returns: true if the spine candidate satisifies the data row on the criteria defined by the fuzzymatches."
function candidate_satisfies_fuzzy_criteria(spine, candidate_index::Int, datarow, fuzzymatches::Vector{FuzzyMatch})
    isempty(fuzzymatches) && return true
    for fuzzymatch in fuzzymatches
        dataval  = getproperty(datarow,   fuzzymatch.datacolumn)
        spineval = spine[candidate_index, fuzzymatch.spinecolumn]
        distance(fuzzymatch.distancemetric, dataval, spineval) > fuzzymatch.threshold && return false
    end
    true
end

end