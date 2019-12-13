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
using ..runlinkage

"""
Construct a spine by linking a table to itself.
Return the output directory.

The output directory contains:
- A spine with columns: vcat(spineID, names(table)).
- A table containing the primary key of the input table as well as new columns :spineID and :criteriaID.
"""
function construct_spine(configfile::String)
    @info "$(now()) Configuring spine construction"
    cfg = spine_construction_config(configfile)

    @info "$(now()) Importing spine input data"
    data = DataFrame(CSV.File(cfg.spine.datafile; type=String))
    if in(:spineID, names(data))
        select!(data, Not(:spineID))
    end

    @info "$(now()) Constructing groups of linked rows."
    mc = construct_maximal_cliques(cfg, data)

    @info "$(now()) Constructing the spine from the linked rows."
    spinerows = [rowindices[1] for rowindices in mc]  # Reduce each group to 1 row by selecting the first row (arbitrary choice)
    spine     = data[spinerows, :]
    utils.append_spineid!(spine, cfg.spine.schema.primarykey)
    spine     = spine[:, vcat(:spineID, names(data))]

    @info "$(now()) Writing the spine to a temporary directory"
    tmpdir    = mktempdir(dirname(cfg.output_directory))
    spinefile = joinpath(tmpdir, "spine.tsv")
    CSV.write(spinefile, spine; delim='\t')
    spine     = ""  # Enable GC to be triggered

    @info "Constructing a linkage config that uses the spine"
    spineconfig = TableConfig(spinefile, cfg.spine.schemafile, cfg.spine.schema)
    newcfg      = LinkageConfig(cfg.projectname, dirname(cfg.output_directory), spineconfig, cfg.tables, cfg.criteria)
    newcfg_file = joinpath(tmpdir, "constructspine.yml")
    writeconfig(newcfg_file, newcfg)

    @info "Linking data to the spine"
    outdir = run_linkage(newcfg_file)

    @info "$(now()) Cleaning up the output directory"
    rm(joinpath(outdir, "input",  "constructspine.yml"))     # Replace new config file with original config file
    cp(configfile, joinpath(outdir, "input", basename(configfile)))
    cp(spinefile,  joinpath(outdir, "output", "spine.tsv"))  # Replace spine_primarykey_and_spineid.tsv with spine.tsv
    rm(joinpath(outdir, "output", "spine_primarykey_and_spineid.tsv"))
    rm(tmpdir; recursive=true)

    @info "$(now()) Finished spine construction"
    outdir
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
               Record the link (i,j), using `add_edge!(g, i, j)`, if the candidate satisfies the approximate match criteria.
3. Calcluate maximal_cliques(g).
4. Remove singletons (vertices with 0 edges), which are singletons because they have incomplete data on all linkage criteria and thus don't even link to themselves.
"""
function construct_maximal_cliques(cfg::LinkageConfig, spine::DataFrame)
    # 1. Init graph
    n = size(spine, 1)
    g = SimpleGraph{Int}(n)

    # 2. Loop through data
    criteria           = cfg.criteria[1]  # Vector{LinkageIteration}. Each linkagecriteria refers to the data table.
    tablename          = criteria[1].tablename
    criteriaid2index  = utils.construct_table_indexes(criteria, spine)  # linkagecriteria.id => TableIndex(spine, colnames, index)
    criteriaid2key    = Dict(id => fill("", length(tableindex.colnames)) for (id, tableindex) in criteriaid2index)  # Place-holder for lookup keys
    vertices_to_remove = Set{Int}()
    for i_data = 1:n
        datarow = spine[i_data, :]
        nlinks  = 0
        for linkagecriteria in criteria
            tableindex = criteriaid2index[linkagecriteria.id]
            hasmissing = utils.constructkey!(criteriaid2key[linkagecriteria.id], datarow, tableindex.colnames)
            hasmissing && continue                    # datarow[exactmatches] includes a missing value
            k = Tuple(criteriaid2key[linkagecriteria.id])
            !haskey(tableindex.index, k) && continue
            candidate_indices = tableindex.index[k]   # Indices of rows of the spine that satisfy linkagecriteria.exactmatches
            for i_spine in candidate_indices
                ok = candidate_satisfies_approximate_criteria(spine, i_spine, datarow, linkagecriteria.approxmatch)
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

"Returns: true if the spine candidate satisifies the data row on the criteria defined by approxmatches."
function candidate_satisfies_approximate_criteria(spine, candidate_index::Int, datarow, approxmatches::Vector{ApproxMatch})
    isempty(approxmatches) && return true
    for approxmatch in approxmatches
        dataval  = getproperty(datarow,   approxmatch.datacolumn)
        spineval = spine[candidate_index, approxmatch.spinecolumn]
        distance(approxmatch.distancemetric, dataval, spineval) > approxmatch.threshold && return false
    end
    true
end

end
