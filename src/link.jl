module link

using CSV
using Dates
using DataFrames
using Logging
using Schemata

using ..TableIndexes
using ..distances
using ..config


function linktables(cfg::LinkageConfig, spine::DataFrame)
    spineschema = cfg.spine.schema
    linkmap     = DataFrame([UInt, UInt, Int], [:spineid, :recordid, :iterationid], 0)
    for table_iterations in cfg.iterations
        # Make an output directory for the table
        tablename = table_iterations[1].tablename
        @info "$(now()) Starting linkage for table $(tablename)"

        # Create an empty linkmap and store it in the output directory
        linkmap_file = joinpath(cfg.output_directory, "output", "deidentified", "linkmap-$(tablename).tsv")
        CSV.write(linkmap_file, linkmap; delim='\t')

        # Create an empty output file for the data and store it in the output directory
        tableschema   = cfg.tables[tablename].schema
        data          = init_data(tableschema, 0)  # Columns are [:recordid, primarykey_columns...]
        table_outfile = joinpath(cfg.output_directory, "output", "identified", "$(tablename).tsv")
        CSV.write(table_outfile, data; delim='\t')

        # Construct some convenient lookups for computational efficiency
        iterationid2index = construct_table_indexes(table_iterations, spine)  # iteration.id => TableIndex(spine, colnames, index)
        iterationid2key   = Dict(id => fill("", length(tableindex.colnames)) for (id, tableindex) in iterationid2index)  # Place-holder for lookup keys

        # Run the data through each iteration
        table_infile = cfg.tables[tablename].datafile
        linktable(spine, spineschema, table_infile, table_outfile, tableschema, linkmap_file, table_iterations, iterationid2index, iterationid2key)
    end
end

function linktable(spine::DataFrame, spineschema::TableSchema,
                   table_infile::String, table_outfile::String, tableschema::TableSchema,
                   linkmap_file::String,
                   iterations::Vector{LinkageIteration}, iterationid2index::Dict{Int, TableIndex}, iterationid2key::Dict{Int, Vector{String}})
    linkmap   = DataFrame([UInt, UInt, Int], [:spineid, :recordid, :iterationid], 1_000_000)  # Process the data in batches of 1_000_000 rows
    data      = init_data(tableschema, 1_000_000)  # Process the data in batches of 1_000_000 rows
    i_linkmap = 0
    i_data    = 0
    nlinks    = 0
    ndata     = 0
    tablename = tableschema.name
    data_primarykey  = tableschema.primarykey
    spine_primarykey = spineschema.primarykey[1]
    for row in CSV.Rows(table_infile; reusebuffer=true)
        # Store recordid and primary key
        i_data += 1
        for colname in data_primarykey
            data[i_data, colname] = getproperty(row, colname)
        end
        data[i_data, :recordid] = hash(data[i_data, 2:end])

        # Loop through each LinkageIteration
        for iteration in iterations
            # Identify the best matching spine record (if it exists)
            tableindex = iterationid2index[iteration.id]
            hasmissing = constructkey!(iterationid2key[iteration.id], row, tableindex.colnames)
            hasmissing && continue
            k = Tuple(iterationid2key[iteration.id])
            !haskey(tableindex.index, k) && continue  # Row doesn't match any spine records on iteration.exactmatchcols
            candidate_indices = tableindex.index[k]   # Indices of rows of the spine that satisfy iteration.exactmatchcols
            spineid = select_best_candidate(spine, spine_primarykey, candidate_indices, row, iteration.fuzzymatches)
            spineid == 0 && continue

            # Create a record in the linkmap
            i_linkmap += 1
            linkmap[i_linkmap, :spineid]     = spineid
            linkmap[i_linkmap, :recordid]    = data[i_data, :recordid]
            linkmap[i_linkmap, :iterationid] = iteration.id
            break  # Row has been linked, no need to link on other criteria
        end

        # If data is full, write to disk
        if i_data == 1_000_000
            CSV.write(table_outfile, data; delim='\t', append=true)
            i_data = 0  # Reset the row number
            ndata += 1_000_000
            @info "$(now()) Exported $(ndata) rows of table $(tablename)"
        end

        # If linkmap is full, write to disk
        if i_linkmap == 1_000_000
            nlinks    = write_linkmap_to_disk(linkmap_file, linkmap, nlinks, tablename)
            i_linkmap = 0  # Reset the row number
        end
    end

    # Write remaining rows if they exist
    i_linkmap != 0 && write_linkmap_to_disk(linkmap_file, linkmap[1:i_linkmap, :], nlinks, tablename)
    if i_data != 0
        CSV.write(table_outfile, data[1:i_data, :]; append=true, delim='\t')
        ndata += i_data
        @info "$(now()) Exported $(ndata) rows of table $(tablename)"
    end
end

################################################################################
# Utils

"Returns: Dict(iterationid => TableIndex(spine, colnames))"
function construct_table_indexes(iterations::Vector{LinkageIteration}, spine)
    # Create TableIndexes
    tmp = Dict{Int, TableIndex}()
    for iteration in iterations
        colnames = [spine_colname for (data_colname, spine_colname) in iteration.exactmatchcols]
        tmp[iteration.id] = TableIndex(spine, colnames)
    end

    # Replace spine colnames with data colnames
    # A hack to avoid converting from spine colnames to data colnames on each lookup
    result = Dict{Int, TableIndex}()
    for iteration in iterations
        data_colnames = [data_colname for (data_colname, spine_colname) in iteration.exactmatchcols]
        tableindex    = tmp[iteration.id]
        result[iteration.id] = TableIndex(spine, data_colnames, tableindex.index)
    end
    result
end

function init_data(tableschema::TableSchema, n::Int)
    colnames = vcat(:recordid, tableschema.primarykey)
    coltypes = vcat(UInt, fill(Union{Missing, String}, length(tableschema.primarykey)))
    DataFrame(coltypes, colnames, n)
end

function write_linkmap_to_disk(linkmap_file, linkmap, nlinks, tablename)
    nlinks += size(linkmap, 1)
    CSV.write(linkmap_file, linkmap; delim='\t', append=true)
    @info "$(now()) $(nlinks) links created between the spine and table $(tablename)"
    nlinks
end

"Returns: true if row[colnames] includes a missing value."
function constructkey!(result::Vector{String}, row, colnames::Vector{Symbol})
    for (j, colname) in enumerate(colnames)  # Populate result with the row's values of tableindex.colnames (iteration.exactmatchcols)
        val = getproperty(row, colname)
        ismissing(val) && return true
        result[j] = val
    end
    false
end

function select_best_candidate(spine, spine_primarykey::Symbol, candidate_indices::Vector{Int}, row, fuzzymatches::Vector{FuzzyMatch})
    if isempty(fuzzymatches)
        length(candidate_indices) == 1 && return spine[candidate_indices[1], spine_primarykey]  # There is 1 candidate and no further selection criteria to be met
        return 0  # There is more than 1 candidate and no way to select the best one
    end
    result = 0
    min_distance = 1.0
    for i_spine in candidate_indices
        dist = 0.0
        ok   = true  # Each FuzzyMatch criterion is satisfied
        for fuzzymatch in fuzzymatches
            dataval  = getproperty(row, colname)
            spineval = spine[i_spine, fuzzymatch.spinecolumn]
            d        = distance(fuzzymatch.distancemetric, dataval, spineval)
            if d <= fuzzymatch.threshold
                dist += d
            else
                ok = false
                break
            end
        end
        !ok && continue  # Not every FuzzyMatch criterion is satisfied
        dist >= min_distance && continue  # distance(row, candidate) is not minimal among candidates tested so far
        min_distance = dist
        result = spine[i_spine, spine_primarykey]
    end
    result
end

end