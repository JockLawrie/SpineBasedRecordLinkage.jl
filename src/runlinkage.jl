module runlinkage

export run_linkage

using CSV
using DataFrames
using Dates
using Logging
using Schemata

using ..TableIndexes
using ..distances
using ..config
using ..utils

function run_linkage(configfile::String)
    @info "$(now()) Configuring linkage run"
    cfg = LinkageConfig(configfile)

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    d = cfg.output_directory
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    mkdir(joinpath(d, "output", "deidentified"))
    mkdir(joinpath(d, "output", "identified"))
    cp(configfile, joinpath(d, "input", basename(configfile)))      # Copy config file to d/input
    software_versions = utils.construct_software_versions_table()
    CSV.write(joinpath(d, "output", "deidentified", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/output
    iterations = utils.construct_iterations_table(cfg)
    CSV.write(joinpath(d, "output", "deidentified", "Iterations.csv"), iterations; delim=',')               # Write iterations to d/output

    @info "$(now()) Importing spine"
    spine = DataFrame(CSV.File(cfg.spine.datafile; type=String))    # We only compare Strings...avoids parsing values

    @info "$(now()) Appending spineid to spine"
    utils.append_spineid!(spine, cfg.spine.schema.primarykey)

    @info "$(now()) Writing spine_identified.tsv to disk"
    CSV.write(joinpath(cfg.output_directory, "output", "identified", "spine_identified.tsv"), spine[!, vcat(:spineid, cfg.spine.schema.primarykey)]; delim='\t')

    @info "$(now()) Writing spine_deidentified.tsv to disk for reporting"
    spine_deidentified = DataFrame(spineid = spine[!, :spineid])
    CSV.write(joinpath(cfg.output_directory, "output", "deidentified", "spine_deidentified.tsv"), spine_deidentified; delim='\t')

    # Replace the spine's primary key with [:spineid]
    empty!(cfg.spine.schema.primarykey)
    push!(cfg.spine.schema.primarykey, :spineid)

    # Do the linkage
    linktables(cfg, spine)
    @info "$(now()) Finished linkage"
end

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
        iterationid2index = utils.construct_table_indexes(table_iterations, spine)  # iteration.id => TableIndex(spine, colnames, index)
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
            hasmissing = utils.constructkey!(iterationid2key[iteration.id], row, tableindex.colnames)
            hasmissing && continue                    # datarow[colnames] includes a missing value
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
            dataval  = getproperty(row, fuzzymatch.datacolumn)
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

end