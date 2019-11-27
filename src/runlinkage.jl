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
    @info "$(now()) Configuring linkage"
    cfg = LinkageConfig(configfile)

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    d = cfg.output_directory
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    cp(configfile, joinpath(d, "input", basename(configfile)))  # Copy config file to d/input
    software_versions = utils.construct_software_versions_table()
    CSV.write(joinpath(d, "input", "SoftwareVersions.csv"), software_versions; delim=',')      # Write software_versions to d/input
    criteria = utils.construct_criteria_table(cfg)
    CSV.write(joinpath(d, "output", "criteria.tsv"), criteria; delim='\t')  # Write criteria to d/output

    @info "$(now()) Importing spine"
    spine = DataFrame(CSV.File(cfg.spine.datafile; type=String))  # For performance only Strings are compared (avoids parsing values)

    @info "$(now()) Appending spineID to spine"
    utils.append_spineid!(spine, cfg.spine.schema.primarykey)

    @info "$(now()) Writing spine_primarykey_and_spineid.tsv to the output directory"
    CSV.write(joinpath(cfg.output_directory, "output", "spine_primarykey_and_spineid.tsv"), spine[!, vcat(:spineID, cfg.spine.schema.primarykey)]; delim='\t')

    # Replace the spine's primary key with [:spineID]
    empty!(cfg.spine.schema.primarykey)
    push!(cfg.spine.schema.primarykey, :spineID)

    # Do the linkage for each table
    for tablecriteria in cfg.criteria
        # Create an empty output file for the data and store it in the output directory
        tablename = tablecriteria[1].tablename
        @info "$(now()) Starting linkage for table $(tablename)"
        tableschema   = cfg.tables[tablename].schema
        data          = init_data(tableschema, 0)  # Columns are [:spineID, :criteriaID, primarykey_columns...]
        table_outfile = joinpath(cfg.output_directory, "output", "$(tablename)_linked.tsv")
        CSV.write(table_outfile, data; delim='\t')

        # Run the data through each linkage iteration
        table_infile     = cfg.tables[tablename].datafile
        criteriaid2index = utils.construct_table_indexes(tablecriteria, spine)  # criteria.id => TableIndex(spine, colnames, index)
        criteriaid2key   = Dict(id => fill("", length(tableindex.colnames)) for (id, tableindex) in criteriaid2index)  # Place-holder for lookup keys
        link_table_to_spine(spine, table_infile, table_outfile, tableschema, tablecriteria, criteriaid2index, criteriaid2key)
    end
    @info "$(now()) Finished linkage"
end

function link_table_to_spine(spine::DataFrame,
                            table_infile::String, table_outfile::String, tableschema::TableSchema,
                            tablecriteria::Vector{LinkageCriteria}, criteriaid2index::Dict{Int, TableIndex}, criteriaid2key::Dict{Int, Vector{String}})
    data      = init_data(tableschema, 1_000_000)  # Process the data in batches of 1_000_000 rows
    i_data    = 0
    ndata     = 0
    nlinks    = 0
    tablename = tableschema.name
    data_primarykey = tableschema.primarykey
    for row in CSV.Rows(table_infile; reusebuffer=true)
        # Store primary key
        i_data += 1
        for colname in data_primarykey
            data[i_data, colname] = getproperty(row, colname)
        end

        # Link the row to the spine using the first LinkageCriteria that are satisfied (if any)
        for linkagecriteria in tablecriteria
            # Identify the spine records that satisfy the exactmatch criteria (if they exist)
            criteriaid = linkagecriteria.id
            tableindex = criteriaid2index[criteriaid]
            hasmissing = utils.constructkey!(criteriaid2key[criteriaid], row, tableindex.colnames)
            hasmissing && continue                    # datarow[colnames] includes a missing value
            k = Tuple(criteriaid2key[criteriaid])
            !haskey(tableindex.index, k) && continue  # Row doesn't match any spine records on linkagecriteria.exactmatch
            candidate_indices = tableindex.index[k]   # Indices of rows of the spine that satisfy linkagecriteria.exactmatch
            isempty(candidate_indices) && continue    # There are no spine records that satisfy the exactmatch criteria

            # Identify the best matching spine record (if it exists)
            spineid = select_best_candidate(spine, candidate_indices, row, linkagecriteria.approxmatch)
            spineid == 0 && continue

            # Create a record in the linkmap
            nlinks += 1
            data[i_data, :spineID]    = spineid
            data[i_data, :criteriaID] = criteriaid
            break  # Row has been linked, no need to link on other criteria
        end

        # If data is full, write to disk
        if i_data == 1_000_000
            CSV.write(table_outfile, data; delim='\t', append=true)
            ndata += i_data
            @info "$(now()) Exported $(ndata) rows of table $(tablename)"
            i_data = 0  # Reset the row number
        end
    end
    if i_data != 0  # Write remaining rows if they exist
        CSV.write(table_outfile, data[1:i_data, :]; append=true, delim='\t')
        ndata += i_data
        @info "$(now()) Exported $(ndata) rows of table $(tablename)"
    end
    @info "$(now()) $(nlinks) rows of table $(tablename) were linked to the spine."
end

function select_best_candidate(spine, candidate_indices::Vector{Int}, row, approxmatches::Vector{ApproxMatch})
    if isempty(approxmatches)
        length(candidate_indices) == 1 && return spine[candidate_indices[1], :spineID]  # There is 1 candidate and no further selection criteria to be met
        return 0  # There is more than 1 candidate and no way to select the best one
    end
    result = 0
    min_distance = 1.0
    for i_spine in candidate_indices
        dist = 0.0
        ok   = true  # Each ApproxMatch criterion is satisfied
        for approxmatch in approxmatches
            dataval  = getproperty(row, approxmatch.datacolumn)
            spineval = spine[i_spine, approxmatch.spinecolumn]
            d        = distance(approxmatch.distancemetric, dataval, spineval)
            if d <= approxmatch.threshold
                dist += d
            else
                ok = false
                break
            end
        end
        !ok && continue  # Not every ApproxMatch criterion is satisfied
        dist >= min_distance && continue  # distance(row, candidate) is not minimal among candidates tested so far
        min_distance = dist
        result = spine[i_spine, :spineID]
    end
    result
end

function init_data(tableschema::TableSchema, n::Int)
    colnames = vcat(:spineID, :criteriaID, tableschema.primarykey)
    coltypes = vcat(Union{Missing, UInt}, Union{Missing, Int}, fill(Union{Missing, String}, length(tableschema.primarykey)))
    DataFrame(coltypes, colnames, n)
end

end