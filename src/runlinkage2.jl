"""
Notes:
- Usually we want to retain more than the intersection of columns of the input tables, but less than the union of columns.
  E.g., A table may contain name and DOB, another table may contain name, address and social security number,
  and both tables may also contain other episode-level data such as timestamps and service locations.
  We want to retain the entity-level data (name, DOB, address and social security number) and not the the episode-level data.
  Therefore we require a schema for the spine so that the desired columns are explicitly specified.
"""
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
    CSV.write(joinpath(d, "input", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/input
    criteria = utils.construct_criteria_table(cfg)
    CSV.write(joinpath(d, "output", "criteria.tsv"), criteria; delim='\t')  # Write criteria to d/output

    # Init spine
    if isnothing(cfg.spine.datafile)
        @info "$(now()) Initialising spine"
        colnames = vcat(:spineID, cfg.spine.schema.columnorder)
        coltypes = vcat(Union{Missing, UInt}, fill(Union{Missing, String}, length(colnames) - 1))
        spine    = DataFrame(coltypes, colnames, 0)
    else
        @info "$(now()) Importing spine"
        spine = DataFrame(CSV.File(cfg.spine.datafile; type=String))  # For performance only Strings are compared (avoids parsing values)
        spine[!, :spineID] = [parse(UInt, x) for x in spine[!, :spineID]]
    end

    # Do the linkage for each table
    spine_primarykey = cfg.spine.schema.primarykey
    for tablecriteria in cfg.criteria
        # Create an empty output file for the data and store it in the output directory
        tablename = tablecriteria[1].tablename
        @info "$(now()) Starting linkage for table $(tablename)"
        tableschema   = cfg.tables[tablename].schema
        data          = init_data(tableschema, 0)  # Columns are [:spineID, :criteriaID, primarykey_columns...]
        table_outfile = joinpath(cfg.output_directory, "output", "$(tablename)_linked.tsv")
        CSV.write(table_outfile, data; delim='\t')

        # Run the data through each linkage iteration
        table_infile = cfg.tables[tablename].datafile
        link_table_to_spine(spine, spine_primarykey, table_infile, table_outfile, tableschema, tablecriteria)
    end

    @info "$(now()) Writing spine to the output directory"
    spine_outfile = joinpath(cfg.output_directory, "output", "spine.tsv")
    CSV.write(spine_outfile, spine; delim='\t')

    @info "$(now()) Finished linkage"
    cfg.output_directory
end

################################################################################
# Unexported

"Returns a DataFrame with n rows and columns [:spineID, :criteriaID, tableschema.primarykey...]."
function init_data(tableschema::TableSchema, n::Int)
    colnames = vcat(:spineID, :criteriaID, tableschema.primarykey)
    coltypes = vcat(Union{Missing, UInt}, Union{Missing, Int}, fill(Union{Missing, String}, length(tableschema.primarykey)))
    DataFrame(coltypes, colnames, n)
end

function link_table_to_spine(spine::DataFrame, spine_primarykey::Vector{Symbol},
                             table_infile::String, table_outfile::String, tableschema::TableSchema, tablecriteria::Vector{LinkageCriteria})
    data      = init_data(tableschema, 1_000_000)  # Process the data in batches of 1_000_000 rows
    i_data    = 0
    ndata     = 0
    nlinks    = 0
    tablename = tableschema.name
    spinecols = Set(names(spine))
    data_primarykey  = tableschema.primarykey
    criteriaid2index = utils.construct_table_indexes(tablecriteria, spine)  # criteria.id => TableIndex(spine, colnames, index)
    criteriaid2key   = Dict(id => fill("", length(tableindex.colnames)) for (id, tableindex) in criteriaid2index)  # Place-holder for lookup keys
    for row in CSV.Rows(table_infile; reusebuffer=true)
        # Store data primary key
        i_data += 1
        data[i_data, :spineID]    = missing
        data[i_data, :criteriaID] = missing
        for colname in data_primarykey
            data[i_data, colname] = getproperty(row, colname)
        end

        # Link the row to the spine using the first LinkageCriteria that are satisfied (if any)
        nlinks = link_row_to_spine!(data, i_data, row, spine, tablecriteria, criteriaid2index, criteriaid2key, nlinks, spinecols)

        # If row is unlinked, append it to the spine, update the TableIndexes and link
        if ismissing(data[i_data, :spineID])
            append_row_to_spine!(spine, spine_primarykey, row, spinecols)  # Create a new spine record
            for linkagecriteria in tablecriteria
                tableindex = criteriaid2index[linkagecriteria.id]
                utils.update!(tableindex, spine, size(spine, 1))  # Update the tableindex
            end
            nlinks = link_row_to_spine!(data, i_data, row, spine, tablecriteria, criteriaid2index, criteriaid2key, nlinks, spinecols)
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

"""
Modified: data[i_data, :]

Link the row to the spine using the first LinkageCriteria that are satisfied (if any).
"""
function link_row_to_spine!(data, i_data::Int, row, spine, tablecriteria::Vector{LinkageCriteria},
                            criteriaid2index, criteriaid2key, nlinks::Int, spinecols::Set{Symbol})
    for linkagecriteria in tablecriteria
        # Identify the spine records that match the row on linkagecriteria.exactmatch
        criteriaid = linkagecriteria.id
        tableindex = criteriaid2index[criteriaid]
        hasmissing = utils.constructkey!(criteriaid2key[criteriaid], row, tableindex.colnames)
        hasmissing && continue  # datarow[tableindex.colnames] includes a missing value
        k = Tuple(criteriaid2key[criteriaid])
        !haskey(tableindex.index, k) && continue  # Row doesn't match any spine records on linkagecriteria.exactmatch
        candidate_indices = tableindex.index[k]

        # Identify the spine record that best matches the row (if it exists)
        spineid, i_spine = select_best_candidate(spine, candidate_indices, row, linkagecriteria.approxmatch)
        spineid == 0 && continue  # None of the candidates satisfy the approxmatch criteria

        # Merge data from row into spine[i_spine, :]
        mergerow!(row, spine, i_spine, spinecols)

        # Create a link between the spine and the data
        nlinks += 1
        data[i_data, :spineID]    = spineid
        data[i_data, :criteriaID] = criteriaid
        break  # Row has been linked, no need to link on other criteria
    end
    nlinks
end

function select_best_candidate(spine, candidate_indices::Vector{Int}, row, approxmatches::Vector{ApproxMatch})
    if isempty(approxmatches)
        if length(candidate_indices) == 1
            i_spine = candidate_indices[1]
            return spine[i_spine, :spineID], i_spine  # There is 1 candidate and no further selection criteria to be met
        else
            return 0, 0  # There is more than 1 candidate and no way to select the best one
        end
    end
    result = 0, 0
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
        result = spine[i_spine, :spineID], i_spine
    end
    result
end

"Append the row to the spine and return the spineid of the new row."
function append_row_to_spine!(spine, spine_primarykey, row, spinecols::Set{Symbol})
    d = Dict{Symbol, Union{Missing, String}}()
    for colname in spinecols
        d[colname] = hasproperty(row, colname) ? getproperty(row, colname) : missing
    end
    push!(spine, d)
    i = size(spine, 1)
    spine[i, :spineID] = hash(spine[i, spine_primarykey])
end

"""
Merge data from row into spine[i, :].

Currently row[column] is written to spine[i, column] if the latter is missing.
"""
function mergerow!(row, spine, i, spinecols::Set{Symbol})
    for colname in propertynames(row)
        !in(colname, spinecols) && continue
        rowval = getproperty(row, colname)
        ismissing(rowval) && continue
        if ismissing(spine[i, colname])
            spine[i, colname] = rowval
        end
    end
end

end
