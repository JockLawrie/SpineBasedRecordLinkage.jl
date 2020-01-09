"""
Notes:
- Usually we want to retain more than the intersection of columns of the input tables, but less than the union of columns.
  E.g., A table may contain name and DOB, another table may contain name, address and social security number,
  and both tables may also contain other event-level data such as timestamps and service locations.
  We want to retain the entity-level data (name, DOB, address and social security number) and not the event-level data.
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

function run_linkage(configfile::String)
    @info "$(now()) Configuring linkage"
    cfg = LinkageConfig(configfile)

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    d = cfg.output_directory
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    cp(configfile, joinpath(d, "input", basename(configfile)))  # Copy config file to d/input
    software_versions = construct_software_versions_table()
    CSV.write(joinpath(d, "input", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/input
    criteria = construct_criteria_table(cfg)
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
        link_table_to_spine!(spine, spine_primarykey, cfg.append_to_spine, table_infile, table_outfile, tableschema, tablecriteria)
    end

    @info "$(now()) Writing spine to the output directory ($(size(spine, 1)) rows)"
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

"Modified: spine"
function link_table_to_spine!(spine::DataFrame, spine_primarykey::Vector{Symbol}, append_to_spine::Bool,
                              table_infile::String, table_outfile::String, tableschema::TableSchema, tablecriteria::Vector{LinkageCriteria})
    data       = init_data(tableschema, 1_000_000)  # Process the data in batches of 1_000_000 rows
    i_data     = 0
    ndata      = 0
    nlinks     = 0
    tablename  = tableschema.name
    spinecols  = Set(names(spine))
    n_criteria = length(tablecriteria)
    data_primarykey  = tableschema.primarykey
    criteriaid2index = construct_table_indexes(tablecriteria, spine)  # criteria.id => TableIndex(spine, colnames, index)
    criteriaid2key   = Dict(id => fill("", length(tableindex.colnames)) for (id, tableindex) in criteriaid2index)  # Place-holder for lookup keys
    for row in CSV.Rows(table_infile; reusebuffer=true, use_mmap=true)
        # Store data primary key
        i_data += 1
        data[i_data, :spineID]    = missing
        data[i_data, :criteriaID] = missing
        for colname in data_primarykey
            data[i_data, colname] = getproperty(row, colname)
        end

        # Link the row to the spine using the first LinkageCriteria that are satisfied (if any)
        # n_hasmissing = Number of criteria for which row has missing data
        # If n_hasmissing == n_criteria then the row cannot be appended to the spine because no criteria can be satisfied
        nlinks, n_hasmissing = link_row_to_spine!(data, i_data, row, spine, tablecriteria, criteriaid2index, criteriaid2key, nlinks, spinecols)

        # If row is unlinked, append it to the spine, update the TableIndexes and link
        if append_to_spine && n_hasmissing < n_criteria && ismissing(data[i_data, :spineID])
            append_row_to_spine!(spine, spine_primarykey, row, spinecols)  # Create a new spine record
            for linkagecriteria in tablecriteria
                tableindex = criteriaid2index[linkagecriteria.id]
                update!(tableindex, spine, size(spine, 1))  # Update the tableindex
            end
            nlinks, n_hasmissing = link_row_to_spine!(data, i_data, row, spine, tablecriteria, criteriaid2index, criteriaid2key, nlinks, spinecols)
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
        CSV.write(table_outfile, view(data, 1:i_data, :); append=true, delim='\t')
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
    n_hasmissing = 0  # Number of criteria for which row has missing data
    for linkagecriteria in tablecriteria
        # Identify the spine records that match the row on linkagecriteria.exactmatch
        criteriaid = linkagecriteria.id
        tableindex = criteriaid2index[criteriaid]
        hasmissing = constructkey!(criteriaid2key[criteriaid], row, tableindex.colnames)
        if hasmissing
            n_hasmissing += 1
            continue  # datarow[tableindex.colnames] includes a missing value
        end
        k = Tuple(criteriaid2key[criteriaid])
        !haskey(tableindex.index, k) && continue  # Row doesn't match any spine records on linkagecriteria.exactmatch
        candidate_indices = tableindex.index[k]

        # Identify the spine record that best matches the row (if it exists)
        spineid, i_spine = select_best_candidate(spine, candidate_indices, row, linkagecriteria.approxmatch)
        spineid == 0 && continue  # None of the candidates satisfy the approxmatch criteria

        # Create a link between the spine and the data
        nlinks += 1
        data[i_data, :spineID]    = spineid
        data[i_data, :criteriaID] = criteriaid
        break  # Row has been linked, no need to link on other criteria
    end
    nlinks, n_hasmissing
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

"""
Modified: spine.

Append the row to the spine and return the spineid of the new row.
"""
function append_row_to_spine!(spine, spine_primarykey, row, spinecols::Set{Symbol})
    d = Dict{Symbol, Union{Missing, String}}()
    for colname in spinecols
        d[colname] = hasproperty(row, colname) ? getproperty(row, colname) : missing
    end
    push!(spine, d)
    i = size(spine, 1)
    spine[i, :spineID] = hash(spine[i, spine_primarykey])
end

################################################################################
# Utils

function construct_software_versions_table()
    pkg_version = get_package_version()
    DataFrame(software=["Julia", "SpineBasedRecordLinkage.jl"], version=[VERSION, pkg_version])
end

function get_package_version()
    pkg_version = "unknown"
    srcdir = @__DIR__
    r      = findfirst("SpineBasedRecordLinkage.jl", srcdir)  # i:j
    pkgdir = srcdir[1:r[end]]
    f = open(joinpath(pkgdir, "Project.toml"))
    i = 0
    for line in eachline(f)
        i += 1
        if i == 4
            v = split(line, "=")  # line: version = "0.1.0"
            pkg_version = replace(strip(v[2]), "\"" => "")  # v0.1.0
            close(f)
            return pkg_version
        end
    end
end

function construct_criteria_table(cfg::LinkageConfig)
    colnames = (:criteriaID, :TableName, :ExactMatches, :ApproxMatches)
    coltypes = Tuple{Int, String, Dict{Symbol, Symbol}, Union{Missing, Vector{ApproxMatch}}}
    result   = NamedTuple{colnames, coltypes}[]
    for v in cfg.criteria
        for x in v
            am = isempty(x.approxmatch) ? missing : x.approxmatch
            r  = (criteriaID=x.id, TableName=x.tablename, ExactMatches=x.exactmatch, ApproxMatches=am)
            push!(result, r)
        end
    end
    DataFrame(result)
end

"Returns: Dict(criteriaid => TableIndex(spine, colnames))"
function construct_table_indexes(criteria::Vector{LinkageCriteria}, spine)
    # Create TableIndexes
    tmp = Dict{Int, TableIndex}()
    for linkagecriteria in criteria
        colnames = [spine_colname for (data_colname, spine_colname) in linkagecriteria.exactmatch]
        tmp[linkagecriteria.id] = TableIndex(spine, colnames)
    end

    # Replace spine colnames with data colnames
    # A hack to avoid converting from spine colnames to data colnames on each lookup
    result = Dict{Int, TableIndex}()
    for linkagecriteria in criteria
        data_colnames = [data_colname for (data_colname, spine_colname) in linkagecriteria.exactmatch]
        tableindex    = tmp[linkagecriteria.id]
        result[linkagecriteria.id] = TableIndex(spine, data_colnames, tableindex.index)
    end
    result
end

"Update the TableIndex with table[i, :]"
function update!(tableindex::TableIndex, table, i::Int)
    k = Tuple(table[i, tableindex.colnames])
    TableIndexes.update!(tableindex.index, k, i)
end

"Returns: true if row[colnames] includes a missing value."
function constructkey!(result::Vector{String}, row, colnames::Vector{Symbol})
    for (j, colname) in enumerate(colnames)  # Populate result with the row's values of tableindex.colnames (iteration.exactmatches)
        val = getproperty(row, colname)
        ismissing(val) && return true
        result[j] = val
    end
    false
end

end
