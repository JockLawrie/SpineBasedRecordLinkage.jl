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
    run_linkage(cfg, configfile)
end

function run_linkage(cfg::LinkageConfig, configfile::String="")
    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    d = cfg.output_directory
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    if isempty(configfile)
        write_config(joinpath(d, "input", "config.yaml"), cfg)  # Write cfg to config.yaml
    else
        cp(configfile, joinpath(d, "input", basename(configfile)))  # Copy config file to d/input
    end
    software_versions = construct_software_versions_table()
    CSV.write(joinpath(d, "input", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/input
    criteria = construct_criteria_table(cfg)
    CSV.write(joinpath(d, "output", "criteria.tsv"), criteria; delim='\t')  # Write criteria to d/output

    # Init spine
    if isnothing(cfg.spine.datafile)
        @info "$(now()) Initialising spine"
        colnames = cfg.spine.schema.columnorder
        isnothing(findfirst(==(:EntityId), colnames)) && error("The spine has no EntityId column.")
        spine = DataFrame()
        for colname in colnames
            spine[!, colname] = colname == :EntityId ? UInt[] : Union{Missing, String}[]
        end
    else
        @info "$(now()) Importing spine"
        spine = DataFrame(CSV.File(cfg.spine.datafile; types=Union{Missing, String}))  # For performance only Strings are compared (avoids parsing values)
        spine[!, :EntityId] = [parse(UInt, x) for x in spine[!, :EntityId]]
    end

    # Init Links table
    links         = DataFrame(TableName=String[], EventId=UInt[], EntityId=UInt[], CriteriaId=Int[])
    nlinks        = 0  # Number of rows in the links table stored on disk
    links_outfile = joinpath(cfg.output_directory, "output", "links.tsv")
    CSV.write(links_outfile, links; delim='\t')
    n     = 1_000_000
    links = DataFrame(TableName=Vector{String}(undef, n), EventId=Vector{UInt}(undef, n), EntityId=Vector{UInt}(undef, n), CriteriaId=Vector{Int}(undef, n))

    # Do the linkage for each events table
    construct_entityid_from = cfg.construct_entityid_from
    for tablecriteria in cfg.criteria
        # Create an empty output file for the events table and store it in the output directory
        tablename = tablecriteria[1].tablename
        @info "$(now()) Starting linkage for table $(tablename)"
        events_schema  = cfg.tables[tablename].schema
        events         = init_events(events_schema, 0)  # Columns are [:EventId, primarykey_columns...]
        events_outfile = joinpath(cfg.output_directory, "output", "$(tablename)_primarykey_and_eventid.tsv")
        CSV.write(events_outfile, events; delim='\t')

        # Run the data through each linkage iteration
        events_infile = cfg.tables[tablename].datafile
        nlinks = link_table_to_events!(links, nlinks, links_outfile,
                                       spine, construct_entityid_from, cfg.append_to_spine,
                                       events_infile, events_outfile, events_schema, tablecriteria)
    end

    @info "$(now()) Writing spine to the output directory ($(format_number(size(spine, 1))) rows)"
    spine_outfile = joinpath(cfg.output_directory, "output", "spine.tsv")
    CSV.write(spine_outfile, spine; delim='\t')

    @info "$(now()) Finished linkage"
    cfg.output_directory
end

################################################################################
# Unexported

"Returns a DataFrame with n rows and columns [:EventId, events_schema.primarykey...]."
function init_events(events_schema::TableSchema, n::Int)
    result = DataFrame(EventId = Vector{Union{Missing, UInt}}(undef, n))
    for colname in events_schema.primarykey
        result[!, colname] = Vector{Union{Missing, String}}(undef, n)
    end
    result
end

"""
Modified: links, spine.

For each eventrow in the events table:
1. If the eventrow concerns an entity in the spine, append a row to links.
2. Else if append_to_spine is true, append a row to the spine and link it to the eventrow.
"""
function link_table_to_events!(links::DataFrame, nlinks::Int, links_outfile::String,
                               spine::DataFrame, construct_entityid_from::Vector{Symbol}, append_to_spine::Bool,
                               events_infile::String, events_outfile::String, events_schema::TableSchema, tablecriteria::Vector{LinkageCriteria})
    nlinks0    = nlinks  # Number of links due to other tables
    events     = init_events(events_schema, 1_000_000)  # Process the events in batches of 1_000_000 rows
    eventids   = Set{UInt}()  # Enables avoidance of duplicated EventIds
    buffer     = IOBuffer()   # For building EventIds
    i_events   = 0  # Number of this table's rows stored in-memory
    nevents    = 0  # Number of this table's rows stored on disk
    i_links    = 0  # Number of this table's rows stored in the in-memory links table
    tablename  = String(events_schema.name)
    n_criteria = length(tablecriteria)
    events_primarykey = events_schema.primarykey
    criteriaid2index  = construct_table_indexes(tablecriteria, spine)  # criteria.id => TableIndex(spine, colnames, index)
    criteriaid2key    = Dict(id => fill("", length(tableindex.colnames)) for (id, tableindex) in criteriaid2index)  # Place-holder for lookup keys
    for eventrow in CSV.Rows(events_infile; reusebuffer=true)
        # Store event primary key and construct eventid at the same time
        i_events += 1
        primarykey_is_incomplete = false
        print(buffer, tablename)  # Include the tablename in the eventid
        for colname in events_primarykey
            val = getproperty(eventrow, colname)
            if ismissing(val)
                primarykey_is_incomplete = true
                break
            end
            print(buffer, val)
            events[i_events, colname] = val
        end
        eventid = hash(String(take!(buffer)))

        # Check whether an EntityId can be constructed (requires non-missing values for columns in construct_entityid_from)
        cannot_construct_entityid = false
        if append_to_spine
            for colname in construct_entityid_from
                cannot_construct_entityid = ismissing(getproperty(eventrow, colname))
                cannot_construct_entityid && break
            end
        end

        # Roll back if primary key is incomplete, cannot construct EntityId or if EventId is a duplicate
        if primarykey_is_incomplete || cannot_construct_entityid || in(eventid, eventids)
            i_events -= 1  # Unstore the row
            continue
        end

        # Store EventId
        push!(eventids, eventid)
        events[i_events, :EventId] = eventid

        # Link the eventrow to the spine using the first LinkageCriteria that are satisfied (if any)
        # n_hasmissing = Number of criteria for which eventrow has missing data
        # If n_hasmissing == n_criteria then the entity in the eventrow cannot be appended to the spine because no criteria can be satisfied
        i_links, n_hasmissing, islinked = link_event_to_spine!(eventrow, eventid, spine, links, i_links, tablecriteria, criteriaid2index, criteriaid2key, tablename)

        # If eventrow is unlinked, append it to the spine, update the TableIndexes and link
        if append_to_spine && n_hasmissing < n_criteria && !islinked
            append_row_to_spine!(eventrow, spine, construct_entityid_from)  # Create a new spine record
            for linkagecriteria in tablecriteria
                tableindex = criteriaid2index[linkagecriteria.id]
                update!(tableindex, spine, size(spine, 1))  # Update the tableindex
            end
            i_links, n_hasmissing, islinked = link_event_to_spine!(eventrow, eventid, spine, links, i_links, tablecriteria, criteriaid2index, criteriaid2key, tablename)
        end

        # If events table is full, write to disk
        if i_events == 1_000_000
            CSV.write(events_outfile, events; delim='\t', append=true)
            nevents += i_events
            @info "$(now()) Exported $(div(nevents, 1_000_000))M rows of $(tablename)"
            i_events = 0  # Reset the eventrow number
        end

        # If links table is full, write to disk
        if i_links == 1_000_000
            CSV.write(links_outfile, links; delim='\t', append=true)
            nlinks += i_links
            @info "$(now()) Exported $(div(nlinks - nlinks0, 1_000_000))M links between $(tablename) and the spine"
            i_links = 0  # Reset the row number
        end
    end
    if i_events != 0  # Write remaining events if they exist
        CSV.write(events_outfile, view(events, 1:i_events, :); append=true, delim='\t')
        nevents += i_events
        @info "$(now()) Exported $(format_number(nevents)) rows of $(tablename)"
    end
    if i_links != 0  # Write remaining links if they exist
        CSV.write(links_outfile, view(links, 1:i_links, :); append=true, delim='\t')
        nlinks += i_links
        @info "$(now()) Exported $(format_number(nlinks - nlinks0)) links between $(tablename) and the spine"
    end
    nlinks
end

"""
Modified: events[i_events, :]

If possible, link the eventrow to the spine using the first LinkageCriteria that are satisfied (if any).
Record the linkage in the events table.
"""
function link_event_to_spine!(eventrow, eventid::UInt, spine, links::DataFrame, i_links::Int, tablecriteria::Vector{LinkageCriteria}, criteriaid2index, criteriaid2key, tablename)
    n_hasmissing = 0  # Number of criteria for which row has missing data
    islinked     = false
    for linkagecriteria in tablecriteria
        # Identify the spine records that match the row on linkagecriteria.exactmatch
        criteriaid = linkagecriteria.id
        tableindex = criteriaid2index[criteriaid]
        hasmissing = constructkey!(criteriaid2key[criteriaid], eventrow, tableindex.colnames)
        if hasmissing
            n_hasmissing += 1
            continue  # datarow[tableindex.colnames] includes a missing value
        end
        k = Tuple(criteriaid2key[criteriaid])
        !haskey(tableindex.index, k) && continue  # eventrow doesn't match any spine records on linkagecriteria.exactmatch
        candidate_indices = tableindex.index[k]

        # Identify the spine record that best matches the eventrow (if it exists)
        entityid, i_spine = select_best_candidate(spine, candidate_indices, eventrow, linkagecriteria.approxmatch)
        entityid == 0 && continue  # None of the candidates satisfy the approxmatch criteria

        # Create a link between the spine and the events data
        islinked = true
        i_links += 1
        links[i_links, :TableName]  = tablename
        links[i_links, :EventId]    = eventid
        links[i_links, :EntityId]   = entityid
        links[i_links, :CriteriaId] = criteriaid
        break  # eventrow has been linked, no need to link on other criteria
    end
    i_links, n_hasmissing, islinked
end

function select_best_candidate(spine, candidate_indices::Vector{Int}, row, approxmatches::Vector{ApproxMatch})
    if isempty(approxmatches)
        if length(candidate_indices) == 1
            i_spine = candidate_indices[1]
            return spine[i_spine, :EntityId], i_spine  # There is 1 candidate and no further selection criteria to be met
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
        result = spine[i_spine, :EntityId], i_spine
    end
    result
end

"""
Modified: spine.

Append the row to the spine and return the EntityId of the new row.
"""
function append_row_to_spine!(row, spine, construct_entityid_from::Vector{Symbol})
    push!(spine, (EntityId=UInt(0),), cols=:subset)  # Append row containing a dummy EntityId and missing values for all other columns
    i = size(spine, 1)
    spinecols = names(spine)
    for colname_str in spinecols
        colname = Symbol(colname_str)
        colname == :EntityId       && continue
        !hasproperty(row, colname) && continue
        spine[i, colname] = getproperty(row, colname)
    end
    spine[i, :EntityId] = hash(spine[i, construct_entityid_from])
end

################################################################################
# Utils

function construct_software_versions_table()
    pkg_version = get_package_version()
    DataFrame(software=["Julia", "SpineBasedRecordLinkage.jl"], version=[VERSION, pkg_version])
end

function get_package_version()
    pkg_version = "unknown"
    pkgdir = dirname(@__DIR__)
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

"Convert integer to string and insert commas for pretty printing."
function format_number(x::Int)
    s      = string(x)
    n      = length(s)
    lead   = rem(n, 3)  # Number of digits before the 1st comma
    lead   = lead == 0 ? 3 : lead
    result = s[1:lead]
    n_done = lead
    for i= 1:100  # Append remaining digits in groups of 3
        n_done == n && break
        s2 = ",$(s[(n_done + 1):(n_done + 3)])"
        result = result * s2
        n_done += 3
    end
    result
end

function construct_criteria_table(cfg::LinkageConfig)
    colnames = (:CriteriaId, :TableName, :ExactMatches, :ApproxMatches)
    coltypes = Tuple{Int, String, Dict{Symbol, Symbol}, Union{Missing, Vector{ApproxMatch}}}
    result   = NamedTuple{colnames, coltypes}[]
    for v in cfg.criteria
        for x in v
            am = isempty(x.approxmatch) ? missing : x.approxmatch
            r  = (CriteriaId=x.id, TableName=x.tablename, ExactMatches=x.exactmatch, ApproxMatches=am)
            push!(result, r)
        end
    end
    DataFrame(result)
end

"Returns: Dict(CriteriaId => TableIndex(spine, colnames))"
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
