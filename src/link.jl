module link

using CSV
using Dates
using DataFrames
using Logging
using Schemata

using ..config
using ..distances


function linktables(cfg::LinkageConfig, spine::DataFrame)
    for table_iterations in cfg.iterations
        # Make an output directory for the table
        tablename = table_iterations[1].tablename
        @info "$(now()) Starting linkage for table $(tablename)"
        tabledir  = joinpath(joinpath(cfg.directory, "output"), tablename)
        mkdir(tabledir)

        # Create an empty linkmap and store it in the output directory
        linkmap      = init_linkmap(cfg,  0)
        linkmap_file = joinpath(tabledir, "linkmap-$(tablename).tsv")
        CSV.write(linkmap_file, linkmap; delim='\t')

        # Create an empty output file for the data and store it in the output directory
        tableschema   = cfg.tables[tablename].schema
        data          = init_data(tableschema, 0)  # Columns are [:recordid, primarykey_columns...]
        table_outfile = joinpath(tabledir, "$(tablename).tsv")
        CSV.write(table_outfile, data; delim='\t')

        # Run the iterations over the data
        table_infile = cfg.tables[tablename].fullpath
        issues       = linktable(spine, linkmap_file, table_infile, table_outfile, tableschema, table_iterations)

        # Store any issues found when comparing the table's schema to its data
        isempty(issues) == 0 && continue
        issuesfile = joinpath(tabledir, "data_issues.tsv")
        @info "$(now()) Table didn't fully comply with the schema. See $(issuesfile) for details."
        CSV.write(issuesfile, DataFrame(issues); delim='\t')
    end
end


function linktable(spine::DataFrame, linkmap_file::String, table_infile::String, table_outfile::String,
                   tableschema::TableSchema, iterations::Vector{LinkageIteration})
    linkmap   = init_linkmap(cfg, 1_000_000)       # Process the data in batches of 1_000_000 rows
    data      = init_data(tableschema, 1_000_000)  # Process the data in batches of 1_000_000 rows
    i_linkmap = 0
    i_data    = 0
    nlinks    = 0
    row       = Dict{Symbol, String}()  # colname => value
    delim     = table_infile[(end - 2):end] == "csv" ? "," : "\t"
    idx2colname   = nothing
    colnames_done = false
    f = open(table_infile)
    for line in eachline(f)
        # Parse column names. Init row. Init parsedrow.
        if colnames_done!
            idx2colname   = Dict(j => Symbol(colname) for (j, colname) in enumerate(strip.(String.(split(line, sep)))))
            row           = missings(String, length(colname2idx))  # Place holder for cell values
            colnames_done = true
            continue
        end

        # Extract row from line
        extract_row!(row, line, delim, idx2colname)

        # Store primarykey columns
        i_data  += 1
        recordid = hash(line)
        data[i_data, :recordid] = recordid
        for colname in primarykey_colnames
            data[i_data, colname] = row[colname]
        end

        # Loop through each LinkageIteration
        for iteration in iterations
            # Identify the best matching spine record (if it exists)
            candidate_spineids  # Spine records that satisfy iteration.exactmatchcols
            isempty(candidate_spineids) && continue  # Row doesn't match any spine records on iteration.exactmatchcols
            spineid = select_best_candidate(row, spine, candidate_spineids, iteration.fuzzymatches)
            spineid == 0  && continue

            # Create a record in the linkmap
            i_linkmap += 1
            linkmap[i_linkmap, :spineid]     = bestcandidate
            linkmap[i_linkmap, :recordid]    = recordid
            linkmap[i_linkmap, :iterationid] = iteration.id
            break  # Row has been linked, no need to link on other criteria
        end

        # If data is full, write to disk
        if i_data == 1_000_000
            CSV.write(table_outfile, data; delim='\t', append=true)
            i_data = 0  # Reset the row number
        end

        # If linkmap is full, write to disk
        if i_linkmap == 1_000_000
            nlinks    = write_linkmap_to_disk(linkmap_file, linkmap, nlinks)
            i_linkmap = 0  # Reset the row number
        end
    end
    i_linkmap != 0 && write_linkmap_to_disk(linkmap_file, linkmap[1:i_linkmap, :], nlinks)
    close(f)
    issues
end


################################################################################
# Utils

function init_linkmap(cfg::LinkageConfig, n::Int)
    spineid_schema = get_columnschema(cfg.spine.schema, :spineid)
    colnames = [:spineid, :recordid, :iterationid]
    coltypes = [spineid_schema.datatype, Int, Int]
    DataFrame(coltypes, colnames, n)
end

function init_data(tableschema::TableSchema, n::Int)
    colnames = vcat(:recordid, tableschema.primarykey)
    coltypes = [UInt]
    for colname in tableschema.primarykey
        colschema = tableschema.columns[colname]
        push!(coltypes, colschema.datatype)
    end
    DataFrame(coltypes, colnames, n)
end

function write_linkmap_to_disk(linkmap_file, linkmap, nlinks)
    nlinks += size(linkmap, 1)
    CSV.write(linkmap_file, linkmap; delim='\t', append=true)
    @info "$(now()) $(nlinks) created"
    nlinks
end

function extract_row!(row::Dict{Symbol, String}, line::String, delim::String, idx2colname::Dict{Int, Symbol})
    i_start = 1
    colidx  = 0
    for j = 1:10_000
        r       = findnext(delim, line, i_start)  # r = i:i, where line[i] == '\t'
        i_end   = r[1] - 1
        colidx += 1
        row[idx2colname[colidx]] = String(line[i_start:i_end])
        i_start = i_end + 2
    end
end

################################################################################
# OLD

"""
Modified: newrows, linked_tids

Match subsets of rows of the input table to exactly one person in the Person table.

The subsets are determined by exactmatchcols and the fuzzymatch criteria.

For a given row of the data table:
- If there are no fuzzy match criteria and there is more than 1 candidate match then the row is left unlinked
- If there are fuzzy match criteria then the best candidate match (that with the smallest distance from the row) is selected

INPUT
- linked_tids: records of tablename that are already linked
"""
function link!(newrows, linked_tids::Set{String}, tablefullpath::String, exactmatchcols::Vector{Symbol}, fuzzymatches::Vector{FuzzyMatch}, linkmapfile::String)
    pt         = persontable.data["table"]
    dist       = fill(0.0, length(fuzzymatches))        # Work space for storing distances
    rid2idx    = construct_rid2idx(pt, exactmatchcols)  # recordid => Set(row indices), where recordid = recordid(d, exactmatchcols) and d is a row of the Person table.
    csvfile    = CSV.File(tablefullpath; delim='\t')
    rowkeys    = Set(csvfile.names)  # Column names in data table
    n_newlinks = 0
    i = 0
    for row in csvfile
        # Check whether row is already linked
        in(row.recordid, linked_tids) && continue

        # Init candidate matches: matches on exactmatchcols
        v   = [in(colname, rowkeys) ? getproperty(row, colname) : missing for colname in exactmatchcols]
        rid = persontable.recordid(v)
        !haskey(rid2idx, rid) && continue  # row has no candidate matches
        candidates = rid2idx[rid]

        # Select best candidate using fuzzy criteria
        bestcandidate = select_best_candidate(row, candidates, pt, fuzzymatches, dist)

        # Create a new record in the linkmap
        bestcandidate[:i] == 0 && continue  # No candidate satisfied the matching criteria
        i          += 1
        n_newlinks += 1
        newrows[i, :tablerecordid]  = row.recordid
        newrows[i, :personrecordid] = pt[bestcandidate[:i], :recordid]
        push!(linked_tids, row.recordid)

        # If newrows is full, write to disk
        rem(i, 1_000_000) > 0 && continue
        newrows |> CSV.write(linkmapfile; delim='\t', append=true)
        i = 0  # Reset the row number
    end
    if i != 0
        newrows[1:i, :] |> CSV.write(linkmapfile; delim='\t', append=true)
    end
    n_newlinks
end


function construct_rid2idx(pt, exactmatchcols)
    rid2idx = Dict{String, Set{Int}}()
    i = 0
    for r in eachrow(pt)
        i  += 1
        rid = persontable.recordid(r, exactmatchcols)
        if !haskey(rid2idx, rid)
            rid2idx[rid] = Set{Int}()
        end
        push!(rid2idx[rid], i)
    end
    rid2idx
end


function select_best_candidate(row, candidates::Set{Int}, pt, fuzzymatches::Vector{FuzzyMatch}, dist::Vector{Float64})
    isempty(fuzzymatches) && length(candidates) == 1 && return (i=pop!(deepcopy(candidates)), distance=99.0)
    isempty(fuzzymatches) && return (i=0, distance=99.0)
    bestcandidate = (i=0, distance=99.0)
    nfm = size(fuzzymatches, 1)
    for i in candidates
        # Compute distances
        i_is_candidate = true
        fill!(dist, 0.0)
        for j = 1:nfm
            fm      = fuzzymatches[j]
            dist[j] = compute_distance(fm.distancemetric, getproperty(row, fm.tablecolumn), pt[i, fm.personcolumn])
            if dist[j] > fm.threshold
                i_is_candidate = false
                break
            end
        end
        !i_is_candidate && continue

        # Compute overall distance and compare to best candidate
        d = overalldistance(dist)
        if d < bestcandidate[:distance]
            bestcandidate = (i=i, distance=d)
        end
    end
    bestcandidate
end


function get_linked_tids(linkmapfile::String)
    result  = Set{String}()
    csvfile = CSV.File(linkmapfile; delim='\t')
    for r in csvfile
        push!(result, getproperty(r, :tablerecordid))
    end
    result
end


end
