module linkmap

using CSV
using Dates
using DataFrames
using Logging
using Schemata

using ..config
using ..persontable
using ..distances


function init!(cfg::LinkageConfig)
    lmschema    = cfg.linkmap_schema
    tables_done = Set{String}()
    for linkagepass in cfg.linkagepasses
        tablename = linkagepass.tablename
        in(tablename, tables_done) && continue
        push!(tables_done, tablename)
        linkmapfile = joinpath(cfg.outputdir, "linkmap_$(tablename).tsv")
        if isfile(linkmapfile)
            # TODO: Check that the linkmap matches the schema
        else
            colnames = lmschema.col_order
            coltypes = [Union{Missing, lmschema.columns[colname].eltyp} for colname in colnames]
            lmap     = DataFrame(coltypes, colnames, 0)
            lmap |> CSV.write(linkmapfile; delim='\t')
        end
    end
end


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
