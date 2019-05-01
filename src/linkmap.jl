module linkmap

using CSV
using Dates
using DataFrames
using Logging
using Schemata

using ..config
using ..persontable
using ..distances

const data = Dict("fullpath" => "", "table" => DataFrame())


function init!(fullpath::String, tblschema::TableSchema)
    if isfile(fullpath)
        tbl = DataFrame(CSV.File(fullpath; delim='\t'))
        tbl, issues = enforce_schema(tbl, tblschema, false)
        if size(issues, 1) > 0
            issues_file = joinpath(dirname(fullpath), "linkmap_issues.tsv")
            issues |> CSV.write(issues_file; delim='\t')
            @warn "There are some data issues. See $(issues_file) for details."
        end
        data["fullpath"] = fullpath
        data["table"]    = tbl
        @info "The linkage map has $(size(tbl, 1)) rows."
    elseif isdir(dirname(fullpath))
        touch(fullpath)  # Create file
        colnames         = tblschema.col_order
        coltypes         = [Union{Missing, tblschema.columns[colname].eltyp} for colname in colnames]
        data["fullpath"] = fullpath
        data["table"]    = DataFrame(coltypes, colnames, 0)
        @info "The linkage map has 0 rows."
    else
        @error "File name is not valid."
    end
end


"""
Match subsets of rows of the input table to exactly one person in the Person table.

The subsets are determined by exactmatchcols and the fuzzymatch criteria.

For a given row of the data table:
- If there are no fuzzy match criteria and there is more than 1 candidate match then the row is left unlinked
- If there are fuzzy match criteria then the best candidate match (that with the smallest distance from the row) is selected
"""
function link!(tablename::String, tablefullpath::String, exactmatchcols::Vector{Symbol}, fuzzymatches::Vector{FuzzyMatch})
    pt          = persontable.data["table"]
    lmap        = data["table"]
    linked_tids = Set(view(lmap, lmap[:tablename] .== tablename, :tablerecordid))  # records of tablename that are already linked
    dist        = fill(0.0, length(fuzzymatches))        # Work space for storing distances
    rid2idx     = construct_rid2idx(pt, exactmatchcols)  # recordid => Set(row indices), where recordid = recordid(d, exactmatchcols) and d is a row of the Person table.
    csvfile     = CSV.File(tablefullpath; delim='\t')
    rowkeys     = Set(csvfile.names)  # Column names in data table
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
        tid    = row.recordid
        rid    = pt[bestcandidate[:i], :recordid]
        newrow = (tablename=tablename, tablerecordid=tid, personrecordid=rid)
        push!(lmap, newrow)
    end
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


function write_linkmap()
    tbl      = data["table"]
    fullpath = data["fullpath"]
    tbl |> CSV.write(fullpath; delim='\t')
end


end
