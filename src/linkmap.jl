module linkmap

using CSV
using Dates
using DataFrames
using Logging
using Schemata

using ..config
using ..persontable

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


function write_linkmap()
    tbl      = data["table"]
    fullpath = data["fullpath"]
    tbl |> CSV.write(fullpath; delim='\t')
end


"""
Match subsets of rows of the input table to exactly one person in the Person table.

The subsets are determined by exactmatchcols and fuzzymatch_criteria.

A subset of rows is matched if and only if there is exactly 1 candidate match in the Person table.
"""
function link!(tablename::String, tablefullpath::String, exactmatchcols::Vector{Symbol}, fuzzymatches::Vector{FuzzyMatch})
    # Init
    pt          = persontable.data["table"]
    lmap        = data["table"]
    linked_tids = Set(view(lmap, lmap[:tablename] .== tablename, :tablerecordid))  # records of tablename that are already linked

    # Construct recordid => Set(row indices), where recordid = recordid(d, exactmatchcols) and d is a row of the Person table.
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

    # For each row of tablename, identify and rank the rows of the Person table that are candidates for matching
    csvfile = CSV.File(tablefullpath; delim='\t')
    rowkeys = Set(csvfile.names)  # Column names in filename
    for row in csvfile
        # Check whether row is already linked
        in(row.recordid, linked_tids) && continue

        # Init candidates: matches on exactmatchcols
        v   = [in(colname, rowkeys) ? getproperty(row, colname) : missing for colname in exactmatchcols]
        rid = persontable.recordid(v)
        !haskey(rid2idx, rid) && continue  # row has no match candidates
        candidates = rid2idx[rid]

        # Select best candidate: best match using fuzzy criteria
        #=
        bestcandidate = (i=0, distance=9999.0)
        dist = fill(0.0, length(fuzzymatches))  # Work space for storing distances
        nfm  = size(fuzzymatches, 1)
        for i in candidates
            # Compute distances
            i_is_candidate = true
            fill!(dist, 0.0)
            for j = 1:nfm
                fm      = fuzzymatches[j]
                dist[j] = compute_distance(row, pt[i, :], fm)
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
        =#

        # Create a new record in the linkmap
length(candidates) != 1 && continue
bestcandidate = (i=pop!(deepcopy(candidates)), distance=9999.0)
        tid    = row.recordid
        rid    = pt[bestcandidate[:i], :recordid]
        newrow = (tablename=tablename, tablerecordid=tid, personrecordid=rid)
        push!(lmap, newrow)
    end
end


end
