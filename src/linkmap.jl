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
function link!(tablename::String, tablefile::String, exactmatchcols::Vector{Symbol}, fuzzymatches::Vector{FuzzyMatch})
    linkmap     = data["table"]
    linkmap     = view(linkmap, linkmap[:tablename] .== tablename, :)
    linked_tids = Set(linkmap[:tablerecordid])  # Records of tablename that are already linked
    for subdata in groupby(tbl, exactmatchcols)
        # Check if there are any records in the group that haven't yet been linked
        nlinked = 0
        for r in eachrow(subdata)
            if in(r[:recordid], linked_ids)      # Record has already been linked
                nlinked += 1
            end
        end
        nlinked == size(subdata, 1) && continue  # All records in the group have already been linked

        # Get candidate rows from the Person table using exact matching
        p = persontable.data["table"]
        for colname in exactmatchcols
            val = subdata[1, colname]
            if ismissing(val)
                p = view(p, ismissing.(p[colname]), :)
            else
                p = view(p, (.!ismissing.(p[colname])) .& (p[colname] .== val), :)
            end
        end
        size(p, 1) == 0 && continue  # There are no candidates

        # Reduce candidates further with fuzzy matching
        size(p, 1) != 1 && continue  # Number of candidate matches is not 1

        # Match each row in subdata to the candidate row
        for r in eachrow(subdata)
            tid = r[:recordid]
            in(tid,linked_ids) && continue
            rid = p[1, :recordid]
            x   = (tablename=tablename, tablerecordid=tid, personrecordid=rid)
            push!(data["table"], x)
        end
    end
end


end
