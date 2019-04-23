module linkmap

using CSV
using Dates
using DataFrames
using Logging
using Schemata

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
        @info "The linkage map has $(size(tbl, 0)) rows."
    elseif isdir(dirname(fullpath))
        touch(fullpath)  # Create file
        colnames = tblschema.col_order
        coltypes = [Union{Missing, tblschema.columns[colname].eltyp} for colname in colnames]
        data["fullpath"] = fullpath
        data["table"]    = DataFrame(coltypes, colnames, 0)
        @info "The linkage map has 0 rows."
    else
        @error "File name is not valid."
    end
end


#=
function appendrow!(tblname, r)
    rid      = persontable.recordid(r)
    id2index = persontable.data["recordid2index"]
    if haskey(id2index, rid)
        x = (tablename=tblname, tablerecordid=r[:recordid], personrecordid=rid)
        push!(data["table"], x)
    end
end
=#


function write_linkmap()
    tbl      = data["table"]
    fullpath = data["fullpath"]
    tbl |> CSV.write(fullpath; delim='\t')
end


################################################################################
# Link

function link_all_fields!(tblname::String, tbl)
    id2index = persontable.data["recordid2index"]
    for r in eachrow(tbl)
        rid = persontable.recordid(r)
        !haskey(id2index, rid) && continue  # Person is not already in the person table
        x = (tablename=tblname, tablerecordid=r[:recordid], personrecordid=rid)
        push!(data["table"], x)
    end
end


"""
Matches if there is exactly 1 candidate row in the Person table.
"""
function link_some_fields!(tblname::String, tbl, colnames::Vector{Symbol})
    # Init rows that have complete data for colnames
    v = persontable.data["table"]
    for colname in colnames
        v = view(v, .!ismissing.(v[colname]), :)
    end

    # Subset the linkmap
    linkmap     = data["table"]
    linkmap     = view(linkmap, linkmap[:tablename] .== tblname, :)
    linkmap_ids = Set(linkmap[:tablerecordid])

    # Match
    for subdata in groupby(tbl, colnames)
        # Get candidate rows from the Person table
        v2 = v
        row_is_complete = true
        for colname in colnames
            val = subdata[1, colname]
            if ismissing(val)
                row_is_complete = false
                break
            end
            v2 = view(v2, v2[colname] .== val, :)
        end
        !row_is_complete && continue
        size(v2, 1) != 1 && continue  # Number of candidate rows in the Person table is not 1

        # Match each row in subdata to the candidate row
        id2index = persontable.data["recordid2index"]
        for r in eachrow(subdata)
            id = r[:recordid]
            in(id, linkmap_ids) && continue     # r is already in the linkmap
            rid = persontable.recordid(v2[1, :])
            !haskey(id2index, rid) && continue  # Person is not already in the person table
            x = (tablename=tblname, tablerecordid=id, personrecordid=rid)
            push!(data["table"], x)
        end
    end
end


end
