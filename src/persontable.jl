module persontable

using CSV
using Dates
using DataFrames
using Logging
using Schemata

const data = Dict("fullpath" => "",
                  "table" => DataFrame(),
                  "colnames" => Symbol[],
                  "recordid2index" => Dict{UInt64, Int}(),
                  "npeople" => 0)


function init!(fullpath::String, tblschema::TableSchema)
    colnames = tblschema.col_order
    if isfile(fullpath)
        tbl = DataFrame(CSV.File(fullpath; delim='\t'))
        tbl, issues = enforce_schema(tbl, tblschema, false)
        if size(issues, 1) > 0
           issues_file = joinpath(dirname(fullpath), "person_issues.tsv")
           issues |> CSV.write(issues_file; delim='\t')
           @warn "There are some data issues. See $(issues_file) for details."
        end
        data["fullpath"] = fullpath
        data["table"]    = tbl
        data["colnames"] = colnames[4:end]
        data["recordid2index"] = [tbl[i, :recordid] => i for i = 1:size(tbl, 1)]
        data["npeople"]  = length(unique(tbl[:personid]))
        @info "The Person table has $(size(tbl, 1)) rows."
    elseif isdir(dirname(fullpath))
        touch(fullpath)  # Create file
        coltypes = [Union{Missing, tblschema.columns[colname].eltyp} for colname in colnames]
        data["fullpath"] = fullpath
        data["table"]    = DataFrame(coltypes, colnames, 0)
        data["colnames"] = colnames[4:end]
        @info "The Person table has 0 rows."
    else
        @error "File name is not valid."
    end
end


function appendrow!(r, tbl, id2index)
    rid = recordid(r)
    if haskey(id2index, rid)
        @warn "The Person table already has a record with ID $(rid)"
    else  # Complete the new record and append it to the table
        d            = Dict(colname => haskey(r, colname) ? r[colname] : missing for colname in data["colnames"])
        d[:recordid] = rid
        d[:personid] = newpersonid()
        d[:recordstartdate] = haskey(r, :recordstartdate) ? r[:recordstartdate] : missing
        push!(tbl, d)
        id2index[rid]    = size(tbl, 1)
        data["npeople"] += 1
    end
end

appendrow!(r) = appendrow!(r, data["table"], data["recordid2index"])


function updatetable!(tbl)
    pt     = data["table"]
    id2idx = data["recordid2index"]
    for r in eachrow(tbl)
        appendrow!(r, pt, id2idx)
    end
end


function write_persontable()
    tbl      = data["table"]
    fullpath = data["fullpath"]
    tbl |> CSV.write(fullpath; delim='\t')
end


npeople() = data["npeople"]


################################################################################
# Utils

newpersonid() = haskey(data["table"], :personid) ? npeople() + 1 : 1

recordid(r, colnames) = hash([r[colname] for colname in colnames])

recordid(r) = recordid(r, data["colnames"])

end
