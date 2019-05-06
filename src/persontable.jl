module persontable

using Base64
using CSV
using Dates
using DataFrames
using Logging
using Schemata

const data = Dict("fullpath"  => "",
                  "table"     => DataFrame(),
                  "colnames"  => Symbol[],  # names(table) excluding [:recordid, :personid, :recordstartdate]
                  "recordids" => Set{String}(),
                  "npeople"   => 0)


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
        data["fullpath"]  = fullpath
        data["table"]     = tbl
        data["colnames"]  = colnames[4:end]
        data["recordids"] = Set(tbl[:recordid])
        data["npeople"]   = length(unique(tbl[:personid]))
        @info "The Person table has $(size(tbl, 1)) rows."
    elseif isdir(dirname(fullpath))
        coltypes         = [Union{Missing, tblschema.columns[colname].eltyp} for colname in colnames]
        data["fullpath"] = fullpath
        data["table"]    = DataFrame(coltypes, colnames, 0)
        data["colnames"] = colnames[4:end]
        data["table"] |> CSV.write(fullpath; delim='\t')
        @info "The Person table has 0 rows."
    else
        @error "The file name for the Person table is not valid. Check that the containing directory exists."
    end
end


function updatetable!(filename::String)
    pt        = data["table"]
    fullpath  = data["fullpath"]
    recordids = data["recordids"]
    colnames  = data["colnames"]
    csvfile   = CSV.File(filename; delim='\t')
    rowkeys   = Set(csvfile.names)  # Column names in filename
    n_newrows = 0
    for row in csvfile
        d = Dict{Symbol, Any}(colname => in(colname, rowkeys) ? getproperty(row, colname) : missing for colname in colnames)
        row_appended = appendrow!(d, pt, recordids, colnames)
        if row_appended
            n_newrows += 1
        end
    end
    if n_newrows > 0
        pt |> CSV.write(fullpath; delim='\t', append=true)
        @info "$(n_newrows) new records added to the Person table."
    end
end


function appendrow!(d, persontbl, recordids, colnames)
    rid = recordid(d, colnames)
    in(rid, recordids) && return false # Person already exists in the Person table
    data["npeople"]    += 1
    d[:recordid]        = rid
    d[:personid]        = data["npeople"]
    d[:recordstartdate] = haskey(d, :recordstartdate) ? d[:recordstartdate] : missing
    push!(persontbl, d)
    push!(recordids, rid)
    true
end

recordid(v::Vector)   = base64encode(hash(v))

recordid(d, colnames) = recordid([d[colname] for colname in colnames])


end
