module comparelinkageruns

export compare_linkage_runs

using CSV
using DataFrames
using Dates
using Logging

using ..utils

"""
Compares the results of the linkage runs in directory 1 and directory 2, and writes the comparison to outfile.

Typically the linkage run in directory 1 predates that in directory 2.

Results are compared table-wise.
For example, directory1/output/table_A.tsv is compared to directory2/output/table_A.tsv.

The output is a table with columns:

- tablename: A table in either linkage run (directory1/output/ or directory2/output/).
- status1:   The status of a record in the table in the 1st linkage run.
- status2:   The status of a record in the table in the 2nd linkage run.
- nrecords:  The number of records with status1 in the 1st linkage run and status2 in the 2nd linkage run.

The statuses are:

- nonexistent: The record doesn't exist in the linkage run.
- unlinked:    The record (in the table) exists and is not linked to the spine.
- linked with criteria ID X: The record exists and is linked to the spine by criteriaID X.
- existent:    The spine record exists in 1 linkage run but not the other.
"""
function compare_linkage_runs(directory1::String, directory2::String, outfile::String)
    @info "$(now()) Starting comparison of linkage runs."
    dlm = utils.get_delimiter(outfile)
    write_table_report(Dict(("LINKAGE RUNS", directory1, directory2) => missing), outfile, dlm, false)
    outdir1   = joinpath(directory1, "output")
    outdir2   = joinpath(directory2, "output")
    filelist1 = [x for x in readdir(outdir1) if isfile(joinpath(outdir1, x))]
    filelist2 = [x for x in readdir(outdir2) if isfile(joinpath(outdir2, x))]
    filelist  = Set(vcat(filelist1, filelist1))
    filelist  = sort!([x for x in filelist])
    for filename in filelist
        filename == "criteria.tsv" && continue
        tablename = filename == "spine_primarykey_and_spineid.tsv" ? "spine" : filename[1:(findfirst("_linked.tsv", filename)[1] - 1)]
        @info "$(now()) Reporting results for table $(tablename)"
        fullpath1 = joinpath(outdir1, filename)
        fullpath2 = joinpath(outdir2, filename)
        d         = Dict{Tuple{String, String, String}, Int}()  # (tablename, status1, status2) => nrecords
        if tablename == "spine"
            isfile(fullpath1)  && isfile(fullpath2)  && compare_spines!(d, fullpath1, fullpath2)
            isfile(fullpath1)  && !isfile(fullpath2) && report_solitary_spine!(d, fullpath1, 1)
            !isfile(fullpath1) && isfile(fullpath2)  && report_solitary_spine!(d, fullpath2, 2)
        else
            isfile(fullpath1)  && isfile(fullpath2)  && compare_nonspine_tables!(d, fullpath1, fullpath2, tablename)
            isfile(fullpath1)  && !isfile(fullpath2) && report_solitary_nonspine_table!(d, fullpath1, 1, tablename)
            !isfile(fullpath1) && isfile(fullpath2)  && report_solitary_nonspine_table!(d, fullpath2, 2, tablename)
        end
        write_table_report(d, outfile, dlm, true)
    end
    @info "$(now()) Finished comparison of linkage runs."
end

################################################################################
# Compare 2 existing tables.

function compare_spines!(result::Dict, fullpath1::String, fullpath2::String)
    spineids1 = get_set_of_values(fullpath1, :spineID)
    spineids2 = get_set_of_values(fullpath2, :spineID)
    s = intersect(spineids1, spineids2)
    k = ("spine", "existent", "existent")
    increment_value!(result, k, length(s))
    s = setdiff(spineids1, spineids2)
    k = ("spine", "existent", "nonexistent")
    increment_value!(result, k, length(s))
    s = setdiff(spineids2, spineids1)
    k = ("spine", "nonexistent", "existent")
    increment_value!(result, k, length(s))
end

function compare_nonspine_tables!(result::Dict, fullpath1::String, fullpath2::String, tablename::String)
    recordid2criteriaid_1 = construct_recordid2criteriaid(fullpath1)  # recordID => criteriaID if linked, -1 if not linked (nonexistent records have no recordID)
    recordid2criteriaid_2 = construct_recordid2criteriaid(fullpath2)
    for (recordid, criteriaid) in recordid2criteriaid_1
        status1 = linked_status(criteriaid)
        if haskey(recordid2criteriaid_2, recordid)
            c2 = recordid2criteriaid_2[recordid]
            status2 = linked_status(c2)
        else
            status2 = "nonexistent"
        end
        k = (tablename, status1, status2)
        increment_value!(result, k, 1)
    end
    for (recordid, criteriaid) in recordid2criteriaid_2
        haskey(recordid2criteriaid_1, recordid) && continue  # Already processed this recordid
        status2 = linked_status(criteriaid)
        k = (tablename, "nonexistent", status2)
        increment_value!(result, k, 1)
    end
end

################################################################################
# Report on a table that exists in only 1 linkage run.

function report_solitary_spine!(result::Dict{Tuple{String, String, String}, Int}, fullpath::String, status_number::Int)
    tablename = "spine"
    for row in CSV.Rows(fullpath; reusebuffer=true)
        active_status = "existent"
        k = status_number == 1 ? (tablename, active_status, "nonexistent") : (tablename, "nonexistent", active_status)
        increment_value!(result, k, 1)
    end
end

"""
Modified: result.

Reports on outdir/filename.
The tablenamne is derived from filename.
The specified status column (:statusX, where x == status_number) and the nrecords column are filled appropriately.
The other status column is filled with "nonexistent".
"""
function report_solitary_nonspine_table!(result::Dict{Tuple{String, String, String}, Int}, fullpath::String, status_number::Int, tablename::String)
    for row in CSV.Rows(fullpath; reusebuffer=true)
        criteriaID    = getproperty(row, :criteriaID)
        active_status = ismissing(criteriaID) ? "unlinked" : linked_status(criteriaID)
        k = status_number == 1 ? (tablename, active_status, "nonexistent") : (tablename, "nonexistent", active_status)
        increment_value!(result, k, 1)
    end
end

################################################################################
# Utils

linked_status(criteriaID) = criteriaID == "-1" ? "unlinked" : "linked with criteria ID $(criteriaID)"

"Returns: Set(values...) where values are in column colname of the table located at fullpath."
function get_set_of_values(fullpath::String, colname::Symbol)
    result = Set{String}()
    for row in CSV.Rows(fullpath; reusebuffer=true)
        push!(result, getproperty(row, colname))
    end
    result
end

"""
Modified: d.

Increments d[k] by n if it exists, else initialises d[k] to n.
"""
function increment_value!(d, k, n)
    if haskey(d, k)
        d[k] += n
    else
        d[k] = n
    end
end

"Returns: Dict(recordID => criteriaID if linked, -1 if not linked, ...). Nonexistent records have no recordID."
function construct_recordid2criteriaid(fullpath::String)
    result = Dict{UInt, String}()
    pk_colnames = construct_primarykey_colnames(fullpath)
    pk_values   = fill("", length(pk_colnames))
    for row in CSV.Rows(fullpath; reusebuffer=true)
        recordid = construct_recordid(row, pk_colnames, pk_values)
        haskey(result, recordid) && continue
        criteriaID = getproperty(row, :criteriaID)
        criteriaID = ismissing(criteriaID) ? "-1" : criteriaID
        result[recordid] = criteriaID
    end
    result
end

function construct_primarykey_colnames(fullpath::String)
    csvrows = CSV.Rows(fullpath; reusebuffer=true)
    result  = csvrows.names
    splice!(result, findfirst(isequal(:spineID),    result))
    splice!(result, findfirst(isequal(:criteriaID), result))
    result
end

function construct_recordid(row, pk_colnames::Vector{Symbol}, pk_values::Vector{String})
    j = 0
    for colname in pk_colnames
        j += 1
        pk_values[j] = getproperty(row, colname)
    end
    hash(pk_values)
end

function write_table_report(d::Dict, outfile::String, dlm::Char, apnd::Bool)
    result = DataFrame([String, String, String, Union{Int, Missing}], [:tablename, :status1, :status2, :nrecords], 0)
    for (k, v) in d
        push!(result, (tablename=k[1], status1=k[2], status2=k[3], nrecords=v))
    end
    sort!(result, (:tablename, :status1, :status2))
    CSV.write(outfile, result; delim=dlm, append=apnd)
end

end
