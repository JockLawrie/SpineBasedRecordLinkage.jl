module reporting

export summarise_linkage_run, compare_linkage_runs

using CSV
using DataFrames
using Dates
using Logging

"Summarise the results of a linkage run."
function summarise_linkage_run(directory1, outfile)
    @info "$(now()) Starting summary of linkage run."
    directory2 = replace("x$(rand())", "." => "")  # Dummy directory. Random so that it likely doesn't exist.
    for i = 1:1_000_000
        !isdir(directory2) && break  # Directory doesn't exist...break
        directory2 = replace("x$(rand())", "." => "")
    end
    report_on_linkage_runs(directory1, directory2, outfile, 1)
    dlm    = get_delimiter(outfile)
    report = DataFrame(CSV.File(outfile; delim=dlm))
    rename!(report, :status1 => :status)
    CSV.write(outfile, report; delim=dlm, append=false)
    @info "$(now()) Finished summary of linkage run."
end

"Summarise a row-by-row comparison of linkage runs."
function compare_linkage_runs(directory1, directory2, outfile)
    @info "$(now()) Starting comparison of linkage runs."
    report_on_linkage_runs(directory1, directory2, outfile, 2)
    @info "$(now()) Finished comparison of linkage runs."
end

"""
Report on 1 or 2 linkage runs.

If `n_linkage_runs` is 1, then the function reports on the linkage run that was written to directory 1.
If `n_linkage_runs` is 2, then the function compares the linkage runs in directory 1 and directory 2.
In both cases the resulting report is written to the specified output file.

When comparing 2 runs:

- Typically the linkage run in directory 1 predates that in directory 2.
- Results are compared table-wise. For example, directory1/output/table_A.tsv is compared to directory2/output/table_A.tsv.
- The output is a table with columns:
  - tablename: A table in either linkage run (directory1/output/ or directory2/output/).
  - status1:   The status of a record in the table in the 1st linkage run.
  - status2:   The status of a record in the table in the 2nd linkage run.
  - nrecords:  The number of records with status1 in the 1st linkage run and status2 in the 2nd linkage run.

The statuses are:

- nonexistent: The record doesn't exist in the linkage run.
- unlinked:    The record (in the table) exists and is not linked to the spine.
- linked with criteria ID X: The record exists and is linked to the spine by CriteriaId X.
- existent:    The spine record exists in 1 linkage run but not the other.

When reporting on just 1 linkage run the `status2` column is omitted from the result.
"""
function report_on_linkage_runs(directory1::String, directory2::String, outfile::String, n_linkage_runs::Int)
    run_checks(directory1, directory2, outfile, n_linkage_runs)
    dlm = get_delimiter(outfile)
    init_result(Dict(("LINKAGE RUNS", directory1, directory2) => missing), outfile, dlm, n_linkage_runs)
    outdir1   = joinpath(directory1, "output")
    outdir2   = joinpath(directory2, "output")
    filelist1 = [x for x in readdir(outdir1) if isfile(joinpath(outdir1, x))]
    filelist2 = n_linkage_runs == 2 ? [x for x in readdir(outdir2) if isfile(joinpath(outdir2, x))] : String[]
    filelist  = Set(vcat(filelist1, filelist1))
    filelist  = sort!([x for x in filelist])
    for filename in filelist
        filename == "criteria.tsv" && continue
        filename == "links.tsv"    && continue
        tablename = filename == "spine.tsv" ? "spine" : filename[1:(findfirst("_primarykey_and_eventid.tsv", filename)[1] - 1)]
        @info "$(now()) Reporting results for table $(tablename)"
        fullpath1 = joinpath(outdir1, filename)
        fullpath2 = joinpath(outdir2, filename)
        d         = Dict{Tuple{String, String, String}, Int}()  # (tablename, status1, status2) => nrecords
        if tablename == "spine"
            isfile(fullpath1)  && isfile(fullpath2)  && compare_spines!(d, fullpath1, fullpath2)
            isfile(fullpath1)  && !isfile(fullpath2) && report_solitary_spine!(d, fullpath1, 1)
            !isfile(fullpath1) && isfile(fullpath2)  && report_solitary_spine!(d, fullpath2, 2)
        else
            isfile(fullpath1)  && isfile(fullpath2)  && compare_event_tables!(d, fullpath1, fullpath2, tablename)
            isfile(fullpath1)  && !isfile(fullpath2) && report_solitary_event_table!(d, fullpath1, 1, tablename)
            !isfile(fullpath1) && isfile(fullpath2)  && report_solitary_event_table!(d, fullpath2, 2, tablename)
        end
        append_to_result(d, outfile, dlm, n_linkage_runs)
    end
end

################################################################################
# Compare 2 existing tables.

function compare_spines!(result::Dict, spinepath1::String, spinepath2::String)
    entityids1 = get_set_of_values(spinepath1, :EntityId)
    entityids2 = get_set_of_values(spinepath2, :EntityId)
    s = intersect(entityids1, entityids2)
    k = ("spine", "existent", "existent")
    increment_value!(result, k, length(s))
    s = setdiff(entityids1, entityids2)
    k = ("spine", "existent", "nonexistent")
    increment_value!(result, k, length(s))
    s = setdiff(entityids2, entityids1)
    k = ("spine", "nonexistent", "existent")
    increment_value!(result, k, length(s))
end

function compare_event_tables!(result::Dict, fullpath1::String, fullpath2::String, tablename::String)
    eventids1 = get_set_of_values(fullpath1, :EventId)
    eventids2 = get_set_of_values(fullpath2, :EventId)
    eventid2criteraid1 = construct_eventid2criteriaid(joinpath(dirname(fullpath1), "links.tsv"), tablename) # eventid => criteriaid for events in tablename
    eventid2criteraid2 = construct_eventid2criteriaid(joinpath(dirname(fullpath2), "links.tsv"), tablename)
    for eventid in eventids1
        status1 = haskey(eventid2criteraid1, eventid) ? linkage_status(eventid2criteraid1[eventid]) : "unlinked"
        status2 = linkage_status(eventid, eventids2, eventid2criteraid2)
        k       = (tablename, status1, status2)
        increment_value!(result, k, 1)
    end
    for eventid in eventids2
        in(eventid, eventids1) && continue  # Already processed this eventid
        status1 = linkage_status(eventid, eventids1, eventid2criteraid1)
        status2 = haskey(eventid2criteraid2, eventid) ? linkage_status(eventid2criteraid2[eventid]) : "unlinked"     
        k       = (tablename, status1, status2)
        increment_value!(result, k, 1)
    end
end

################################################################################
# Report on a table that exists in only 1 linkage run.

function report_solitary_spine!(result::Dict{Tuple{String, String, String}, Int}, spinepath::String, status_number)
    entityids = get_set_of_values(spinepath, :EntityId)
    n = length(entityids)
    k = status_number == 1 ? ("spine", "existent", "nonexistent") : ("spine", "nonexistent", "existent")
    increment_value!(result, k, n)
end

"""
Modified: result.

Reports on outdir/filename.
The tablenamne is derived from filename.
The specified status column (:statusX, where x == status_number) and the nrecords column are filled appropriately.
The other status column is filled with "nonexistent".
"""
function report_solitary_event_table!(result::Dict{Tuple{String, String, String}, Int}, fullpath::String, status_number::Int, tablename::String)
    eventids   = get_set_of_values(fullpath, :EventId)
    nevents    = length(eventids)
    nlinks     = 0
    links_path = joinpath(dirname(fullpath), "links.tsv")
    for row in CSV.Rows(links_path; reusebuffer=true)
        tname = getproperty(row, :TableName)
        tname != tablename && continue
        nlinks       += 1
        CriteriaId    = getproperty(row, :CriteriaId)
        active_status = linkage_status(CriteriaId)
        k = status_number == 1 ? (tablename, active_status, "nonexistent") : (tablename, "nonexistent", active_status)
        increment_value!(result, k, 1)
    end
    n_unlinked = nevents - nlinks
    k = status_number == 1 ? (tablename, "unlinked", "nonexistent") : (tablename, "unlinked", active_status)
    increment_value!(result, k, n_unlinked)
end

################################################################################
# Utils

linkage_status(criteriaid) = "linked with criteria ID $(criteriaid)"

function linkage_status(eventid, eventids, eventid2criteraid)
    haskey(eventid2criteraid, eventid) && return linkage_status(eventid2criteraid[eventid])  # Linked (exists in links)
    in(eventid, eventids) && return "unlinked"  # Unlinked (exists in events but not in links)
    "nonexistent"                               # Non-existent (does not exist in events)
end

function run_checks(directory1::String, directory2::String, outfile::String, n_linkage_runs::Int)
    msgs = String[]
    !isdir(directory1) && push!(msgs, "This directory does not exist: $(directory1)")
    isdir(outfile)     && push!(msgs, "The output file is a directory. Please specify a file.")
    if !isdir(dirname(outfile))
        msg = "The directory containing the output file does not exist. Please create it or specify a different output file."
        push!(msgs, msg)
    end
    if n_linkage_runs == 1      # Report on a single linkage run
    elseif n_linkage_runs == 2  # Compare 2 linkage runs
        !isdir(directory2) && push!(msgs, "This directory does not exist: $(directory2)")
    else
        push!(msgs, "n_linkage_runs must be 1 or 2 (currently $(n_linkage_runs))")
    end
    !isempty(msgs) && earlyexit(msgs)
end

function get_delimiter(filename::String)
    ext = lowercase(splitext(filename)[2])
    ext = occursin(".", ext) ? replace(ext, "." => "") : "csv"
    ext == "tsv" ? '\t' : ','
end

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
    n == 0 && return
    if haskey(d, k)
        d[k] += n
    else
        d[k] = n
    end
end

function construct_eventid2criteriaid(links_path::String, tablename::String)
    result = Dict{String, String}()
    for row in CSV.Rows(links_path; reusebuffer=true)
        getproperty(row, :TableName) != tablename && continue
        eventid = getproperty(row, :EventId)
        result[eventid] = getproperty(row, :CriteriaId)
    end
    result
end

function earlyexit(msgs::Vector{String})
    isempty(msgs) && return
    for msg in msgs
        @error msg
    end
    @warn "Exiting early."
    exit(1)
end

earlyexit(msg::String) = earlyexit([msg])

################################################################################
# Write result to disk

function init_result(d::Dict, outfile::String, dlm::Char, n_linkage_runs::Int)
    if n_linkage_runs == 1
        write_to_linkage_report(d, outfile, dlm, false)
    elseif n_linkage_runs == 2
        write_to_linkage_comparison(d, outfile, dlm, false)
    else
        error("n_linkage_runs should be not 1 or 2 (currently $(n_linkage_runs))")
    end
end

function append_to_result(d::Dict, outfile::String, dlm::Char, n_linkage_runs::Int)
    if n_linkage_runs == 1
        write_to_linkage_report(d, outfile, dlm, true)
    elseif n_linkage_runs == 2
        write_to_linkage_comparison(d, outfile, dlm, true)
    else
        error("n_linkage_runs should be not 1 or 2 (currently $(n_linkage_runs))")
    end
end

function write_to_linkage_comparison(d::Dict, outfile::String, dlm::Char, apnd::Bool)
    result = DataFrame(tablename=String[], status1=String[], status2=String[], nrecords=Union{Int, Missing}[])
    for (k, v) in d
        push!(result, (tablename=k[1], status1=k[2], status2=k[3], nrecords=v))
    end
    sort!(result, [:tablename, :status1, :status2])
    CSV.write(outfile, result; delim=dlm, append=apnd)
end

function write_to_linkage_report(d::Dict, outfile::String, dlm::Char, apnd::Bool)
    result = DataFrame(tablename=String[], status1=String[], nrecords=Union{Int, Missing}[])
    for (k, v) in d
        push!(result, (tablename=k[1], status1=k[2], nrecords=v))
    end
    sort!(result, [:tablename, :status1])
    CSV.write(outfile, result; delim=dlm, append=apnd)
end

end
