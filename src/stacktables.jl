module stacktables

export stack_tables

using CSV
using Dates
using DataFrames
using Logging

using ..utils

"""
Vertically stacks the tables specified by the filenames and stores the output in the specified output file.

Options:
- replace_outfile: Defaults to false. If true then the output file will be written over. Otherwise an error is raised.
- columns:         Either :intersection or :union. Defaults to :intersection.
                   If :intersection then the output table contains only the columns that are common to all the input files.
                   If :union then the output table contains the union of all columns in input files.
                   In the latter case, columns that are absent from a file are filled with `missing` in the result.
"""
function stack_tables(outfile::String, infiles...; replace_outfile::Bool=false, columns::Symbol=:intersection)
    @info "$(now()) Starting stack_tables"
    run_checks(outfile, replace_outfile, columns, infiles...)
    dlm      = utils.get_delimiter(outfile)
    colnames = get_colnames(columns, infiles)
    data     = DataFrame(fill(String, length(colnames)), colnames, 0)
    CSV.write(outfile, data; delim=dlm, append=false)
    data     = DataFrame(fill(String, length(colnames)), colnames, 1_000_000)
    n = 0
    for filename in infiles 
        @info "$(now()) Stacking $(filename)"
        csvrows   = CSV.Rows(filename; reusebuffer=true)
        colnames1 = intersect(colnames, csvrows.names)
        colnames2 = setdiff(colnames,   csvrows.names)
        i = 0
        for row in csvrows
            i += 1
            for colname in colnames1
                data[i, colname] = getproperty(row, colname)
            end
            for colname in colnames2
                data[i, colname] = missing
            end
            if i == 1_000_000  # If data is full, write to disk
                CSV.write(outfile, data; delim=dlm, append=true)
                n += i
                @info "$(now()) Exported $(n) rows to the output file."
                i = 0  # Reset the row number
            end
        end
        if i != 0  # Write remaining rows if they exist
            CSV.write(outfile, data[1:i, :]; append=true, delim=dlm)
            n += i
            @info "$(now()) Exported $(n) rows to the output file."
        end
    end
    @info "$(now()) Finished stack_tables"
end

function run_checks(outfile, replace_outfile, columns, infiles...)
    msgs = String[]
    if columns != :intersection && columns != :union
        push!(msgs, "The keyword argument columns must be either :intersection or :union.")
    end
    if isfile(outfile) && !replace_outfile
        msg = "The output file is not to be replaced.
               Either specify a different output file or set the keyword argument replace_outfile to true."
        push!(msgs, msg)
    end
    isdir(outfile) && push!(msgs, "The output file is a directory. Please specify a file.")
    if !isdir(dirname(outfile))
        msg = "The directory containing the output file does not exist. Please create it or specifiy a different output file."
        push!(msgs, msg)
    end
    for filename in infiles
        !isfile(filename) && push!(msgs, "$(filename) is not a file. Skipping to next next file.")
    end
    !isempty(msgs) && utils.earlyexit(msgs)
end

function get_colnames(columns::Symbol, infiles)
    colnames = Set{Symbol}()
    for filename in infiles
        csvrows = CSV.Rows(filename; reusebuffer=true)
        if columns == :intersection
            colnames = isempty(colnames) ? csvrows.names : intersect(colnames, csvrows.names)
            if isempty(colnames)
                msg = "The input tables have no common columns. The result will have no columns.
                       Either reduce the number of input files or set the keyword argument column_intersection to false."
                       utils.earlyexit(msg)
            end
        else  # columns == :union
            push!(colnames, csvrows.names)
        end
    end
    sort!([colname for colname in colnames])
end

end
