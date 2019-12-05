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
    utils.run_checks(outfile, replace_outfile, columns, infiles...)
    dlm      = utils.get_delimiter(outfile)
    colnames = utils.get_colnames(columns, infiles)
    data     = DataFrame(fill(Union{Missing, String}, length(colnames)), colnames, 0)
    CSV.write(outfile, data; delim=dlm, append=false)
    data     = DataFrame(fill(Union{Missing, String}, length(colnames)), colnames, 1_000_000)
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

end
