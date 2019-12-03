module combineschemata

export combine_schemata

using CSV
using Dates
using DataFrames
using Logging
using YAML

using ..utils


"""
Combine the schemata of several tables into one schema and store the output in the specified output file.

Options:
- replace_outfile: Defaults to false. If true then the output file will be written over. Otherwise an error is raised.
- columns:         Either :intersection or :union. Defaults to :intersection.
                   If :intersection then the result contains only the columns that are common to all the input schemata.
                   If :union then the result contains the union of all columns in input schemata.
"""
function combine_schemata(outfile::String, infiles...; replace_outfile::Bool=false, columns::Symbol=:intersection)
end

end
