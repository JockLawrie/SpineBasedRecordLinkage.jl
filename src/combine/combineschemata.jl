module combineschemata

export combine_schemata

using Dates
using Logging
using Schemata

using ..utils

"""
Combine the schemata of several tables into one schema and store the output in the specified output file.

Options:
- replace_outfile: Defaults to false. If true then the output file will be written over. Otherwise an error is raised.
- columns:         Either :intersection or :union. Defaults to :intersection.
                   If :intersection then the result contains only the columns that are common to all the input schemata.
                   If :union then the result contains the union of all columns in input schemata.

The resulting schema is obtained from the input schemata as follows:

1. name = name1-name2-...-namek, where name1 is the name of the first input schema (table).
2. description = "Schema obtained from the columns common to {name}", where name is as above.
3. primarykey = If columns == :intersection, the set of common columns (except :spineID), else the union of all columns.
4. columns = List of ColumnSchema of columns in the intersection or union (specified by the columns keyword argument) of the columns of the input tables.
5. intrarow_constraints = Function[] for now, because it is hard to select an appropriate subset of constraints for the column selection.
"""
function combine_schemata(outfile::String, schemafiles...; replace_outfile::Bool=false, columns::Symbol=:intersection)
    @info "$(now()) Starting combine_schemata"
    utils.run_checks(outfile, replace_outfile, columns, schemafiles...)
    fname, ext = splitext(outfile)
    outfile    = in(ext, Set([".yaml", ".yml"])) ? outfile : "$(fname).yml"
    combined_tableschema = combine_table_schemata(columns, schemafiles...)
    writeschema(outfile, combined_tableschema)
    @info "$(now()) Finished combine_schemata"
end

function combine_table_schemata(columns::Symbol, schemafiles...)
    name = ""
    j    = 0
    colname2colschema = Dict{Symbol, ColumnSchema}()
    for schemafile in schemafiles
        @info "$(now()) Combining schema $(schemafile)"
        j += 1
        tableschema = readschema(schemafile)
        name        = isempty(name) ? "$(tableschema.name)" : "$(name)-$(tableschema.name)"
        colname2colschema_j = Dict{Symbol, ColumnSchema}()
        for (colname, colschema) in tableschema.colname2colschema
            colschema.isrequired = true  # Primary key colums require this
            if j == 1
                colname2colschema[colname] = colschema
            else
                colname2colschema_j[colname] = colschema
            end
        end
        j == 1 && continue
        colnames     = Set(collect(keys(colname2colschema)))
        new_colnames = Set(collect(keys(colname2colschema_j)))
        colnames     = columns == :intersection ? intersect(colnames, new_colnames) : union(colnames. new_colnames)
        for (colname, colschema) in colname2colschema  # Delete k-v pairs if k is not in colnames
            in(colname, colnames) && continue  # Retain this column
            delete!(colname2colschema, colname)
        end
        for colname in colnames
            haskey(colname2colschema, colname) && continue  # Already have this column
            colname2colschema[colname] = colname2colschema_j[colname]
        end
    end
    description = "Schema obtained from the columns in the $(columns) of these tables: $(replace(name, "-" => ", "))."
    primarykey  = sort!([k for (k, v) in colname2colschema])
    colschemata = sort!([v for (k, v) in colname2colschema], by=(x) -> x.name)
    TableSchema(Symbol(name), description, colschemata, primarykey)
end

end
