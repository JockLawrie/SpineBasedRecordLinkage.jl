module combine_linkage_configurations

export combine_linkage_configs

using Dates
using Logging
using Schemata

using ..config
using ..utils


"""
Combine several linkage configs into 1 linkage config and store the result in outfile.

This function is used when constructing a spine from several tables.

The resulting linkage config file is obtained from the input configs as follows:

1. projectname: The 1st argument.
2. output_directory = The 2nd argument.
3. spine    = {datafile: spine_datafile, schemafile: spine_schemafile}
4. tables   = The union of the tables specified in the constructspine files.
5. criteria = The union of the criteria specified in the constructspine files.
"""
function combine_linkage_configs(projectname::String, output_directory::String, spine_datafile::String, spine_schemafile::String,
                                 outfile::String, linkagefiles...; replace_outfile::Bool=false)
    @info "$(now()) Starting combine_spine_construction_configs"
    utils.run_checks(outfile, replace_outfile, :intersection, linkagefiles...)
    !isdir(output_directory) && error("Output directory does not exist.")
    fname, ext  = splitext(outfile)
    outfile     = in(ext, Set([".yaml", ".yml"])) ? outfile : "$(fname).yml"
    spineschema = readschema(String(spine_schemafile))
    spine       = config.TableConfig(spine_datafile, spine_schemafile)
    tables      = Dict{String, config.TableConfig}()
    criteria    = Vector{LinkageCriteria}[]
    for linkagefile in linkagefiles
        @info "$(now()) Combining config $(linkagefile)"
        cfg = spine_construction_config(linkagefile)
        for (tablename, tableconfig) in cfg.tables
            tables[tablename] = tableconfig
        end
        v = LinkageCriteria[]
        for criterion in cfg.criteria[1]  # cfg.criteria[1] isa Vector{LinkageCriteria}
            !criterion_cols_are_in_spineschema(criterion, spineschema) && continue
            push!(v, criterion)
        end
        isempty(v) && continue
        push!(criteria, v)
    end
    cfg = LinkageCriteria(projectname, output_directory, spine, tables, criteria)
    writeconfig(outfile, cfg)
    @info "$(now()) Finished combine_spine_construction_configs"
end

"Returns true if all the columns in the criterion are in the spine schema. Else return false."
function criterion_cols_are_in_spineschema(criterion::LinkageCriteria, spineschema::TableSchema)
    schema_colnames = Set(spineschema.columnorder)
    for (data_colname, spine_colname) in criterion.exactmatch
        !in(data_colname,  schema_colnames) && return false
        !in(spine_colname, schema_colnames) && return false
    end
    for am in criterion.approxmatch
        !in(am.datacolumn,  schema_colnames) && return false
        !in(am.spinecolumn, schema_colnames) && return false
    end
    true
end

"Returns true if criteria already has d in it."
function already_have_criterion(d::Dict{String, Any}, criteria::Vector{Dict{String, Any}})
    for criterion in criteria
        d["tablename"]   != criterion["tablename"]   && continue
        if haskey(d, "exactmatch") && haskey(criterion, "exactmatch")
            d["exactmatch"]  != criterion["exactmatch"]  && continue
        end
        if haskey(d, "approxmatch") && haskey(criterion, "approxmatch")
            d["approxmatch"] != criterion["approxmatch"] && continue
        end
        return true
    end
    false
end

end
