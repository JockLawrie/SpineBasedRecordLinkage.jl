module combine_spine_construction_configurations

export combine_spine_construction_configs

using Dates
using Logging
using Schemata
using YAML

using ..config
using ..utils


"""
Combine several spine construction configs into 1 spine construction config and store the result in outfile.

This function is used when constructing a spine from several tables.

The resulting linkage config file is obtained from the input configs as follows:

1. projectname = The 1st argument.
2. output_directory = The 2nd argument.
3. spine    = {datafile: spine_datafile, schemafile: spine_schemafile}
4. tables   = "spinedata" => {datafile: spine_datafile, schemafile: spine_schemafile}
5. criteria = the largest set of criteria from the linkage files that can be applied.
"""
function combine_spine_construction_configs(projectname::String, output_directory::String, spine_datafile::String, spine_schemafile::String,
                                            outfile::String, linkagefiles...; replace_outfile::Bool=false)
    @info "$(now()) Starting combine_spine_construction_configs"
    utils.run_checks(outfile, replace_outfile, :intersection, linkagefiles...)
    !isdir(output_directory) && error("Output directory does not exist.")
    fname, ext  = splitext(outfile)
    outfile     = in(ext, Set([".yaml", ".yml"])) ? outfile : "$(fname).yml"

    # HACK: Use Symbols so that the YAML writer doesn't quote the corresponding strings, which causes an error when the written YAML file is read back in.
    projectname      = Symbol(projectname)
    output_directory = Symbol(output_directory)
    spine_datafile   = Symbol(spine_datafile)
    spine_schemafile = Symbol(spine_schemafile)

    # Construct components
    criteria    = Dict{String, Any}[]
    tablename   = :spinedata  # Using a Symbol so that the YAML writer doesn't quote the string
    spineschema = readschema(String(spine_schemafile))
    for linkagefile in linkagefiles
        @info "$(now()) Combining config $(linkagefile)"
        cfg = spine_construction_config(linkagefile)
        for criterion in cfg.criteria[1]  # cfg.criteria isa Vector{LinkageCriteria}
            !criterion_cols_are_in_spineschema(criterion, spineschema) && continue
            d = Dict{String, Any}()
            d["tablename"]   = tablename
            d["exactmatch"]  = criterion.exactmatch  # Dict{Symbol, Symbol}
            if !isempty(criterion.approxmatch)
                d["approxmatch"] = [Dict(fieldname => getfield(obj, fieldname) for fieldname in fieldnames(typeof(obj))) for obj in criterion.approxmatch]
            end
            already_have_criterion(d, criteria) && continue
            push!(criteria, d)
        end
    end

    # Construct result
    result = Dict{String, Any}()
    result["projectname"] = projectname
    result["output_directory"] = output_directory
    result["spine"]    = Dict("datafile" => spine_datafile, "schemafile" => spine_schemafile)
    result["tables"]   = Dict(tablename  => Dict("datafile" => spine_datafile, "schemafile" => spine_schemafile))
    result["criteria"] = criteria
    YAML.write_file(outfile, result)
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
