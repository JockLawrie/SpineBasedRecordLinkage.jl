module config

export LinkageConfig, LinkageCriteria, ApproxMatch, spine_construction_config

using Dates
using Schemata
using YAML

using ..distances

################################################################################

"""
datafile: The complete path to the data file.
schema:   Schema for the data.
"""
struct TableConfig
    datafile::String
    schema::TableSchema
end

function TableConfig(name::String, spec::Dict)
    !isfile(spec["datafile"])   && error("The data file for the $(name) table does not exist.")
    !isfile(spec["schemafile"]) && error("The schema file for the $(name) table does not exist.")
    schema_dict = YAML.load_file(spec["schemafile"])
    schema      = TableSchema(schema_dict)
    TableConfig(spec["datafile"], schema)
end

################################################################################

"""
An ApproxMatch specifies how a column in a data table can be compared to a column in the spine in an inexact (approximate) manner.
The comparison between two values, one from each column, is quantified as a distance between them.
A user-specified upper threshold is used to determine whether the 2 values are sufficiently similar (distance sufficiently small).
If so, the 2 values are deemed to match.

datacolumn: Data column name
spinecolumn: Spine column name
distancemetric: Name of distance metric
threshold: Acceptable upper bound on distance
"""
struct ApproxMatch
    datacolumn::Symbol
    spinecolumn::Symbol
    distancemetric::Symbol
    threshold::Float64

    function ApproxMatch(datacolname, spinecolname, distancemetric, threshold)
        ((threshold <= 0.0) || (threshold >= 1.0)) && error("Distance threshold must be between 0 and 1 (excluding 0 and 1).")
        if !haskey(distances.metrics, distancemetric)
            allowed_metrics = sort!(collect(keys(distances.metrics)))
            msg = "Unknown distance metric in fuzzy match criterion: $(distancemetric).\nMust be one of: $(allowed_metrics)"
            error(msg)
        end
        new(datacolname, spinecolname, distancemetric, threshold)
    end
end

function ApproxMatch(d::Dict)
    datacolname    = Symbol.(d["datacolumn"])
    spinecolname   = Symbol.(d["spinecolumn"])
    distancemetric = Symbol(d["distancemetric"])
    threshold      = d["threshold"]
    ApproxMatch(datacolname, spinecolname, distancemetric, threshold)
end

################################################################################

"""
tablename: Name of table to be linked to the spine.
exactmatch: Dict(data_column_name => spine_column_name, ...)
approxmatch::Vector{ApproxMatch}
"""
struct LinkageCriteria
    id::Int
    tablename::String
    exactmatch::Dict{Symbol, Symbol}
    approxmatch::Vector{ApproxMatch}
end

function LinkageCriteria(id::Int, d::Dict)
    tablename   = d["tablename"]
    exactmatch  = Dict(Symbol(k) => Symbol(v) for (k, v) in d["exactmatch"])
    approxmatch = haskey(d, "approxmatch") ? [ApproxMatch(amspec) for amspec in d["approxmatch"]] : ApproxMatch[]
    LinkageCriteria(id, tablename, exactmatch, approxmatch)
end

################################################################################

"""
output_directory: A directory created specifically for the linkage run. It contains all output.
spine:            TableConfig for the spine.
tables:           Dict of (tablename, TableConfig) pairs, with 1 pair for each data table.
criteria:         Vector{Vector{LinkageCriteria}}, where criteria[i] = [criteria for a table i].
"""
struct LinkageConfig
    projectname::String
    output_directory::String
    spine::TableConfig
    tables::Dict{String, TableConfig}
    criteria::Vector{Vector{LinkageCriteria}}  # iterations[i] = [iterations for a table i]
end

function LinkageConfig(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d = YAML.load_file(configfile)
    LinkageConfig(d, "linkage")
end

function LinkageConfig(d::Dict, purpose::String)
    (purpose != "linkage") && (purpose != "spineconstruction") && error("Config purpose is not recognised. Must be either linkage or spineconstruction.")
    projectname = d["projectname"]
    dttm        = "$(round(now(), Second(1)))"
    dttm        = replace(dttm, "-" => ".")
    dttm        = replace(dttm, ":" => ".")
    outdir      = joinpath(d["output_directory"], "$(purpose)-$(projectname)-$(dttm)")
    spine       = TableConfig("spine", d["spine"])
    tables      = Dict(tablename => TableConfig(tablename, tableconfig) for (tablename, tableconfig) in d["tables"])

    # Criteria: retains original order but grouped by tablename for computational convenience
    criteria      = Vector{LinkageCriteria}[]
    criterionid   = 0
    tablename2idx = Dict{String, Int}()
    for x in d["criteria"]
        tablename = x["tablename"]
        if !haskey(tablename2idx, tablename)
            push!(criteria, LinkageCriteria[])
            tablename2idx[tablename] = size(criteria, 1)
        end
        criterionid += 1
        push!(criteria[tablename2idx[tablename]], LinkageCriteria(criterionid, x))
    end
    LinkageConfig(projectname, outdir, spine, tables, criteria)
end

"""
Returns: A LinkageConfig with additional constraints necessary for the construction of a spine.

Specifically, since spine construction involves linking a table to itself, it requires:
1. Exactly 1 data table.
2. The data table's data file must be the same as the spine's data file.
3. The dta table's schema file must be the same as the spine's schema file.

This function performs these checks, and if they all pass a LinkageConfig is returned.
"""
function spine_construction_config(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d = YAML.load_file(configfile)
    length(d["tables"]) != 1 && error("Config for spine construction requires the specification of exactly 1 data table.")
    data_config = first(d["tables"])[2]  # first(d::Dict) = k=>v. tablename => Dict("datafile" => filename, "schemafile" => filename)
    d["spine"]["datafile"]   != data_config["datafile"]   && error("Config for spine construction requires the data table's data file to be the same as the spine's data file.")
    d["spine"]["schemafile"] != data_config["schemafile"] && error("Config for spine construction requires the data table's schema file to be the same as the spine's schema file.")
    LinkageConfig(d, "spineconstruction")
end

end
