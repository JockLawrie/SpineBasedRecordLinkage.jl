module config

export ApproxMatch, LinkageCriteria, LinkageConfig

using Dates
using Schemata
using YAML

using ..distances

################################################################################

"""
datafile:   The complete path to the data file.
            If datafile is nothing, then the spine is constructed as part of the run_linkage function.
schemafile: The complete path to the schema file.
schema:     The TableSchema for the data, specified in the schema file.
"""
struct TableConfig
    datafile::Union{Nothing, String}
    schemafile::String
    schema::TableSchema

    function TableConfig(datafile, schemafile, schema)
        !isnothing(datafile) && !isfile(datafile) && error("The data file for the table does not exist.")
        !isfile(schemafile) && error("The schema file for the table does not exist.")
        new(datafile, schemafile, schema)
    end
end

function TableConfig(datafile, schemafile)
    d      = YAML.load_file(schemafile)
    schema = TableSchema(d)
    TableConfig(datafile, schemafile, schema)
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
projectname:      Project name.
description:      A description of the linkage run.
output_directory: A directory created specifically for the linkage run, identifiable by the project name and timestamp of the run. It contains all output.
spine:            TableConfig for the spine.
append_to_spine:  If true then unlinked rows are appended to the spine and linked.
                  If false then unlinked rows are left unlinked.
tables:           Dict of (tablename, TableConfig) pairs, with 1 pair for each data table.
criteria:         Vector{Vector{LinkageCriteria}}, where criteria[i] = [criteria for a table i].
"""
struct LinkageConfig
    projectname::String
    description::String
    output_directory::String
    spine::TableConfig
    append_to_spine::Bool
    tables::Dict{String, TableConfig}
    criteria::Vector{Vector{LinkageCriteria}}  # iterations[i] = [iterations for a table i]
end

function LinkageConfig(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d = YAML.load_file(configfile)
    LinkageConfig(d)
end

function LinkageConfig(d::Dict)
    projectname = d["projectname"]
    description = d["description"]
    dttm        = "$(round(now(), Second(1)))"
    dttm        = replace(dttm, "-" => ".")
    dttm        = replace(dttm, ":" => ".")
    outdir      = joinpath(d["output_directory"], "linkage-$(projectname)-$(dttm)")
    spinedata   = d["spine"]["datafile"] == "" ? nothing : d["spine"]["datafile"]
    spine       = TableConfig(spinedata, d["spine"]["schemafile"])
    append_to_spine = d["append_to_spine"]
    tables      = Dict(tablename => TableConfig(tableconfig["datafile"], tableconfig["schemafile"]) for (tablename, tableconfig) in d["tables"])

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
    LinkageConfig(projectname, description, outdir, spine, append_to_spine, tables, criteria)
end

end
