module config

export LinkageConfig, LinkageIteration, FuzzyMatch, spine_construction_config

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
A FuzzyMatch specifies how a column in a data table can be compared to a column in the spine in an inexact manner.
The comparison between two values, one from each column, is quantified as a distance between them.
A user-specified upper threshold is used to determine whether the 2 values are sufficiently similar (distance sufficianetly small).
If so, the 2 values are deemed to match.

datacolumn: Data column name
spinecolumn: Spine column name
distancemetric: Name of distance metric
threshold: Acceptable upper bound on distance
"""
struct FuzzyMatch
    datacolumn::Symbol
    spinecolumn::Symbol
    distancemetric::Symbol
    threshold::Float64

    function FuzzyMatch(datacol, spinecol, distancemetric, threshold)
        ((threshold <= 0.0) || (threshold >= 1.0)) && error("Distance threshold must be between 0 and 1 (excluding 0 and 1).")
        if !haskey(distances.metrics, distancemetric)
            allowed_metrics = sort!(collect(keys(distances.metrics)))
            msg = "Unknown distance metric in fuzzy match criterion: $(distancemetric).\nMust be one of: $(allowed_metrics)"
            error(msg)
        end
        new(datacol, spinecol, distancemetric, threshold)
    end
end

function FuzzyMatch(d::Dict)
    datacol, spinecol = Symbol.(d["columns"])
    distancemetric    = Symbol(d["distancemetric"])
    threshold         = d["threshold"]
    FuzzyMatch(datacol, spinecol, distancemetric, threshold)
end

################################################################################

"""
tablename: Name of table to be linked to the spine.
exactmatchcols: Dict of data_column_name => spine_column_name
fuzzymatches::Vector{FuzzyMatch}
"""
struct LinkageIteration
    id::Int
    tablename::String
    exactmatchcols::Dict{Symbol, Symbol}
    fuzzymatches::Vector{FuzzyMatch}
end

function LinkageIteration(id::Int, d::Dict)
    tablename      = d["tablename"]
    exactmatchcols = Dict(Symbol(k) => Symbol(v) for (k, v) in d["exactmatch_columns"])
    fuzzymatches   = haskey(d, "fuzzymatches") ? [FuzzyMatch(fmspec) for fmspec in d["fuzzymatches"]] : FuzzyMatch[]
    LinkageIteration(id, tablename, exactmatchcols, fuzzymatches)
end

################################################################################

"""
output_directory: A directory created specifically for the linkage run. It contains all output.
spine:            TableConfig for the spine.
tables:           Dict of (tablename, TableConfig) pairs, with 1 pair for each data table.
iterations:       Vector{Vector{LinkageIteration}}, where iterations[i] = [iterations for a table i].
"""
struct LinkageConfig
    output_directory::String
    spine::TableConfig
    tables::Dict{String, TableConfig}
    iterations::Vector{Vector{LinkageIteration}}  # iterations[i] = [iterations for a table i]
end

function LinkageConfig(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d = YAML.load_file(configfile)
    LinkageConfig(d, "linkage")
end

function LinkageConfig(d::Dict, purpose::String)
    (purpose != "linkage") && (purpose != "spine-construction") && error("Config purpose is not recognised. Must be either linkage or spine-construction.")
    dttm   = "$(round(now(), Second(1)))"
    dttm   = replace(dttm, "-" => ".")
    dttm   = replace(dttm, ":" => ".")
    outdir = joinpath(d["output_directory"], "$(purpose)-$(d["projectname"])-$(dttm)")
    spine  = TableConfig("spine", d["spine"])
    tables = Dict(tablename => TableConfig(tablename, tableconfig) for (tablename, tableconfig) in d["tables"])
    length(spine.schema.primarykey) > 1 && error("The spine's primary key has more than 1 column. For computational efficiency please use a primary key with 1 column.")

    # Iterations: retains original order but grouped by tablename for computational convenience
    iterations    = Vector{LinkageIteration}[]
    iterationid   = 0
    tablename2idx = Dict{String, Int}()
    for x in d["iterations"]
        tablename = x["tablename"]
        if !haskey(tablename2idx, tablename)
            push!(iterations, LinkageIteration[])
            tablename2idx[tablename] = size(iterations, 1)
        end
        iterationid += 1
        push!(iterations[tablename2idx[tablename]], LinkageIteration(iterationid, x))
    end
    LinkageConfig(outdir, spine, tables, iterations)
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
    LinkageConfig(d, "spine-construction")
end

end