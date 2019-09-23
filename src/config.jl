module config

export LinkageConfig, LinkageIteration, FuzzyMatch

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
    d      = YAML.load_file(configfile)
    dttm   = "$(round(now(), Second(1)))"
    dttm   = replace(dttm, "-" => ".")
    dttm   = replace(dttm, ":" => ".")
    outdir = joinpath(d["output_directory"], "linkage-$(d["projectname"])-$(dttm)")
    spine  = TableConfig("spine", d["spine"])
    tables = Dict(tablename => TableConfig(tablename, tableconfig) for (tablename, tableconfig) in d["tables"])

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

end