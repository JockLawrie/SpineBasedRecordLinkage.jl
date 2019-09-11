module config

export LinkageConfig

using Schemata
using YAML

using ..distances

################################################################################

struct TableConfig
    filename::String
    schema::TableSchema
end

function TableConfig(name::String, spec::Dict, dirs::Dict)
    datadir     = name == "spine" ? dirs["spine"] : dirs["tables"]
    filename    = joinpath(datadir,          spec["filename"])
    schemafile  = joinpath(dirs["schemata"], spec["schema"])
    schema_dict = YAML.load_file(schemafile)
    schema      = TableSchema(schema_dict)
    TableConfig(filename, schema)
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
    tablename::String
    exactmatchcols::Dict{Symbol, Symbol}
    fuzzymatches::Vector{FuzzyMatch}
end

function LinkageIteration(d::Dict)
    tablename      = d["tablename"]
    exactmatchcols = Dict(Symbol(k) => Symbol(v) for (k, v) in d["exactmatch_columns"])
    fuzzymatches   = haskey(d, "fuzzymatches") ? [FuzzyMatch(fmspec) for fmspec in d["fuzzymatches"]] : FuzzyMatch[]
    LinkageIteration(tablename, exactmatchcols, fuzzymatches)
end


################################################################################

struct LinkageConfig
    directories::Dict{String, String}
    spine::TableConfig
    tables::Dict{String, TableConfig}
    iterations::Vector{LinkageIteration}
end

function LinkageConfig(d::Dict)
    dirs       = process_directories(d["directories"])
    spine      = TableConfig("spine", d["spine"], dirs)
    tables     = Dict(tablename => TableConfig(tablename, tableconfig, dirs) for (tablename, tableconfig) in d["tables"])
    iterations = [LinkageIteration(x) for x in d["iterations"]]
    LinkageConfig(dirs, spine, tables, iterations)
end

function process_directories(d::Dict)
    result   = Dict{String, String}()
    required = ["schemata", "spine", "linkmap", "tables", "output"]
    for name in required
        !haskey(d, name) && error("The directory for $(name) has not been specified.")
        !isdir(d[name])  && error("The specified directory for $(name) does not exist.")
        result[name] = d[name]
    end
    result
end

end