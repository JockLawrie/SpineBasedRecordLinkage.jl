module config

export LinkageConfig

using Dates
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
    !isfile(filename) && error("The data file for the $(name) table does not exist.")
    schemafile  = joinpath(dirs["schemata"], spec["schema"])
    !isfile(schemafile) && error("The file containing the schema for the $(name) table does not exist.")
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
    projectname::String
    configfile::String
    directories::Dict{String, String}
    spine::TableConfig
    tables::Dict{String, TableConfig}
    iterations::Vector{LinkageIteration}  # Grouped by tablename
end

function LinkageConfig(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d           = YAML.load_file(configfile)
    projectname = d["projectname"]
    dirs        = process_directories(d["directories"], projectname)
    spine       = TableConfig("spine", d["spine"], dirs)
    tables      = Dict(tablename => TableConfig(tablename, tableconfig, dirs) for (tablename, tableconfig) in d["tables"])

    # Iterations: retains original order but grouped by tablename for computational convenience
    idx2iter       = Dict(i => LinkageIteration(x) for (i, x) in enumerate(d["iterations"]))
    tablename2iter = Dict{String, Vector{LinkageIteration}}()
    tableorder     = String[]
    for i = 1:length(idx2iter)       # Construct tablename => [iteration1, ...]]
        x = idx2iter[i]
        tablename = x.tablename
        if !haskey(tablename2iter, tablename)
            tablename2iter[tablename] = LinkageIteration[]
            push!(tableorder, tablename)
        end
        push!(tablename2iter[tablename], x)
    end
    iterations = LinkageIteration[]  # Combine into 1 vector
    for tablename in tableorder
        for x in tablename2iter[tablename]
            push!(iterations, x)
        end
    end
    LinkageConfig(projectname, configfile, dirs, spine, tables, iterations)
end

function process_directories(d::Dict, projectname::String)
    result   = Dict{String, String}()
    required = ["lastrun", "thisrun", "spine", "schemata", "tables"]
    for name in required
        !haskey(d, name) && error("The directory for $(name) has not been specified.")
        if name == "lastrun" && d[name] == ""
            result["lastrun"] = d[name]
        elseif name == "thisrun"
            dttm   = replace(replace("$(now())"[1:(end - 4)], ":" => "."), "-" => ".")  # yyyy.mm.ddTHH.MM.SS
            result["thisrun"] = joinpath(d["thisrun"], "linkage-$(projectname)-$(dttm)")
        else
            !isdir(d[name]) && error("The specified directory for $(name) does not exist.")
            result[name] = d[name]
        end
    end
    result
end

end