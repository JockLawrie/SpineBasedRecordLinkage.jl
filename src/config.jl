module config

export LinkageConfig, LinkagePass, FuzzyMatch

using Schemata
using YAML


################################################################################

"""
tablecolumn and personcolumn denote columns in the data and person tables respectively that being compared in the fuzzy match.
"""
struct FuzzyMatch
    tablecolumn::Symbol
    personcolumn::Symbol
    distancemetric::Symbol
    threshold::Float64
end


################################################################################

struct LinkagePass
    tablename::String
    exactmatchcols::Vector{Symbol}
    fuzzymatches::Vector{FuzzyMatch}
end


function LinkagePass(d::Dict)
    tablename      = d["tablename"]
    exactmatchcols = Symbol.(d["exactmatch_columns"])
    fuzzymatches   = FuzzyMatch[]
    if haskey(d, "fuzzymatches")
        fm_specs = d["fuzzymatches"]
        for x in fm_specs
            tablecol, personcol = Symbol.(x["columns"])
            distancemetric      = Symbol(x["distancemetric"])
            threshold           = x["threshold"]
            fm                  = FuzzyMatch(tablecol, personcol, distancemetric, threshold)
            push!(fuzzymatches, fm)
        end
    end
    LinkagePass(tablename, exactmatchcols, fuzzymatches)
end


################################################################################

struct LinkageConfig
    datadir::String
    person_schema::TableSchema
    linkmap_schema::TableSchema
    updatepersontable::Vector{String}  # Tables with which to update the Person table directly
    linkagepasses::Vector{LinkagePass}
end


function LinkageConfig(filename::String)
    d = YAML.load_file(filename)
    LinkageConfig(d)
end


function LinkageConfig(d::Dict)
    datadir           = d["datadir"]
    person_schema     = TableSchema(d["persontable"])
    linkmap_schema    = TableSchema(d["linkmap"])
    updatepersontable = haskey(d, "update_person_table") ? d["update_person_table"] : String[]
    if updatepersontable isa String
        updatepersontable = [updatepersontable]
    end
    linkagepasses = [LinkagePass(x) for x in d["linkage_passes"]]
    LinkageConfig(datadir, person_schema, linkmap_schema, updatepersontable, linkagepasses)
end

end
