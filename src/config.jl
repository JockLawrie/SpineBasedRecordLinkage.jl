module config

export LinkageConfig, LinkagePass, FuzzyMatch

using Schemata


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
    inputdir::String
    outputdir::String
    datatables::Dict{String, String}   # tablename => filename
    person_schema::TableSchema
    linkmap_schema::TableSchema
    updatepersontable::Vector{String}  # Tables with which to update the Person table directly
    linkagepasses::Vector{LinkagePass}
end


function LinkageConfig(linkage::Dict, persontbl::Dict, lmap::Dict)
    inputdir          = linkage["inputdir"]
    outputdir         = linkage["outputdir"]
    datatables        = linkage["datatables"]
    person_schema     = TableSchema(persontbl)
    linkmap_schema    = TableSchema(lmap)
    updatepersontable = haskey(linkage, "update_person_table") ? linkage["update_person_table"] : String[]
    if updatepersontable isa String
        updatepersontable = [updatepersontable]
    end
    linkagepasses = [LinkagePass(x) for x in linkage["linkage_passes"]]
    LinkageConfig(inputdir, outputdir, datatables, person_schema, linkmap_schema, updatepersontable, linkagepasses)
end

end
