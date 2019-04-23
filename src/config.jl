module config

export LinkageConfig

using Schemata
using YAML

using ..persontable


#########################################################################################

struct LinkagePass
    tablename::String
    exactmatchcols::Vector{Symbol}
    fuzzymatch_criteria::Dict{String, Any}
end


function LinkagePass(d::Dict)
    tablename      = d["tablename"]
    exactmatchcols = d["exactmatch_columns"]
    exactmatchcols = exactmatchcols == "all" ? persontable.data["colnames"] : Symbol.(exactmatchcols)
    x              = d["fuzzymatch_criteria"]
    fuzzy_criteria = Dict("colnames" => Symbol.(x["columns"]), "distancemetric" => Symbol(x["distancemetric"]), "threshold" => x["threshold"])
    LinkagePass(tablename, exactmatchcols, fuzzy_criteria)
end


#########################################################################################

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
