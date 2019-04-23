module run

export main

using DataFrames
using Logging
using Schemata

using ..persontable
using ..linkmap
using ..config


function main(filename::String, data::Dict{String, DataFrame})
    @info "Configuring linkage run"
    cfg = LinkageConfig(filename)

    @info "Initialising the Person table"
    @info "The fields that identify a person are\n:    $(persontable.data["colnames"])"
    persontable.init!(joinpath(cfg.datadir, "input", "person.tsv"), cfg.person_schema)

    @info "Initialising the Linkage Map"
    linkmap.init!(joinpath(cfg.datadir, "input", "linkmap.tsv"), cfg.linkmap_schema)

    @info "Updating the Person table"
    n = size(persontable.data["table"], 1)
    for tablename in cfg.updatepersontable
        persontable.updatetable!(data[tablename])
    end
    if size(persontable.data["table"], 1) > n  # New records have been added
	@info "Writing the Person table to disk"
	persontable.write_persontable()
    end

    @info "Starting linkage passes"
    n = 0
    for linkagepass in cfg.linkagepasses
        n += 1
        @info "Linkage pass: $(n)"
        tablename      = linkagepass.tablename
        exactmatchcols = linkagepass.exactmatch_columns
        fmc            = linkagepass.fuzzymatch_crtieria
        linkmap.link!(tablename, data, exactmatchcols, fmc)
    end

    @info "Writing results to disk"
    linkmap.write_linkmap()
end


end
