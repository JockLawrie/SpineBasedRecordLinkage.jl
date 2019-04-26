module run

export main

using DataFrames
using Logging
using Schemata

using ..config
using ..persontable
using ..linkmap


function main(d::Dict)
    @info "Configuring linkage run"
    cfg = LinkageConfig(d["linkage"], d["persontable"], d["linkmap"])

    @info "Initialising the Person table"
    persontable.init!(joinpath(cfg.inputdir, "person.tsv"), cfg.person_schema)
    @info "The fields that identify a person are:\n  $(persontable.data["colnames"])"

    # Update the Person table with new records if they exist
    if !isempty(cfg.updatepersontable)
        @info "Updating the Person table"
        n = size(persontable.data["table"], 1)
        for tablename in cfg.updatepersontable
            filename = joinpath(cfg.inputdir, cfg.datatables[tablename])
            persontable.updatetable!(filename)
        end

        # Write Person table to disk if there are new records
        n_new = size(persontable.data["table"], 1)
        if n_new > n
            @info "$(n_new - n) new records added to the Person table. Writing to disk."
            persontable.write_persontable()
        end
    end

    @info "Initialising the Linkage Map"
    linkmap.init!(joinpath(cfg.inputdir, "linkmap.tsv"), cfg.linkmap_schema)

    @info "Starting linkage passes"
    nlink = size(linkmap.data["table"], 1)
    npass = 0
    for linkagepass in cfg.linkagepasses
        npass += 1
        @info "Linkage pass: $(npass)"
        tablename      = linkagepass.tablename
        tablefullpath  = joinpath(cfg.inputdir, cfg.datatables[tablename])
        exactmatchcols = linkagepass.exactmatchcols
        fuzzymatches   = linkagepass.fuzzymatches
        linkmap.link!(tablename, tablefullpath, exactmatchcols, fuzzymatches)
    end

    # Write linkmap to disk if there are new records
    nlink_new = size(linkmap.data["table"], 1)
    if nlink_new > nlink
        @info "$(nlink_new - nlink) new records added to the link map. Writing to disk."
        linkmap.write_linkmap()
    end
end


end
