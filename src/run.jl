module run

export run_linkage_pipeline

using DataFrames
using Logging
using Schemata

using ..config
using ..persontable
using ..linkmap


"Run the entire data linkage pipeline."
function run_linkage_pipeline(d::Dict, run_preprocessing_stage::Function)
    @info "Starting linkage pipeline"
    for stage in d["stages"]
        if stage == "preprocessing"
            @info "Starting stage: preprocessing"
            run_preprocessing_stage(d["preprocessing"])
        elseif stage == "linkage"
            @info "Starting stage: linkage"
            run_linkage_stage(d)
        else
            error("Unknown stage: $(stage)")
        end
    end
    @info "Finished linkage pipeline"
end



function run_linkage_stage(d::Dict)
    @info "Configuring linkage run"
    cfg = LinkageConfig(d["linkage"], d["persontable"], d["linkmap"])

    @info "Initialising the Person table"
    persontable.init!(joinpath(cfg.outputdir, "person.tsv"), cfg.person_schema)
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
    linkmap.init!(joinpath(cfg.outputdir, "linkmap.tsv"), cfg.linkmap_schema)

    @info "Starting linkage passes"
    nlink0  = size(linkmap.data["table"], 1)
    nlink1  = nlink0
    nlink2  = nlink0
    npass   = 0
    npasses = size(cfg.linkagepasses, 1)
    for linkagepass in cfg.linkagepasses
        npass += 1
        @info "Linkage pass: $(npass) of $(npasses)"
        tablename      = linkagepass.tablename
        tablefullpath  = joinpath(cfg.inputdir, cfg.datatables[tablename])
        exactmatchcols = linkagepass.exactmatchcols
        fuzzymatches   = linkagepass.fuzzymatches
        linkmap.link!(tablename, tablefullpath, exactmatchcols, fuzzymatches)
        nlink2 = size(linkmap.data["table"], 1)
        @info "$(nlink2 - nlink1) new records added to the link map."
        nlink1 = nlink2
    end

    # Write linkmap to disk if there are new records
    if nlink2 > nlink0
        @info "$(nlink2 - nlink0) new records added to the link map. Writing to disk."
        linkmap.write_linkmap()
    end
end


end
