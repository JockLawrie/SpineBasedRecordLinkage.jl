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
        for tablename in cfg.updatepersontable
            filename = joinpath(cfg.inputdir, cfg.datatables[tablename])
            persontable.updatetable!(filename)
        end
    end

    @info "Initialising the Linkage Map"
    linkmap.init!(cfg)

    @info "Starting linkage passes"
    npass         = 0
    npasses       = size(cfg.linkagepasses, 1)
    prevtable     = ""
    linkmapfile   = ""
    tablefullpath = ""
    linked_tids   = Set{String}()
    newrows       = DataFrame(tablerecordid=fill("", 1_000_000), personrecordid=fill("", 1_000_000))
    for linkagepass in cfg.linkagepasses
        npass    += 1
        tablename = linkagepass.tablename
        @info "Linkage pass: $(npass) of $(npasses) (Table is $(tablename))"
        if tablename != prevtable
            linkmapfile   = joinpath(cfg.outputdir, "linkmap_$(tablename).tsv")
            linked_tids   = linkmap.get_linked_tids(linkmapfile)
            tablefullpath = joinpath(cfg.inputdir, cfg.datatables[tablename])
            prevtable     = tablename
        end
        exactmatchcols = linkagepass.exactmatchcols
        fuzzymatches   = linkagepass.fuzzymatches
        n_newlinks     = linkmap.link!(newrows, linked_tids, tablefullpath, exactmatchcols, fuzzymatches, linkmapfile)
        @info "$(n_newlinks) new records added to the link map for table $(tablename)."
    end
end


end
