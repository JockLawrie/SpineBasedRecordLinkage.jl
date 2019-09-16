module run

export run_linkage

using CSV
using DataFrames
using Dates
using Logging
using Schemata

using ..utils
using ..config
#using ..linkmap


function run_linkage(configfile::String)
    @info "$(now()) Configuring linkage run"
    cfg = LinkageConfig(configfile)

    @info "$(now()) Initialising linkage directory: $(cfg.directories["thisrun"])"
    d = cfg.directories["thisrun"]
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    cp(cfg.configfile, joinpath(d, "input", basename(configfile)))             # Copy config file to d/input
    pkg_version       = utils.get_package_version()
    software_versions = DataFrame(software=["Julia", "RecordLinkage.jl"], version=[VERSION, pkg_version])
    CSV.write(joinpath(d, "output", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/output
    iterations = DataFrame()
    iterations[!, :IterationID]  = [i for i = 1:length(cfg.iterations)]
    iterations[!, :TableName]    = [x.tablename      for x in cfg.iterations]
    iterations[!, :ExactMatches] = [x.exactmatchcols for x in cfg.iterations]
    iterations[!, :FuzzyMatches] = [x.fuzzymatches   for x in cfg.iterations]
    CSV.write(joinpath(d, "output", "Iterations.csv"), iterations; delim=',')  # Write iterations to d/output

    @info "$(now()) Importing spine"
    spine0 = DataFrame(CSV.File(cfg.spine.filename; limit=10))
    spine, issues = enforce_schema(spine0, cfg.spine.schema, false)
    if size(issues, 1) != 0
        CSV.write(joinpath(d, "output", "SpineIssues.tsv"), DataFrame(issues); delim='\t')
        #error("The spine does not match its schema. See $(joinpath(d, "output", "SpineIssues.tsv")) for details.")
    end
println("")
    for x in issues
        println(x)
    end
println("")
println(spine0[1:2, :])


    #=
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
    =#
    @info "$(now()) Finished linkage"
end


end
