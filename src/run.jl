module runlinkage

export run_linkage

using CSV
using DataFrames
using Dates
using Logging
using Schemata

using ..utils
using ..config
using ..link


function run_linkage(configfile::String)
    @info "$(now()) Configuring linkage run"
    cfg = LinkageConfig(configfile)

    @info "$(now()) Initialising linkage directory: $(cfg.directories["thisrun"])"
    d = cfg.directories["thisrun"]
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    cp(cfg.configfile, joinpath(d, "input", basename(configfile)))             # Copy config file to d/input
    software_versions = utils.construct_software_versions_table()
    CSV.write(joinpath(d, "output", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/output
    iterations = utils.construct_iterations_table(cfg)
    CSV.write(joinpath(d, "output", "Iterations.csv"), iterations; delim=',')  # Write iterations to d/output

    @info "$(now()) Importing spine"
    spine0 = DataFrame(CSV.File(cfg.spine.filename; limit=10))
    spine, issues = enforce_schema(spine0, cfg.spine.schema, false)
    if size(issues, 1) != 0
        CSV.write(joinpath(d, "output", "SpineIssues.tsv"), DataFrame(issues); delim='\t')
        @error "The spine does not match its schema. See $(joinpath(d, "output", "SpineIssues.tsv")) for details."
    end

    @info "$(now()) Initialising the linkage maps"
    #link.init_linkmaps(cfg)

    @info "$(now()) Starting the linkage iterations"
    #link.run_iterations()

    @info "$(now()) Finished linkage"
end

end