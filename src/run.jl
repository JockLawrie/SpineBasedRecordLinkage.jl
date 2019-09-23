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

    @info "$(now()) Initialising linkage directory: $(cfg.directory)"
    d = cfg.directory
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    cp(cfg.configfile, joinpath(d, "input", basename(configfile)))             # Copy config file to d/input
    software_versions = utils.construct_software_versions_table()
    CSV.write(joinpath(d, "output", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/output
    iterations = utils.construct_iterations_table(cfg)
    CSV.write(joinpath(d, "output", "Iterations.csv"), iterations; delim=',')  # Write iterations to d/output

    @info "$(now()) Importing spine"
    spine = DataFrame(CSV.File(cfg.spine.filename; type=String, limit=100))    # We only compare Strings...avoids parsing values
    # TODO: remove limit after testing

    # Do the linkage
    link.linktables(cfg, spine)

    @info "$(now()) Finished linkage"
end

end
