println("Starting linkage")

using Pkg
Pkg.activate(".")

configfile = ARGS[1]
if !isfile(configfile)
    error("Config file does not exist. Please include the full path and check for typos.")
end

using YAML
cfg = YAML.load_file(configfile)

using RecordLinkage
run_linkage(cfg)