println("Starting linkage")

using Pkg
Pkg.activate(".")

using RecordLinkage
configfile = ARGS[1]
run_linkage(configfile)