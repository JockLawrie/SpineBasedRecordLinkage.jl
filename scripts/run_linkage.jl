println("Starting linkage")

using Pkg
Pkg.activate(".")

using SpineBasedRecordLinkage
configfile = ARGS[1]
run_linkage(configfile)