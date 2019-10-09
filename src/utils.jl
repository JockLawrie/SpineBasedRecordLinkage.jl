module utils

using DataFrames

using ..config

function get_package_version()
    pkg_version = "unknown"
    srcdir = @__DIR__
    r      = findfirst("SpineBasedRecordLinkage.jl", srcdir)  # i:j
    pkgdir = srcdir[1:r[end]]
    f = open(joinpath(pkgdir, "Project.toml"))
    i = 0
    for line in eachline(f)
        i += 1
        if i == 4
            v = split(line, "=")  # line: version = "0.1.0"
            pkg_version = replace(strip(v[2]), "\"" => "")  # v0.1.0
            close(f)
            return pkg_version
        end
    end
end

function construct_software_versions_table()
    pkg_version = get_package_version()
    DataFrame(software=["Julia", "SpineBasedRecordLinkage.jl"], version=[VERSION, pkg_version])
end

function construct_iterations_table(cfg::LinkageConfig)
    colnames = (:IterationID, :TableName, :ExactMatches, :FuzzyMatches)
    coltypes = Tuple{Int, String, Dict{Symbol, Symbol}, Vector{FuzzyMatch}}
    result   = NamedTuple{colnames, coltypes}[]
    for v in cfg.iterations
        for x in v
            r = (IterationID=x.id, TableName=x.tablename, ExactMatches=x.exactmatchcols, FuzzyMatches=x.fuzzymatches)
            push!(result, r)
        end
    end
    DataFrame(result)
end

function append_spineid!(spine::DataFrame, primarykey::Vector{Symbol})
    n = size(spine, 1)
    spine[!, :spineid] = missings(UInt64, n)
    for i = 1:n
        spine[i, :spineid] = hash(spine[i, primarykey])
    end
end

end
