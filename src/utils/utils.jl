module utils

using CSV
using Dates
using DataFrames
using Logging
using Schemata

using ..TableIndexes
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

"Returns: Dict(iterationid => TableIndex(spine, colnames))"
function construct_table_indexes(iterations::Vector{LinkageIteration}, spine)
    # Create TableIndexes
    tmp = Dict{Int, TableIndex}()
    for iteration in iterations
        colnames = [spine_colname for (data_colname, spine_colname) in iteration.exactmatchcols]
        tmp[iteration.id] = TableIndex(spine, colnames)
    end

    # Replace spine colnames with data colnames
    # A hack to avoid converting from spine colnames to data colnames on each lookup
    result = Dict{Int, TableIndex}()
    for iteration in iterations
        data_colnames = [data_colname for (data_colname, spine_colname) in iteration.exactmatchcols]
        tableindex    = tmp[iteration.id]
        result[iteration.id] = TableIndex(spine, data_colnames, tableindex.index)
    end
    result
end

function init_data(tableschema::TableSchema, n::Int)
    colnames = vcat(:recordid, tableschema.primarykey)
    coltypes = vcat(UInt, fill(Union{Missing, String}, length(tableschema.primarykey)))
    DataFrame(coltypes, colnames, n)
end

"Returns: true if row[colnames] includes a missing value."
function constructkey!(result::Vector{String}, row, colnames::Vector{Symbol})
    for (j, colname) in enumerate(colnames)  # Populate result with the row's values of tableindex.colnames (iteration.exactmatchcols)
        val = getproperty(row, colname)
        ismissing(val) && return true
        result[j] = val
    end
    false
end

function write_linkmap_to_disk(linkmap_file, linkmap, nlinks, tablename)
    nlinks += size(linkmap, 1)
    CSV.write(linkmap_file, linkmap; delim='\t', append=true)
    @info "$(now()) $(nlinks) links created between the spine and table $(tablename)"
    nlinks
end

end