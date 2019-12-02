module utils

using CSV
using Dates
using DataFrames
using Logging
using Schemata

using ..TableIndexes
using ..config

function earlyexit(msgs::Vector{String})
    isempty(msgs) && return
    for msg in msgs
        @error msg
    end
    @warn "Exiting early."
    exit(1)
end

earlyexit(msg::String) = earlyexit([msg])

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

function construct_criteria_table(cfg::LinkageConfig)
    colnames = (:criteriaID, :TableName, :ExactMatches, :ApproxMatches)
    coltypes = Tuple{Int, String, Dict{Symbol, Symbol}, Union{Missing, Vector{ApproxMatch}}}
    result   = NamedTuple{colnames, coltypes}[]
    for v in cfg.criteria
        for x in v
            am = isempty(x.approxmatch) ? missing : x.approxmatch
            r  = (criteriaID=x.id, TableName=x.tablename, ExactMatches=x.exactmatch, ApproxMatches=am)
            push!(result, r)
        end
    end
    DataFrame(result)
end

function append_spineid!(spine::DataFrame, primarykey::Vector{Symbol})
    n = size(spine, 1)
    spine[!, :spineID] = missings(UInt64, n)
    for i = 1:n
        spine[i, :spineID] = hash(spine[i, primarykey])
    end
end

"Returns: Dict(criteriaid => TableIndex(spine, colnames))"
function construct_table_indexes(criteria::Vector{LinkageCriteria}, spine)
    # Create TableIndexes
    tmp = Dict{Int, TableIndex}()
    for linkagecriteria in criteria
        colnames = [spine_colname for (data_colname, spine_colname) in linkagecriteria.exactmatch]
        tmp[linkagecriteria.id] = TableIndex(spine, colnames)
    end

    # Replace spine colnames with data colnames
    # A hack to avoid converting from spine colnames to data colnames on each lookup
    result = Dict{Int, TableIndex}()
    for linkagecriteria in criteria
        data_colnames = [data_colname for (data_colname, spine_colname) in linkagecriteria.exactmatch]
        tableindex    = tmp[linkagecriteria.id]
        result[linkagecriteria.id] = TableIndex(spine, data_colnames, tableindex.index)
    end
    result
end

"Returns: true if row[colnames] includes a missing value."
function constructkey!(result::Vector{String}, row, colnames::Vector{Symbol})
    for (j, colname) in enumerate(colnames)  # Populate result with the row's values of tableindex.colnames (iteration.exactmatches)
        val = getproperty(row, colname)
        ismissing(val) && return true
        result[j] = val
    end
    false
end

function get_delimiter(filename::String)
    ext = lowercase(splitext(filename)[2])
    ext = occursin(".", ext) ? replace(ext, "." => "") : "csv"
    ext == "tsv" ? '\t' : ','
end

end
