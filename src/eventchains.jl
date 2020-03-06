module eventchains

export construct_event_chains

using CSV
using DataFrames
using Dates
using Logging
using YAML

function construct_event_chains(configfile::String)
    @info "$(now()) Configuring event chain construction"
    cfg = ChainsConfig(configfile)

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    outdir = cfg.output_directory
    mkdir(outdir)
    mkdir(joinpath(outdir, "input"))
    mkdir(joinpath(outdir, "output"))
    cp(configfile, joinpath(outdir, "input", basename(configfile)))  # Copy config file to d/input

    @info "$(now()) Importing the links table"
    types = Dict(:TableName => String, :EntityId => UInt, :EventId => UInt, :CriteriaId => Int)
    links = DataFrame(CSV.File(cfg.links_table; types=types))
    select!(links, Not(:CriteriaId))  # Drop CriteriaId column

    @info "$(now()) Initialising the event_chains table"
    # Append columns: DateTime, EventTag, ChainId
    lowerbound = cfg.time_window.lowerbound
    upperbound = cfg.time_window.upperbound
    entityid2chainid2dttm = augment_links!(links, cfg)  # EntityId => ChainId => DateTime
    oldchainid2newchainid = merge_chains(entityid2chainid2dttm, lowerbound, upperbound)  # oldChainId => newChainId. Merge chains if they overlap.

    @info "$(now()) Constructing event chains"
    # Include events in chains if they satisfy inclusion criteria
    chainid2chainname = augment_chains!(links, entityid2chainid2dttm, oldchainid2newchainid, lowerbound, upperbound)

    @info "$(now()) Exporting event chain definitions"
    x = sort!([(ChainId=k, ChainName=v) for (k,v) in chainid2chainname], by=(x) -> x.ChainId)
    CSV.write(joinpath(outdir, "output", "event_chain_definitions.tsv"), x)

    @info "$(now()) Exporting event chains"
    x = view(links, links[!, :ChainId] .> 0, [:ChainId, :EventId])
    CSV.write(joinpath(outdir, "output", "event_chains.tsv"), x)

    @info "$(now()) Finished constructing event chains. Results are stored at:\n    $(outdir)"
    outdir
end

################################################################################
# Config

struct ChainsConfig
    projectname::String
    description::String
    output_directory::String
    links_table::String                 # filename
    event_tables::Dict{String, String}  # tablename => filename
    tags::Dict{String, Symbol}          # tablename => colname
    timestamps::Dict{String, Symbol}    # tablename => colname
    tag_of_interest::Dict{String, String}  # keys: tablename, column, value
    time_window::Dict{Symbol, Any}      # keys: unit, lowerbound, upperbound
end

function ChainsConfig(configfile::String)
    !isfile(configfile) && error("The config file $(configfile) does not exist.")
    d = YAML.load_file(configfile)
    ChainsConfig(d)
end

const registered_units = Dict("day" => Day)

function ChainsConfig(d::Dict)
    projectname  = d["projectname"]
    description  = d["description"]
    dttm         = "$(round(now(), Second(1)))"
    dttm         = replace(dttm, "-" => ".")
    dttm         = replace(dttm, ":" => ".")
    outdir       = joinpath(d["output_directory"], "eventchains-$(projectname)-$(dttm)")
    links_table  = d["links_table"]
    event_tables = d["event_tables"]
    tags         = Dict{String, Symbol}(tablename => Symbol(tag) for (tablename,tag) in d["tags"])
    timestamps   = Dict{String, Symbol}(tablename => Symbol(tag) for (tablename,tag) in d["timestamps"])
    tag_of_interest = d["criteria"]["tag_of_interest"]
    tw    = d["criteria"]["time_window"]  # Example: "-7 to 30 days"
    tw    = split(tw, " ")
    lb    = parse(Int, tw[1])
    ub    = parse(Int, tw[3])
    units = registered_units[tw[4][1:3]]
    timewindow = Dict{Symbol, Any}(:unit => units, :lowerbound => units(lb), :upperbound => units(ub))
    ChainsConfig(projectname, description, outdir, links_table, event_tables, tags, timestamps, tag_of_interest, timewindow)
end

################################################################################

function augment_links!(links::DataFrame, cfg::ChainsConfig)
    result = Dict{UInt, Dict{Int, DateTime}}()  # EntityId => ChainId => DateTime
    n      = size(links, 1)
    links[!, :DateTime] = missings(DateTime, n)
    links[!, :EventTag] = missings(String,   n)
    links[!, :ChainId]  = missings(Int,      n)
    chainid             = 0
    tablename           = ""
    eventid2dttm_tag    = ""  # EventId => (datetime, tag)
    table_of_interest   = cfg.tag_of_interest["tablename"]
    tag_of_interest     = cfg.tag_of_interest["value"]
    for i = 1:n
        # Update table if necessary
        new_tablename = links[i, :TableName]
        if new_tablename != tablename
            tablename = new_tablename
            eventid2dttm_tag = construct_eventid2dttm_tag(cfg, tablename)
        end

        # Populate new columns of links table
        dttm, tag = eventid2dttm_tag[links[i, :EventId]]
        links[i, :DateTime] = dttm
        links[i, :EventTag] = tag
        if tablename == table_of_interest && tag == tag_of_interest
            chainid += 1
            links[i, :ChainId] = chainid
        end

        # Populate result
        entityid = links[i, :EntityId]
        if !haskey(result, entityid)
            result[entityid] = Dict{Int, DateTime}()
        end
        result[entityid][chainid] = dttm
    end
    result
end

function construct_eventid2dttm_tag(cfg, tablename::String)
    result       = Dict{UInt, Tuple{DateTime, String}}()
    dttm_colname = cfg.timestamps[tablename]
    tag_colname  = cfg.tags[tablename]
    for row in CSV.Rows(cfg.event_tables[tablename]; use_mmap=true, reusebuffer=true)
        eventid = parse(UInt, getproperty(row, :EventId))
        dttm    = DateTime(getproperty(row, dttm_colname))
        tag     = getproperty(row, tag_colname)
        result[eventid] = (dttm, tag)
    end
    result
end

function merge_chains(entityid2chainid2dttm, lowerbound, upperbound)
    result = Dict{Int, Int}()  # oldChainId => newChainId. Merge chains if they overlap.
    for (entityid, chainid2dttm) in entityid2chainid2dttm
        for (oldchainid, dttm) in chainid2dttm
            newchainid = get_chainid(result, dttm, lowerbound, upperbound)
            result[oldchainid] = newchainid == 0 ? oldchainid : newchainid
        end
    end
    for (oldchainid, newchainid) in result
        oldchainid != newchainid && continue
        delete!(result, oldchainid)
    end
    result
end

function augment_chains!(links, entityid2chainid2dttm, oldchainid2newchainid, lowerbound, upperbound)
    result = Dict{Int, String}()  # ChainId => ChainName
    sort!(links, (:EntityId, :DateTime))
    n = size(links, 1)
    for i = 1:n
        entityid = links[i, :EntityId]
        !haskey(entityid2chainid2dttm, entityid) && continue  # Entity has no chains of interest
        chainid = links[i, :ChainId]
        chainid2dttm = entityid2chainid2dttm[entityid] 
        chainid = ismissing(chainid) ? get_chainid(chainid2dttm, links[i, :DateTime], lowerbound, upperbound) : chainid
        chainid == 0 && continue  # The event is not part of any chain
        chainid = haskey(oldchainid2newchainid, chainid) ? oldchainid2newchainid[chainid] : chainid
        links[i, :ChainId] = chainid
        event_tag = "$(links[i, :TableName]).$(links[i, :EventTag])"  # tablename.tag
        result[chainid] = haskey(result, chainid) ? "$(result[chainid]) -> $(event_tag)" : event_tag
    end
    result
end

"""
Returns the first ChainId encountered for which ts-lowerbound <= dttm <= ts+upperbound,
where ts is the timestamp of the event of interest in the chain.

If no such chain exists the function returns 0.
"""
function get_chainid(chainid2dttm::Dict{Int, DateTime}, dttm::DateTime, lowerbound::T, upperbound::T) where {T <: Period}
    for (chainid, ts) in chain2dttm
        dttm < ts - lowerbound && continue
        dttm > ts + upperbound && continue
        return chainid
    end
    0
end

end
