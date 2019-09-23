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
    length(cfg.spine.schema.primarykey) > 1 && error("The spine's primary key has more than 1 column. For computational efficiency please use a primary key with 1 column.")

    @info "$(now()) Initialising output directory: $(cfg.output_directory)"
    d = cfg.output_directory
    mkdir(d)
    mkdir(joinpath(d, "input"))
    mkdir(joinpath(d, "output"))
    cp(configfile, joinpath(d, "input", basename(configfile)))      # Copy config file to d/input
    software_versions = utils.construct_software_versions_table()
    CSV.write(joinpath(d, "output", "SoftwareVersions.csv"), software_versions; delim=',')  # Write software_versions to d/output
    iterations = utils.construct_iterations_table(cfg)
    CSV.write(joinpath(d, "output", "Iterations.csv"), iterations; delim=',')               # Write iterations to d/output

    @info "$(now()) Importing spine"
    spine = DataFrame(CSV.File(cfg.spine.datafile; type=String))    # We only compare Strings...avoids parsing values

    @info "$(now()) Parsing spine primary key"
    pk_colname = cfg.spine.schema.primarykey[1]
    parse_spine_primarykey!(cfg.spine.schema.columns[pk_colname], spine)

    # Do the linkage
    link.linktables(cfg, spine)

    @info "$(now()) Finished linkage"
end


"TODO: Include a version of thi function in Schemata.jl, delete this function."
function parse_spine_primarykey!(colschema::ColumnSchema, indata)
    n            = size(indata, 1)
    target_type  = colschema.datatype
    outdata      = missings(target_type, n)
    datacol      = indata[!, colschema.name]
    parser       = colschema.parser
    validvals    = colschema.validvalues
    invalid_vals = Set{Any}()
    for i = 1:n
        val = datacol[i]
        ismissing(val) && continue
        val isa String && val == "" && continue
        is_invalid = false
        if !(val isa target_type)  # Convert type
            try
                val = parse(parser, val)
            catch
                is_invalid = true
            end
        end
        # Value has correct type, now check that value is in the valid range
        if !is_invalid && !Schemata.handle_validvalues.value_is_valid(val, validvals)
            is_invalid = true
        end
        # Record invalid value
        if is_invalid && !set_invalid_to_missing && length(invalid_vals) < 5  # Record no more than 5 invlaid values
            push!(invalid_vals, val)
        end
        # Write valid value to outdata
        !(val isa target_type) && continue
        if !is_invalid || (is_invalid && !set_invalid_to_missing)
            outdata[i] = val
        end
    end
    if !isempty(invalid_vals)
        invalid_vals = [x for x in invalid_vals]  # Convert Set to Vector
        sort!(invalid_vals)
        @info "The spine's primary key has some invalid values: $(invalid_vals)"
    end
    indata[!, colschema.name] = outdata
end

end
