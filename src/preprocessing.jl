"""
Utility functions for preprocessing.
"""
module preprocessing


export addressparts


const roadtypes = Dict("st => street", "rd" => "road", "ave" => "avenue", "av" => "avenue", "dr" => "drive", "drv" => "drive", "crt" => "court",
                       "pwy" => "parkway", "hwy" => "highway", "fwy" => "freeway")


"""
Returns: Dict containing address parts, extracted from the input string.

Example:
  INPUT:  Holmes House, 221b Baker St, Melbourne, VIC, 3000

  OUTPUT: Dict(:buildingname => "holmes house",
               :streetnumber => "221b", :streetname => "baker", :roadtype => "street",
               :locality => "melbourne", :state => "VIC", :postcode => 3000)
"""
function addressparts(s::String, auxinfo::Dict{String, Any}=Dict{String, Any}())
    result = Dict{Symbol, Any}()  # partname => partvalue
    s = lowercase(s)
    v = split(s, " ")
    for word in v
    end

end


end
