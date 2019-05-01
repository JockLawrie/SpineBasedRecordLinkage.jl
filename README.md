# RecordLinkage.jl

Record linkage in Julia, configurable in YAML.

## Methodology

The central concepts are described here. See below for an example.

1. Define a central `Person` table.
   - This is a DataFrame with each row representing a person for a specified time period.
   - The columns are: recordid, personid, recordstartdate, columns that identify a person.
   - For example, the identifying columns typically include:
       firstname, middlename, lastname, gender, birthdate, deathdate, streetaddress, postcode, locality, state.
   - The format for each column is specified by a `Schema` as provided by the `Schemata` package.
     This explicitly specifies data standardisation and implicitly enforces the requirements of the pre-processing stage.
   - People can have multiple records, for example if they change their name or address.
   - People with multiple records will have the same `personid`.
   - Therefore the primary key of the table is [`personid`, `recordstartdate`]
   - This package sets `recordid = base64encode(hash(vals...))"`, where vals are the values of all columns other than `personid` and `recordstartdate`.

2. A persistent linkage map is initialised.
   Each row defines a match between a record in the `Person` table and a record in another table of interest.
   - The columns are: `tablename`, `tablerecordid`, `personrecordid`.
   - Note that this table implicitly defines matches between records of different tables of interest.
     That is, 2 records from different tables that link to the same record in the `Person` table are implicitly matched.
   - The linkage map also avoids the need to rerun the linkage algorithm on historical data, thus yielding faster linkage on new data.

3. Ensure that each table to be linked to the `Person` table has a `recordid` column with unique values and no missing values.

4. A linkage pipeline has the following stages:
   - Pre-processing: Transforming the input data into the standardised formats present in the `Person` table.
   - Linkage:        For each table in your data set, try to match each record to a record in the `Person` table. The results are recorded in the linkage map.
   - Reporting:      Summarise the results of the linkage run, including how many records were matched and how.

5. The `linkage` stage consists of an ordered sequence of _linkage passes_.
   Each linkage pass specifies:
   - The name of the table to be linked.
   - The set of columns on which both records in a matched pair must match exactly.
   - A set of fuzzy match criteria.

   Note that multiple linkage passes can be made against the same table. For example, you can match on name and birth date, then on name and address.

6. Each fuzzy match criterion must specify:
   - The pair of columns being compared (1 in the data table and 1 in the `Person` table).
   - A distance metric, which quantifies how "close" the pair of values is.
   - A threshold, below which the pair of values is considered sufficiently close.

7. If no fuzzy matching criteria are specified then a record can only be linked to the `Person` table if there is exactly 1 candidate match in the `Person` table.

8. If fuzzy matching criteria are specified and there are several candidate matches in the `Person` table, then:
   - For each candidate record an overall distance between it and the data record is calculated.
     This distance is calculated as the sum of the individual column distances specified by the fuzzy match criteria.
   - The best candidate is that with the smallest distance from the data record.
     This is deemed the matching record.


## Example Usage

To run a linkage pipeline you need to provide:
1. A YAML file that specifies the pipeline's configuration. See the example below.

2. A function for performing the preprocessing stage, since this will be specific to your data.
The preprocessing function takes a single argument, namely the `Dict` specified in the `preprocessing` stage of your config file.
By convention the function reads in data from the specified input location, transforms it as required, then writes it to the specified output location. However, you can have it do anything you like provided it takes the `Dict` specified in the `preprocessing` stage of your config file.

Once this is done you can run some code like the following:

```julia
using RecordLinkage
using YAML

d = YAML.load_file("myconfig.yaml")
# Define preprocessing function here
run_linkage_pipeline(d, preprocessing_func)
```

Here is the config file, _myconfig.yaml_. Note that the `Person` table and the linkage map are defined using the [Schemata](https://github.com/JockLawrie/Schemata.jl) package.

```
#########################################################################################################
# PIPELINE

stages: [preprocessing, linkage]

#########################################################################################################
# PREPROCESSING

preprocessing:
    inputdir:  "/projects/recordlinkage/data/input"
    outputdir: "/projects/recordlinkage/data/preprocessed"
    datatables:
        table1_raw: {infile: "table1.csv", outfile: "preprocessed_table1.tsv"}
        table2_raw: {infile: "table2.csv", outfile: "preprocessed_table2.tsv"}

#########################################################################################################
# LINKAGE

linkage:
    inputdir:  "/projects/recordlinkage/data/preprocessed"
    outputdir: "/projects/recordlinkage/data/linked"
    datatables:
        table1: "preprocessed_table1.tsv"
        table2: "preprocessed_table2.tsv"
    update_person_table: table1
    linkage_passes:
        - {tablename: table1, exactmatch_columns: [firstname, middlename, lastname, birthdate, gender, streetaddress, locality, postcode, state]}
        - {tablename: table2, exactmatch_columns: [firstname, lastname, birthdate, gender]}
        - {tablename: table2, exactmatch_columns: [firstname, lastname, birthdate]}
        - {tablename: table2, exactmatch_columns: [birthdate, gender],
                              fuzzymatches: [{columns: [firstname, firstname], distancemetric: levenshtein, threshold: 0.2},
                                             {columns: [lastname,  lastname],  distancemetric: levenshtein, threshold: 0.2}]}

#########################################################################################################
# DEFINITION OF PERSON

persontable:
    name: person
    description: Person table
    primary_key: recordid
    columns:
        - recordid: {description: Record ID, datatype: String, categorical: false, required: true, unique: true,  validvalues: String}
        - personid: {description: Person ID, datatype: Int,    categorical: false, required: true, unique: false, validvalues: Int}
        - recordstartdate: {description: Date from which the record is valid,
                            datatype: Date, categorical: false, required: false, unique: false, validvalues: Date}
        - firstname:  {description: First name, datatype: String, categorical: false, required: false, unique: false, validvalues: String}
        - middlename: {description: Middle name, datatype: String, categorical: false, required: false, unique: false, validvalues: String}
        - lastname:   {description: Last name, datatype: String, categorical: false, required: false, unique: false, validvalues: String}
        - birthdate:  {description: Birth date, datatype: Date, categorical: false, required: false, unique: false, validvalues: Date}
        - deathdate:  {description: Date of death, datatype: Date, categorical: false, required: false, unique: false, validvalues: Date}
        - gender:     {description: Gender, datatype: String, categorical: true, required: false, unique: false, validvalues: ["m", "f"]}
        - streetaddress:  {description: "Street address (street name and number)",
                           datatype: String, categorical: false, required: false, unique: false, validvalues: String}
        - locality:       {description: "Locality (typically suburb)",
                           datatype: String, categorical: false, required: false, unique: false, validvalues: String}
        - postcode:       {description: Post code, datatype: Int, categorical: false, required: false, unique: false, validvalues: "1000:9999"}
        - state:          {description: State, datatype: String,  categorical: true,  required: false, unique: false,
                           validvalues: ["ACT", "NSW", "NT", "SA", "QLD", "TAS", "VIC", "WA"]}

#########################################################################################################
# DEFINITION OF LINKMAP

linkmap:
    name: linkmap
    description: Linkage map
    primary_key: [tablename, tablerecordid, personrecordid]
    columns:
        - tablerecordid:  {description: Record ID from data table,   datatype: String, categorical: false, required: true, unique: false, validvalues: String}
        - personrecordid: {description: Record ID from person table, datatype: String, categorical: false, required: true, unique: false, validvalues: String}
```


## TODO

- Implement the reporting stage
- When doing fuzzy matching, handle missing data better.
- Implement aliases for names. E.g., robert, rob, bob, bobby, etc.
- Create a `Libpostal.jl` package.
- When populating persontable:
    - Combine rows that are probably the same person (needs fuzzy matching).
    - E.g., all fields match but 1 row has missing postcode.
    - Distinguish missing values from differing values (more lenient with the former?)
