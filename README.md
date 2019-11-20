# SpineBasedRecordLinkage.jl

Spine-based record linkage in Julia.

## Concepts

- We start with 1 or more tabular data sets.

- Each record in each table describes either an entity or an event involving an entity.

- An __entity__ is the unit of interest. It is usually a person, but may be something else such as a business enterprise.

- A __event__ involving an entity may be a sale, a hospital admission, an arrest, a mortgage payment, and so on.
  In some contexts, such as healthcare, events are known as episodes or encounters.
  In others, such as sales, events are transactions.

- __Record linkage__ is, at its core, the problem of determining whether two records refer to the same entity.

- The 2 basic approaches to record linkage are:
  - __Cluster-based linkage__: Records from the various data sets are clustered according to their content.
  - __Spine-based linkage__:   Records are linked one at a time to a __spine__ - a table in which each record specifies an entity.


## Usage

This package provides 2 functions:

1. `construct_spine` is used to construct a spine from a given table.

2. `run_linkage` is used to link several tables to the spine.

Both operations are configured in YAML files and run as scripts, so that users needn't write any Julia code.

__Notes__:

- The spine is currently required to fit into memory, though the tables to be linked to the spine can be arbitrarily large.
  For example, the package has been tested with files up to 60 million records.
- For performance this package only compares string values.
  Therefore it is important that data be formatted correctly before linkage, and before spine construction if you don't already have a spine.
  For example, dates should have a common format in all tables, invalid values should be removed, etc.
  Using the [Schemata.jl](https://github.com/JockLawrie/Schemata.jl) package is strongly recommended for this purpose,
  as it is easy to use and you can reuse the schema files for spine construction and linkage.

## Linkage

We describe the `run_linkage` function first because, while some of the underlying concepts used are also used in spine construction, they are more naturally introduced in the context of linkage.

### Configuration

Suppose you have a file called `linkage_config.yaml` which contains the following:

```yaml
projectname: myproject
output_directory:  "/path/to/linkage/output"
spine: {datafile: "/path/to/spine.tsv", schemafile: "/path/to/spine_schema.yaml"}
tables:
    table1: {datafile: "/path/to/table1.tsv", schemafile: "/path/to/table1_schema.yaml"}
    table2: {datafile: "/path/to/table2.tsv", schemafile: "/path/to/table2_schema.yaml"}
criteria:
    - {tablename: table1, exactmatch:  {firstname: First_Name, lastname: Last_Name, birthdate: DOB}}
    - {tablename: table1, exactmatch:  {birthdate: DOB},
                          approxmatch: [{datacolumn: firstname, spinecolumn: First_Name, distancemetric: levenshtein, threshold: 0.2},
                                        {datacolumn: lastname,  spinecolumn: Last_Name,  distancemetric: levenshtein, threshold: 0.2}]}
    - {tablename: table2, exactmatch:  {firstname: First_Name, middlename: Middle_Name, surname: Last_Name, birthdate: DOB}}
```

In this example we have:

- A project name and a directory that will contain the output. See the __Run Linkage__ section below for details on how these 2 fields are utilised.
- A pre-existing spine located at `/path/to/spine.tsv`.
  - See below for how to construct a spine if you don't already have one.
  - The spine is a _tab-separated values_ file, which indicated by the `tsv` extension.
  - A comma-separated values (`csv`) file would be fine too, provided that commas don't appear as values in any of the columns.
    Since commas are generally more common in data than tabs, a `tsv` is usually safer than a `csv`, though not foolproof.
- A schema of the spine specified in `/path/to/spine_schema.yaml`.
  This file specifies the columns, data types etc of the spine.
  See [Schemata.jl](https://github.com/JockLawrie/Schemata.jl) for examples of how to write a schema.
- Two tables, named `table1` and `table2`, to be linked to the spine.
  - The names are arbitrary.
  - The locations of each table's data file and schema file are specified in the same way as those of the spine. 
- A list of linkage criteria.
  - The list is processed in sequence.
  - Each element of the list is a set of criteria.
  - For each set of criteria:
    - The rows of the specified table are iterated over and the criteria are checked.
    - For a given row, if the criteria are satisifed then it is linked to a row of the spine.
  - In our example:
    - The 1st iteration will loop through the rows of `table1`.
      - A row is linked to a row in the spine if the values of `firstname`, `lastname` and `birthdate` in the row __exactly__ match the values of `First_Name`, `Last_Name` and `DOB` respectively in the spine row.
      - This scenario is equvialent to a SQL join, but does not require `table1` to fit into memory.
      - If the rows of `table1` specify events (instead of entities - see above), several rows in `table1` may link to a given spine row.
    - The 2nd iteration requires an exact match for `birthdate` and approximate matches for `firstname` and `lastname`.
      Specifically, this iteration will match a row from `table1` to a row in the spine if:
      - The 2 rows match exactly on birth date.
      - The Levenshtein distance (see the notes below) between the first names in the 2 rows is no more than 0.2, and ditto for the last names.
    - The 3rd iteration iterates through the rows of `table2` and requires exact matches on first name, middle name, last name and birth date.
      
__Notes on approximate matches__

- Approximate matching relies on _edit distances_, which measure how different 2 strings are.
- In this package edit distances are scaled to be between 0 and 1, where 0 denotes an exact match (no difference) and 1 denotes complete difference.
- The distance between a missing value and anothr value (missing or not) is defined to be 1 (complete difference).
- The Levenshtein distance in our example is an example of an edit distance.
- For example:
  - `Levenshtein("robert", "robert") = 0`
  - `Levenshtein("robert", "rob") = 0.5`
  - `Levenshtein("robert", "bob") = 0.667`
  - `Levenshtein("rob",    "bob") = 0.333`
  - `Levenshtein("rob",    "tim") = 1`
  - `Levenshtein("rob",    missing) = 1`
- There are several edit distance measures available, see [StringDistances.jl](https://github.com/matthieugomez/StringDistances.jl) for other possibilities.

### Run Linkage

You can then run the linkage with the following code:

```julia
using SpineBasedRecordLinkage

run_linkage("linkage_config.yaml")
```

Alternatively you can run the following script on Linux or Mac:

```bash
$ julia /path/to/SpineBasedRecordLinkage.jl/scripts/run_linkage.jl /path/to/linkage_config.yaml
```

Or using Windows PowerShell:

```bash
PS julia path\to\SpineBasedRecordLinkage.jl\scripts\run_linkage.jl path\to\linkage_config.yaml
```

When you run the script or `run_linkage("linkage_config.yaml")`, the following happens:

1. A new directory is created which will contain all output. Its name has the form: `{output_directory}/linkage-{projectname}-{timestamp}`
2. The directory contains `input` and `output` directories.
3. The `input` directory contains a copy of the config file and a file containing the versions of Julia and this package.
4. The output directory contins `identified` and `deidentified` directories.




## Constructing a spine


## Methodology

3. An entity is identified by a set of fields. For example, a person can be identified by his/her name, birth date, address, etc.

4. An entity's identifying information may change over time, though the entity remains the same. For example, a person may change his/her address.

5. Records that specify the entities of interest are stored in a table in which:
   - Each row represents an entity for a specified time period.
   - The schema is defined using the `Schemata.jl` package.
   - The required columns are: `recordid`, `entityid`, `recordstartdate`
   - There are other columns that identify an entity, such as name, birth date, etc.
   - Entities can have multiple records.
   - Entities with multiple records will have the same `entityid`.
   - Therefore the primary key of the table is [`entityid`, `recordstartdate`]
   - This package sets `recordid = base64encode(hash(vals...))"`, where vals are the values of all columns other than `entityid` and `recordstartdate`.


config specifies locations of linkmaps.
if the linkmap files don't exist, the package will init them.
linkmaps are never overwritten. This forces the user to remove the linkmaps manually if s/he wants to overwrite them.


When writing output, create a new directory using the run's timestamp. Do not overwrite any input! 

- Tips for users:
  - When specifying the project name in the config, make it recognisable, such as rq452.
  - For each data table, if last_updated column exists, include it in the primary key.
  - Governance (i.e., versioning) of the spine and the input data tables is the responsibility of the user.
    - It is out of the scope of the linkage engine.
    - Users must ensure that the spine and data used in a linkage run is preserved without any changes. Otherwise the linkage run may not be reproducible.

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