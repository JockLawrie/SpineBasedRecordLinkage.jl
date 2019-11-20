# SpineBasedRecordLinkage.jl

Spine-based record linkage in Julia.

## Concepts

- We start with 1 or more tabular data sets.

- Each record in each table describes either an entity or an event involving an entity.

- An __entity__ is the unit of interest. It is usually a person, but may be something else such as a business enterprise.

- A __event__ involving an entity may be a sale, a hospital admission, an arrest, a mortgage payment, and so on.
  In some contexts, such as healthcare, events are often known as episodes or encounters.
  In others, such as sales, events are transactions.

- __Record linkage__ is, at its core, the problem of determining whether two records refer to the same entity.

- The 2 basic approaches to record linkage are:
  - __Cluster-based linkage__: Records from the various data sets are clustered according to their content.
  - __Spine-based linkage__:   Records are linked one at a time to a __spine__ - a table in which each record specifies an entity.

## Usage

This package provides 2 functions:

1. `construct_spine` is used to construct a spine from a given table.

2. `run_linkage` is used to link one or more tables to the spine.

Both operations are configured in YAML files and run as scripts, so that users needn't write any Julia code.

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

In this example configuration we have:

- A project name and a directory that will contain the output. See the __Run linkage__ section below for details on how these 2 fields are utilised.
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
  - The list is processed in sequence, so that multiple sets of criteria can be compared to the same table in a specified order.
    For example, you can match on name and birth date, then on name and address.
  - Each element of the list is a set of criteria.
  - For each set of criteria:
    - The rows of the specified table are iterated over and the criteria are checked.
    - For a given row, if the criteria are satisifed then it is linked to a row of the spine.
  - In our example:
    - The 1st iteration will loop through the rows of `table1`.
      - A row is linked to a row in the spine if the values of `firstname`, `lastname` and `birthdate` in the row __exactly__ match the values of `First_Name`, `Last_Name` and `DOB` respectively in the spine row.
      - This scenario is equvialent to a SQL join, but does not require `table1` to fit into memory.
      - If the rows of `table1` specify events (instead of entities - see above), several rows in `table1` may link to a given spine row,
        though any given row can only link to 1 row in the spine.
    - The 2nd iteration requires an exact match for `birthdate` and approximate matches for `firstname` and `lastname`.
      Specifically, this iteration will match a row from `table1` to a row in the spine if:
      - The 2 rows match exactly on birth date.
      - The Levenshtein distance (see the notes below) between the first names in the 2 rows is no more than 0.2, and ditto for the last names.
    - The 3rd iteration iterates through the rows of `table2` and requires exact matches on first name, middle name, last name and birth date.

__Notes on approximate matches__

- Approximate matching relies on _edit distances_, which measure how different 2 strings are.
- In this package edit distances are scaled to be between 0 and 1, where 0 denotes an exact match (no difference) and 1 denotes complete difference.
- The distance between a missing value and another value (missing or not) is defined to be 1 (complete difference).
- The Levenshtein distance in our example is an example of an edit distance.
- For example:
  - `Levenshtein("robert", "robert") = 0`
  - `Levenshtein("robert", "rob") = 0.5`
  - `Levenshtein("robert", "bob") = 0.667`
  - `Levenshtein("rob",    "bob") = 0.333`
  - `Levenshtein("rob",    "tim") = 1`
  - `Levenshtein("rob",    missing) = 1`
- There are several edit distance measures available, see [StringDistances.jl](https://github.com/matthieugomez/StringDistances.jl) for other possibilities.
- If approximate matching criteria are specified and several rows in the spine satisfy the criteria for a given data row,
  then the best matching spine row is selected as the match for the data row.
- The best match is the spine row with the lowest total distance from the data row.

__Notes on exact matches__

- The notion of distance introduced above implies that a pair of values that match exactly have a distance between them of 0. For example, `Levenshtein(value1, value2) = 0`.
- Similalrly, a missing value cannot be part of an exact match because it has distance 1 from any other value. For example, `Levenshtein(value1, missing) = 1`.
- If no approximate matching criteria are specified then a record can only be linked to the spine if there is exactly 1 candidate match in the spine.

### Run linkage

Once you have a spine and your config is set up, you can run the following script from the command line on Linux or Mac:

```bash
$ julia /path/to/SpineBasedRecordLinkage.jl/scripts/run_linkage.jl /path/to/linkage_config.yaml
```

If you're on Windows you can run this from PowerShell:

```bash
PS julia path\to\SpineBasedRecordLinkage.jl\scripts\run_linkage.jl path\to\linkage_config.yaml
```

Alternatively you can run the following code:

```julia
using SpineBasedRecordLinkage

run_linkage("/path/to/linkage_config.yaml")
```

### Inspect the results

The results of `run_linkage` are structured as follows:

1. A new directory is created which will contain all output.

   Its name has the form: `{output_directory}/linkage-{projectname}-{timestamp}`
2. The directory contains `input` and `output` directories.
3. The `input` directory contains a copy of the config file and a file containing the versions of Julia and this package.
   The spine and data tables are not copied to the `input` directory because they may be very large and take a long time.
4. The `output` directory contains the information necessary to inspect the linkage results and construct linked content data.
   It contains the following files:
   - A `criteria.tsv` table, in which each row specifies a linkage criterion.
   - A `spine_simplified.tsv` file, containing only a `spineID` column and the columns of the spine's primary key (as specified in the schema used in the linkage configuration).
   - Simplified data tables. Each simplified table contains:
     - A `spineID` column which links the table to the spine.
     - The table's primary key columns, which enable the construction of linked content data.
     - A `criteriaID` column that links to the `criteria.tsv` file, so that we can see what criteria werre satisfied for each link.
5. The tables of the `output` directory can be read into a BI reporting tool for easy interrogation visualisation.
   We can then easily answer questions like:
   - How many links were made?
   - What links have remained unchaneged since the last run?
   - What links are new? Broken? Intact but now satisfying different criteria?
   - How many records remain unlinked? And which ones are they?

## Constructing a spine

A spine can be constructed using the `construct_spine` function, which links a table to itself then removes duplicate links.
Therefore a configuration file for spine construction has the same format as a configuration file for linkage,
with the following constraints:

1. There is only 1 table to be linked to the spine.
2. The data files for the spine and the table are the same.
3. The schema files for the spine and the table are the same.

For example, the following configuration file is suitable for spine construction:

```yaml
projectname: myproject
output_directory:  "/path/to/linkage/output"
spine: {datafile: "/path/to/mytable.tsv", schemafile: "/path/to/mytable_schema.yaml"}
tables:
    mytable: {datafile: "/path/to/mytable.tsv", schemafile: "/path/to/mytable_schema.yaml"}
criteria:
    - {tablename: mytable, exactmatch:  {firstname: First_Name, lastname: Last_Name, birthdate: DOB}}
    - {tablename: mytable, exactmatch:  {birthdate: DOB},
                           approxmatch: [{datacolumn: firstname, spinecolumn: First_Name, distancemetric: levenshtein, threshold: 0.2},
                                         {datacolumn: lastname,  spinecolumn: Last_Name,  distancemetric: levenshtein, threshold: 0.2}]}
```

To evaluate the results:

1. Copy the configuration file that you used for spine construction.
2. Replace the spine data file with the location of the result of the spine construction process, namely:

   `{output_directory}/spineconstruction-{projectname}-{timestamp}/output/spine.tsv`

3. Run the linkage script with the modified configuration file.
4. Inspect the results as described above.

## Tips for users

- The spine is currently required to fit into memory, though the tables to be linked to the spine can be arbitrarily large.
  For example, the package has been tested with files up to 60 million records.
- For performance this package only compares string values.
  Therefore it is important that data be formatted correctly before linkage, and before spine construction if you don't already have a spine.
  For example, dates should have a common format in all tables, invalid values should be removed, etc.
  Using the [Schemata.jl](https://github.com/JockLawrie/Schemata.jl) package is strongly recommended for this purpose,
  as it is easy to use and you can reuse the schema files for spine construction and linkage.
- When specifying the project name in the config, make it easily recognisable.
- For each data table, if a `last_updated` column exists, include it in the primary key.
- Governance (i.e., versioning) of the input tables is the responsibility of the user.
  - It is out of the scope of the linkage engine.
  - Users must ensure that the spine and data used in a linkage run is preserved without any changes. Otherwise the linkage run may not be reproducible.