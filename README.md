# SpineBasedRecordLinkage.jl

Spine-based record linkage in Julia.

## Concepts

- We start with 1 or more tabular data sets.

- Each record in each table describes either an entity or an event involving an entity.

- An __entity__ is the unit of interest. It is usually a person, but may be something else such as a business enterprise.

- A __event__ involving an entity may be a sale, a hospital admission, an arrest, a mortgage payment, and so on.
  In some contexts, such as healthcare, events are often known as episodes or encounters.
  In others, such as sales, events are transactions.

- __Record linkage__ is, at its core, the problem of determining whether two events refer to the same entity.

- Spine-based record linkage links events one at a time to a __spine__ - a table in which each record specifies an entity.

## Usage

This package provides 3 functions:

1. `run_linkage` is used to construct a spine from one or more tables and link the tables to the spine.
   Alternatively, an existing spine can be passed and `run_linkage` will only perform the linkage step.
   __A linkage is configured in a YAML file and can run as a script, so that users needn't write any Julia code.__

2. `summarise_linkage_run` provides a summary report of the results of a linkage run as a CSV file.

3. `compare_linkage_runs` provides a summary comparison of 2 linkage runs as a CSV file.

## Example

We demonstrate linkage configuration and execution using an example from the test suite.

In the example we have a population of people (entities) using various health services (each usage is an event).

We have 3 tables in which each row describes a usage of a health service.
That is, we have 3 event-based tables in which each row specifies an event that refers to an entity.
Note that we do not have a linkage spine.

The 3 tables are:

- `hospital_admissions` describes admissions to various hospitals.
- `emergency_presentations` describes presentations to the emergency departments of various hospitals.
- `influenza_cases` contains reports cases of influenza that are required to be notified to the central health department.

The schema for each of these tables can be found in the `test/schema` directory.

Each row of each of these tables contains personally identifiable information, such as names and birth dates,
so that the person (entity) that the event refers to can be identified.
Each row also contains enough information to uniquely identify the event, such as a hospital ID and presentation time stamp,
but doesn't contain all of the event's data, such as the reason for the emergency.
This is common practice in data linkage, whereby the information required for linkage and that required for analysis are separated and handled
by different people in order to preserve anonymity.

Our goal is to link these tables so that we can ask question such as:

- How many influenza cases presented to an emergency department last year?
- How many of these were hospitalised?
- What were the most common reasons for repeated emergency presentations?
- How often do people utilise multiple hospitals for the same underlying problem?

### Configuration

Consider the following linkage configuration file, `link_all_health_service_events.yml`, which is in the `test/config` directory.

```yaml
projectname: health-service-usage
description: Construct a spine from 3 health service usage tables and link the tables to the spine.
output_directory: "output"  # During testing this expands to: /path/to/SpineBasedRecordLinkage.jl/test/output/
spine: {datafile: "", schemafile: "schema/spine.yml"}
append_to_spine: true
construct_entityid_from: [firstname, lastname, birthdate]
tables:
    hospital_admissions:     {datafile: "data/hospital_admissions.csv",     schemafile: "schema/hospital_admissions.yml"}
    emergency_presentations: {datafile: "data/emergency_presentations.csv", schemafile: "schema/emergency_presentations.yml"}
    influenza_cases:         {datafile: "data/influenza_cases.csv",         schemafile: "schema/influenza_cases.yml"}
criteria:
    - {tablename: hospital_admissions,     exactmatch: {firstname: firstname, lastname: lastname, birthdate: birthdate}}
    - {tablename: emergency_presentations, exactmatch: {firstname: firstname, lastname: lastname, birthdate: birthdate}}
    - {tablename: emergency_presentations, exactmatch: {birthdate: birthdate},
                                           approxmatch: [{datacolumn: firstname, spinecolumn: firstname, distancemetric: levenshtein, threshold: 0.3},
                                                         {datacolumn: lastname,  spinecolumn: lastname,  distancemetric: levenshtein, threshold: 0.3}]}
    - {tablename: influenza_cases, exactmatch: {firstname: firstname, middlename: middlename, lastname: lastname, birthdate: birthdate}}
    - {tablename: influenza_cases, exactmatch: {firstname: firstname, lastname: lastname, birthdate: birthdate}}
    - {tablename: influenza_cases, exactmatch: {firstname: firstname, birthdate: birthdate},
                                                approxmatch: [{datacolumn: lastname, spinecolumn: lastname, distancemetric: levenshtein, threshold: 0.3}]}
    - {tablename: influenza_cases, exactmatch: {lastname: lastname, birthdate: birthdate},
                                                approxmatch: [{datacolumn: firstname, spinecolumn: firstname, distancemetric: levenshtein, threshold: 0.5}]}
```

The configuration contains:

- A `projectname`, which enables linkage output to be easily identified.
- A linkage `description`, which should describe the purpose of the linkage.
- The output of a linkage run will be contained in a directory with the form `{output_directory}/linkage-{projectname}-{timestamp}`
- A schema of the spine specified in `/path/to/spine_schema.yaml`.
  This file specifies the columns, data types etc of the spine.
  See the `test/schema` directory as well as [Schemata.jl](https://github.com/JockLawrie/Schemata.jl) for examples of how to write a schema.
- A file path that contains the spine's pre-existing data. If the spine does not already exist, set the spine's `datafile` value to `""`.
- If constructing a spine from scratch, or appending rows to an existing spine (for example with updated data), set `append_to_spine` to true.
  If `append_to_spine` is true then records in the input tables that cannot link to an existing row in the spine are appended to the spine and linked.
  Otherwise these records are left unlinked.
- If `append_to_spine` is true then `construct_entityid_from` is required. It is a list of columns from which to construct the `EntityId` column (the `EntityId` is a hash of these columns).
- Specifications of the 3 tables:
  - The table names are arbitrary.
  - The locations of each table's data file and schema file are specified in the same way as those of the spine. 
- A list of linkage criteria.
  - The list is processed in sequence, so that multiple sets of criteria can be compared to the same table in a specified order.
    For example, you can match on name and birth date, and if that fails then try matching on name and address.
  - Each element of the list is a set of criteria.
  - For each set of criteria:
    - The rows of the specified table are iterated over and the criteria are checked.
    - For a given row, if the criteria are satisifed then it is linked to a row of the spine.
  - In our example:
    - The 1st iteration will loop through the rows of the hospital admissions table.
      - A row is linked to a row in the spine if the values of `firstname`, `lastname` and `birthdate` in the row __exactly__ match the values of `firstname`, `lastname` and `birthdate` respectively in the spine row.
      - If no such spine row exists, the row is appended to the spine and linked because `append_to_spine` is set to `true`.
      - This scenario is equvialent to a SQL join, and does not require the hospital admissions table to fit into memory.
      - Several rows in the hospital admissions table may link to a given spine row, because a person may be admitted many times.
        But any given row can only link to 1 row in the spine.
        That is, a hospital admission refers to exactly 1 person.
    - The 2nd iteration is similar but links the emergency presentations table instead.
    - The 3rd iteration requires birth date to match exactly, but allows some imprecision on first name and last name.
      Specifically, this iteration will match a row from the emergency presentations to a row in the spine if:
      - The 2 rows match exactly on birth date.
      - The Levenshtein distance (see the notes below) between the first names in the 2 rows is no more than 0.3, _and_ ditto for the last names.

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

Once your schemata and linkage configuration are set up, you can run the following script from the command line on Linux or Mac:

```bash
$ julia /path/to/SpineBasedRecordLinkage.jl/scripts/run_linkage.jl /path/to/link_all_health_service_events.yml
```

If you're on Windows you can run this from PowerShell:

```bash
PS julia path\to\SpineBasedRecordLinkage.jl\scripts\run_linkage.jl path\to\link_all_health_service_events.yml
```

Alternatively you can run the following code from the Julia REPL:

```julia
using SpineBasedRecordLinkage

run_linkage("/path/to/link_all_health_service_events.yml")
```

### Inspect the results

The results of `run_linkage` are structured as follows:

1. A new directory is created which will contain all output. Its name has the form:

   `{output_directory}/linkage-{projectname}-{timestamp}`
2. The directory contains `input` and `output` directories.
3. The `input` directory contains a copy of the config file and a file containing the versions of Julia and this package.
   The data tables are not copied to the `input` directory because they may be very large and take a long time.
   Ditto for the spine if it exists prior to the linkage run.
4. The `output` directory contains the information necessary to inspect the linkage results and construct linked content data.
   It contains the following files:
   - A `spine.tsv` file, containing the columns specified in the spine's schema.
     The schema __must__ include `EntityId` as the primary key, with data type `UInt`.
   - A `links.tsv` table, that links events to entities.
       - The columns are `TableName`, `EventId`, `EntityId` and `CriteriaId`.
       - The _links_ table joins to the spine on `EntityId` in a many-to-one relationship.
       - For each row the `CriteriaId` specifies which set of linkage criteria was satisfied to enable the link.
   - A `criteria.tsv` table, in which each row specifies a linkage criterion. It joins to the _links_ table on `CriteriaId` in a one-to-many relationship.
   - For each input table there is a file containing the table's primary key and a new `EventId` column.
       - For each row the `EventId` is constructed as a hash of the table's name concatenated with the row's primary key.
       - These tables join to the _links_ table on `EventId` in a one-to-one relatinship.
       - Each of these tables also joins to the corresponding table containing de-identified data via the primary key. In this way de-identified data needn't be updated every time a linkage run is performed. Put another way, the output of a linkage run joins to the de-identified data and by doing so links events to entities. Note that this only requires the _links_ and _primary key to eventid_ tables - __the identifiable information in the spine needn't be exposed to those analysing the de-identified data__.

### Summarise the results

We often want to answer questions like:

- How many links are there?
- What links have remained unchanged since the last run?
- What links are new? Broken? Intact but now satisfying different criteria?
- How many records remain unlinked? And which ones are they?

For a quick summary of a given linkage run, use the `summarise_linkage_run` function which has 2 arguments:

- `directory`: The directory that contains the results of the linkage run, i.e., `{output_directory}/linkage-{projectname}-{timestamp}`
- `outfile` :  The full path of the (csv or tsv) file that contains the report.

On Linux/Mac, with the report stored in `linkage_report.tsv`:

```bash
$ julia /path/to/SpineBasedRecordLinkage.jl/scripts/summarise_linkage_run.jl {output_directory}/linkage-{projectname}-{timestamp} linkage_report.tsv
```

On Windows (from PowerShell):

```bash
PS julia path\to\SpineBasedRecordLinkage.jl\scripts\summarise_linkage_run.jl {output_directory}\linkage-{projectname}-{timestamp} linkage_report.tsv
```

From the Julia REPL:

```julia
using SpineBasedRecordLinkage

summarise_linkage_run("{output_directory}/linkage-{projectname}-{timestamp}", "linkage_report.tsv")
```

To compare the results of 2 linkage runs use the `compare_linkage_runs` function which has 3 arguments:

- `directory1`: The directory that contains the results of the first linkage run. Typically this is the earlier run.
- `directory2`: The directory that contains the results of the second linkage run.
- `outfile` :  The full path of the (csv or tsv) file that contains the report.

On Linux/Mac, with the report stored in `linkage_comparison.tsv`:

```bash
$ julia /path/to/SpineBasedRecordLinkage.jl/scripts/summarise_linkage_run.jl directory1 directory2 linkage_comparison.tsv
```

On Windows (from PowerShell):

```bash
PS julia path\to\SpineBasedRecordLinkage.jl\scripts\summarise_linkage_run.jl directory1 directory2 linkage_comparison.tsv
```

From the Julia REPL:

```julia
using SpineBasedRecordLinkage

summarise_linkage_run(directory1, directory2, "linkage_comparison.tsv")
```

## Tips for users

- When using a pre-existing spine, either comma-separated values (csv) or tab-separated values (tsv) are fine.
  Since commas are generally more common in data than tabs, a `tsv` is usually safer than a `csv`, though not foolproof.
- The spine is currently required to fit into memory, though the tables to be linked to the spine can be arbitrarily large.
  For example, the package has been tested with files up to 60 million records on a commodity machine with 8GB of RAM.
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
