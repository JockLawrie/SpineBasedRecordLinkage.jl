# RecordLinkage.jl

## Methodology

1. Define a central `Person` table.
   - A DataFrame with each row representing a person for a specified date range.
   - The columns are: personid, recordstartdate, firstname, middlename, lastname, sex, birthdate, deathdate, streetaddress, postcode, locality, state, medicarenumber (first 8 digits).
   - People can have multiple records, for example if they change their name or address.
   - People with multiple records will have the same `personid`.
   - Therefore the primary key of the table is [`personid`, `recordstartdate`]
   - Set `recordid = hash((col1=val1,...))"`, for columns other than `personid` and `recordstartdate`.
     This serves as a quick lookup and also defines a primary key (it is one-to-one to the primary key).

2. Define a standard format for each column in the Person table.
   This minimises record duplication and implicitly specifies the data pre-processing steps.
   We specify the formats in a schema.

3. Initialise a persistent linkage map in which each row defines a match between a person in the `Person` table and a record in another table of interest.
   - The columns are: `tablename`, `tablerecordid`, `personrecordid`.
   - Note that this table implicitly defines matches between records of different tables of interest.
     That is, 2 records from different tables that link to the same `personid` are implicitly matched.
   - The linkage map avoids the need to rerun the linkage algorithm on historical data, thus yielding faster linkage on new data.

4. Undertake pre-processing.
   This is the data cleaning and formatting implied by the standardised field formats defined in Step 2.

5. Ensure that each table has a persistent unique row identifier (`recordid`).
   This facilitates the persistent linkage map.
   For example, set `recordid = hash((col1=val1,...))` for all columns in the table that are also columns of the Person table.

6. Exact matching on all fields.
   For each record in a given table of interest:
   - Test whether the person specified by the record is in the Person table.
   - If so, make an entry in the linkage map.

7. Exact matching on some fields.
   For each table of interest:
   a. Select the columns for exact matching.
   b. Construct an index for the selected columns, defined uniquely and persistently based on the values in the columns. Here we use `rowid = hash(col1=val1, ...)`.
   c. Construct a smaller table consisting of the `recordid` from Step 5 and the `rowid` defined in 7b.
   d. Repeat for the Person table, yielding a table with columns `recordid` and `rowid`.
   e. Join these smaller tables on the `rowid` fields constructed in Steps 7b and 7d.
   f. Append the matched records to the persistent linkage map.

Undertake exact matching on all relevant fields.



8. Fuzzy matching.
   For each table of interest:
   a. Specify columns on which exact matching is required.
   b. Identify groups of records defined by these columns.
   For each unmatched record in each group:
    - Construct distance matrix
    - Set distance threshold
    - Apply distance matrix


## TODO

1. Check person.locality against the values available from the ABS.
2. Pre-processing:
   - st => street, crt => court, dr => drive, drv => drive, hwy => highway, etc
   - vdi.fulladress: start at the end of the string, identify each word as one of postcode, state, locality or road type (st, drv, ave, etc)
   - Implement as a function that takes a string and returns (streetaddress,postcode,locality).
   - Use ABS tables to get state from postcode
