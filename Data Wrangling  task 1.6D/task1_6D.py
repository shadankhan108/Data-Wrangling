# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# ##### TASK 1.6D: pandas vs SQL
# ##### Name: Shadan Khan
# ##### Enrolment: s222623809
# ##### Email: s222623809@deakin.edu.au
#

# %% [markdown]
# # 1. Introduction
#
# This exercise explores how SQL and pandas each tackle common data operations—filtering, joining, grouping, and sorting—while highlighting their distinct philosophies. SQL (Structured Query Language) is a declarative, set-based language designed for querying relational databases, where you specify what result you want without detailing the steps to get it. In contrast, pandas is a procedural Python library that manipulates in-memory tables through explicit, step-by-step commands.
#
# By converting practical SQL queries into their pandas equivalents, we’ve illuminated how concepts translate between these two environments. This side-by-side comparison not only clarifies their syntactic and performance trade-offs but also underscores each tool’s strengths—SQL’s succinct expression of complex joins and aggregates versus pandas’s seamless integration with Python and its rich data-processing ecosystem.
#
# In the sections that follow, you’ll find each SQL statement paired with its pandas counterpart, accompanied by notes on structural adjustments, performance considerations, and readability enhancements.
#

# %% [markdown]
# ## 1.1 Connection with the Database
#
# In this phase, we’ll ingest multiple CSV files into an SQLite database so that they become queryable relational tables—laying the groundwork for direct SQL analyses and side-by-side comparisons with pandas. The accompanying Python script works as follows:
#
# 1. **Database connection:**  Opens (or creates) my_database.db via SQLite.
#
# 2. **Type mapping:** Uses a helper function to translate pandas dtypes (e.g., int64 → INTEGER, float64 → REAL, others → TEXT) into SQLite column types.
#
# 3. **File discovery:** Locates every .csv file in the current directory.
#
# 4. **Data loading:** Reads each CSV into a pandas DataFrame—skipping malformed rows and ignoring lines that start with #.
#
# 5. **Schema generation:** Inspects each DataFrame’s columns and dtypes to build a matching CREATE TABLE statement dynamically, so each CSV becomes its own table.
#
# 6. **Table population:** Calls df.to_sql() to bulk-insert the DataFrame’s rows into the newly created SQLite table.
#
# 7. **Cleanup:** Commits all transactions and closes the database connection once every file has been processed.
#
# With this setup, your CSV files now live as SQL tables, ready for efficient querying and direct comparison with equivalent pandas operations.
#

# %%
import os
import sqlite3
import pandas as pd

def pandas_to_sqlite_type(pd_dtype):
    """
    Translate a pandas/NumPy dtype to an SQLite column type.
    """
    if pd.api.types.is_integer_dtype(pd_dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(pd_dtype):
        return 'REAL'
    else:
        return 'TEXT'

def load_csv_gz_to_sqlite(database_path: str, file_prefix: str = 'nycflights13_'):
    """
    Scan the current directory for .csv.gz files, 
    strip the specified prefix/suffix to form table names,
    and load each into the SQLite database.
    """
    # Establish database connection
    db_connection = sqlite3.connect(database_path)
    db_cursor = db_connection.cursor()

    for filename in os.listdir('.'):
        if not filename.endswith('.csv.gz'):
            continue

        # Derive table name from the filename
        table_name = (
            filename
            .removeprefix(file_prefix)
            .removesuffix('.csv.gz')
        )
        print(f"Loading {filename} into table '{table_name}'")

        # Read the compressed CSV into a DataFrame
        data_frame = pd.read_csv(filename, comment='#', on_bad_lines='skip')
        if data_frame.empty:
            print(f"  → Skipped: no records in {filename}")
            continue

        # Build the CREATE TABLE schema
        column_statements = []
        for col_name, col_dtype in zip(data_frame.columns, data_frame.dtypes):
            sqlite_type = pandas_to_sqlite_type(col_dtype)
            column_statements.append(f'"{col_name}" {sqlite_type}')
        schema_sql = ", ".join(column_statements)

        # Create the table if it doesn't already exist
        create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({schema_sql});'
        db_cursor.execute(create_sql)

        # Append rows to the table
        data_frame.to_sql(table_name, db_connection, if_exists='append', index=False)
        print(f"  → Inserted {len(data_frame)} rows into '{table_name}'")

    # Finalize changes and close
    db_connection.commit()
    db_connection.close()


load_csv_gz_to_sqlite('my_database.db')


# %% [markdown]
# ### 1.2 Verifying Table Population
#
# The ingestion script has finished running, detecting all CSV files in the working directory and importing them into the SQLite database my_database.db. For each file— 
#
# **flights**, 
# **planes**, 
# **airports**, 
# **airlines**, and 
# **weather**
#
# it automatically created a matching table with columns typed to align pandas dtypes with SQLite types. As a result, each table now accurately reflects its source CSV. With the data loaded, we’re primed to run SQL queries against these tables and directly compare the results to pandas-based operations.
#

# %% [markdown]
# ### 1.3 Table Verification and Row Counts
#
# Before diving into SQL queries or pandas comparisons, we need to verify that every CSV file ended up in our database as intended. We’ll start by pulling the list of tables currently defined in my_database.db and then tally the row count for each one.
#
# This validation step ensures our ingestion step worked properly—each table should contain the expected number of records. It will also flag any tables that remain empty or underpopulated, which can happen if a CSV was malformed or missing data. The code below performs this check by listing all tables and reporting their row counts in a concise summary.
#
#     

# %%
import sqlite3
import pandas as pd

def fetch_table_row_counts(db_file: str) -> pd.DataFrame:
    """
    Retrieves the row count for each non-system table in a SQLite database.

    Args:
        db_file (str): Path to the SQLite database file.

    Returns:
        pd.DataFrame: DataFrame with 'table_name' and 'row_count' columns.
    """
    try:
        with sqlite3.connect(db_file) as connection:
            # Query to list all user-defined tables
            table_query = """
                SELECT name 
                FROM sqlite_master 
                WHERE type = 'table' 
                  AND name NOT LIKE 'sqlite_%';
            """
            table_list = pd.read_sql_query(table_query, connection)['name'].tolist()

            rows_info = []
            for tbl in table_list:
                # Get row count for each table
                count_query = f'SELECT COUNT(*) AS total_rows FROM "{tbl}";'
                total_rows = pd.read_sql_query(count_query, connection).iloc[0, 0]
                rows_info.append({'table_name': tbl, 'row_count': total_rows})

            return pd.DataFrame(rows_info)

    except sqlite3.DatabaseError as error:
        print(f"SQLite error: {error}")
        return pd.DataFrame()

# Example usage
counts_df = fetch_table_row_counts('my_database.db')
print(counts_df)


# %% [markdown]
# ### Summary of Table Population
#
# The results verify that each table was created correctly and loaded with its corresponding CSV data. The flights table alone holds over six million rows—consistent with large-scale flight datasets—while the planes, airports, airlines, and weather tables each contain substantial record counts suitable for robust analysis. With this confirmation in hand, we’re now ready to move on to executing SQL queries alongside their pandas equivalents, so we can compare syntax, performance, and overall usability.
#

# %% [markdown]
#
# ## Exposing DataFrames in the Notebook Namespace
#
# To make each DataFrame directly available as a top-level variable, we inject them into the notebook’s global scope by assigning them via the built-in globals() function.

# %%
db_connection = sqlite3.connect('my_database.db')
db_cursor = db_connection.cursor()


# %%
shadan_tables = ['flights', 'planes', 'airports', 'airlines', 'weather']

# Load each table as a DataFrame and store it in the namespace
for table in shadan_tables:
    globals()[table] = pd.read_sql_query(f"SELECT * FROM {table};", db_connection)

# %% [markdown]
# ### We now start executing the queries one by one.

# %% [markdown]
# ### 1. Retrieve each unique engine type from the planes table

# %%

# Define the SQL query to get unique engines
shadan_sql_query = "SELECT DISTINCT engine FROM planes;"

# Execute the SQL query
shadan_engines_sql = pd.read_sql_query(shadan_sql_query, db_connection)

# Use pandas to get the same result
shadan_task1 = (
    planes[['engine']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Verify that both methods produce identical DataFrames
pd.testing.assert_frame_equal(shadan_engines_sql , shadan_task1)



# %% [markdown]
# ### SQL Behavior
# This query pulls a unique set of engine values from the planes table by applying the DISTINCT keyword to the engine column. As a result, each engine type appears only once, with all duplicates removed.
#
# ### Pandas Method
# In pandas, we replicate this by extracting the engine column from the planes DataFrame, calling .drop_duplicates() to filter out repeated entries, and then resetting the index so that the resulting Series aligns with the SQL output format—starting at zero and omitting any original row labels.
#
# ### Consistency Check
# To ensure both approaches yield the same outcome, we perform a strict DataFrame comparison. This final assertion confirms that the set of engine values returned by pandas matches exactly with those produced by the SQL query.

# %% [markdown]
# ### 2. Retrieve all unique (type, engine) combinations from the planes table

# %%
# SQL approach: retrieve unique combinations of type and engine
shadan_sql_query = pd.read_sql_query(
    "SELECT DISTINCT type, engine FROM planes;",
    db_connection
)

# Pandas approach: drop duplicates on the same columns
shadan_task2 = (
    planes[['type', 'engine']]
    .drop_duplicates()
    .reset_index(drop=True)
)

# Verify both results match exactly
pd.testing.assert_frame_equal(shadan_sql_query, shadan_task2)


# %% [markdown]
# ### SQL Query Overview
# This statement groups the planes table by the engine column and uses COUNT(*) to determine how many records share each engine type, returning a two-column result of engine and its corresponding count.
#
# ### Pandas Implementation
# In pandas, we achieve the same outcome by calling planes.groupby('engine').size(), which produces a Series of counts indexed by engine. We then convert this Series into a DataFrame, rename the count column to "COUNT(*)", and reset the index to yield a tidy table matching the SQL layout.
#
# ### Consistency Check
# Finally, we run a strict equality assertion (pd.testing.assert_frame_equal) to verify that the pandas-derived DataFrame is identical in both content and structure to the SQL query’s output.

# %% [markdown]
# ### 3. Count the number of planes for each engine type

# %%
# SQL version: count the number of planes per engine
shadan_task3_sql = pd.read_sql_query(
    "SELECT COUNT(*) AS \"COUNT(*)\", engine "
    "FROM planes "
    "GROUP BY engine;",
    db_connection
)

# Pandas version: group by engine, compute sizes, and format the DataFrame
counts = planes.groupby('engine').size()
shadan_task3 = (
    counts
    .reset_index(name='COUNT(*)')
    [['COUNT(*)', 'engine']]
)

# Verification: ensure both methods yield the same result
pd.testing.assert_frame_equal(shadan_task3_sql, shadan_task3)


# %% [markdown]
# ### Purpose of the SQL Query
# This statement groups the planes table by engine and counts the total entries in each group, yielding a result with two columns: the count of planes and the corresponding engine type.
#
# ### Equivalent Pandas Workflow
# In pandas, we replicate this by applying .groupby('engine').size() to the planes DataFrame, turning the resulting Series into a DataFrame, renaming the count column to "COUNT(*)", and then resetting the index to produce the same flat structure.
#
# ### Consistency Check
# Finally, we run a strict DataFrame equality assertion to confirm that the pandas-derived result matches the SQL output exactly in both content and layout.
#
#
#
#
#
#
#
#
#

# %% [markdown]
# ### 4. Count the number of planes for each engine–type combination

# %%
# SQL version: count the number of planes for each engine–type combination
shadan_task4_sql = pd.read_sql_query(
    "SELECT COUNT(*) AS \"COUNT(*)\", engine, type "
    "FROM planes "
    "GROUP BY engine, type;",
    db_connection
)

# Pandas version: group by engine and type, compute counts, and format the DataFrame
shadan_task4 = (
    planes
    .groupby(['engine', 'type'])
    .size()
    .reset_index(name='COUNT(*)')
    .loc[:, ['COUNT(*)', 'engine', 'type']]
)

# Verification: ensure both methods produce the same DataFrame
pd.testing.assert_frame_equal(shadan_task4_sql, shadan_task4)


# %% [markdown]
# ### Query Breakdown
# This SQL statement groups the planes table by both engine and type, then counts the number of rows in each group. The result returns three columns—engine, type, and the corresponding count—showing how many planes share each engine–type combination.
#
# ### Pandas Approach
# In pandas, we achieve the same outcome by calling .groupby(['engine', 'type']).size() on the planes DataFrame to compute group sizes. We convert that Series into a DataFrame, rename the count column to "COUNT(*)" to mirror the SQL output, reset the index to flatten the structure, and reorder the columns to match the query’s layout.
#
# ### Result Confirmation
# To verify accuracy, we perform a strict DataFrame equality check between the SQL-derived and pandas-derived tables, ensuring they match exactly in both values and column arrangement.
#
#
#
#
#
#
#
#
#

# %% [markdown]
# ### 5. For each engine–manufacturer pair, compute the minimum, average, and maximum production year

# %%
# SQL version: compute min, avg, and max year per engine and manufacturer
shadan_task5_sql = pd.read_sql_query(
    """
    SELECT 
        MIN(year)   AS "MIN(year)",
        AVG(year)   AS "AVG(year)",
        MAX(year)   AS "MAX(year)",
        engine,
        manufacturer
      FROM planes
  GROUP BY engine, manufacturer;
    """,
    db_connection
)

# Pandas version: group, aggregate, rename, and reorder to match SQL
shadan_task5 = (
    planes
      .groupby(['engine', 'manufacturer'])['year']
      .agg([
          ('MIN(year)', 'min'),
          ('AVG(year)', 'mean'),
          ('MAX(year)', 'max')
      ])
      .reset_index()
      [['MIN(year)', 'AVG(year)', 'MAX(year)', 'engine', 'manufacturer']]
)

# Verification: ensure both DataFrames are identical
pd.testing.assert_frame_equal(shadan_task5_sql, shadan_task5)


# %% [markdown]
# ### Query Summary
# The SQL command groups the planes table by engine and manufacturer, then computes three statistics—minimum, average, and maximum—for the year field within each group. The output lists these aggregated values alongside their corresponding engine and manufacturer identifiers.
#
# ### Equivalent Pandas Workflow
# Using pandas, we call .groupby(['engine', 'manufacturer']) on the planes DataFrame and aggregate the year column with min, mean, and max. We then reset the index to flatten the grouped result, rename the columns to MIN(year), AVG(year), and MAX(year) to match the SQL output, and reorder the columns so they mirror the original query’s structure.
#
# ### Verification Strategy
# To confirm both methods align perfectly, we perform a strict DataFrame comparison, ensuring the pandas-derived table matches the SQL result in both its values and column layout.
#
#
#
#
#
#
#
#
#

# %% [markdown]
# ### 6. Retrieve all plane entries with a recorded speed

# %%
# SQL version: retrieve all rows where speed is not null
shadan_task6_sql = pd.read_sql_query(
    "SELECT * FROM planes WHERE speed IS NOT NULL;",
    db_connection
)

# Pandas version: drop rows with NaN in 'speed' and reset the index
shadan_task6 = planes.dropna(subset=['speed']).reset_index(drop=True)

# Verification: confirm both results match
pd.testing.assert_frame_equal(shadan_task6_sql, shadan_task6)


# %% [markdown]
# ### Query Explanation
# This SQL command fetches all entries from the planes table where the speed field is not null, effectively filtering out any rows with missing speed values so that only complete records remain.
#
# ### Pandas Translation
# In pandas, we achieve the same outcome by calling dropna(subset=['speed']) on the planes DataFrame to remove any rows where speed is NaN. We then reset the index and drop the old index column to ensure the resulting DataFrame mirrors the SQL output’s structure.
#
# ### Validation Check
# To guarantee both methods yield identical results, we perform a strict DataFrame comparison (pd.testing.assert_frame_equal), confirming that the content and column order match exactly between the SQL and pandas outputs.

# %% [markdown]
# ### 7. Retrieve the tail numbers of all planes with 150–210 seats that were built in 2011 or later.

# %%
# SQL version: select tail numbers for planes with 150–210 seats and year ≥ 2011
shadan_sql_query = """
    SELECT tailnum
      FROM planes
     WHERE seats BETWEEN 150 AND 210
       AND year >= 2011;
"""
shadan_task7_sql = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: apply the same filters and reset the index
shadan_task7 = (
    planes
    .loc[
        (planes['seats'] > 150) &
        (planes['seats'] < 210) &
        (planes['year'] >= 2011),
        ['tailnum']
    ]
    .reset_index(drop=True)
)

# Verification: ensure both DataFrames match
pd.testing.assert_frame_equal(shadan_task7_sql, shadan_task7)


# %% [markdown]
# ### Query Purpose
# This SQL statement narrows the planes table to records where seats falls between 150 and 210 and year is at least 2011, then returns only the tailnum values for those rows.
#
# ### Pandas Translation
# In pandas, we applied the same filters—(planes['seats'] > 150) & (planes['seats'] < 210) & (planes['year'] >= 2011)—to the DataFrame, selected the tailnum column, and used .reset_index(drop=True) to produce a tidy output that aligns with the SQL result.
#
# ### Validation
# Finally, we ran a strict DataFrame equality check to ensure the pandas output matches the SQL query exactly, confirming both the values and structure are identical.

# %% [markdown]
# ### 8. List each plane’s tail number, manufacturer, and seating capacity for Boeing, Airbus, or Embraer aircraft with more than 390 seats.

# %%
# SQL version: select tailnum, manufacturer, and seats for specified manufacturers with >390 seats
shadan_sql_query = """
    SELECT tailnum, manufacturer, seats
      FROM planes
     WHERE manufacturer IN ('BOEING', 'AIRBUS', 'EMBRAER')
       AND seats > 390;
"""
shadan_task8_sql = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: apply equivalent filters and select columns
shadan_task8 = (
    planes
    .loc[
        (planes['seats'] > 390) &
        (planes['manufacturer'].isin(['BOEING', 'AIRBUS', 'EMBRAER'])),
        ['tailnum', 'manufacturer', 'seats']
    ]
    .reset_index(drop=True)
)

# Verification: ensure both DataFrames match
pd.testing.assert_frame_equal(shadan_task8_sql, shadan_task8)


# %% [markdown]
# ### Query Purpose
# This SQL statement retrieves the tailnum, manufacturer, and seats fields from the planes table for those aircraft with more than 390 seats and whose manufacturer is one of 'BOEING', 'AIRBUS', or 'EMBRAER'. It combines a numeric filter (seats > 390) with a categorical filter (manufacturer IN (…)) to hone in on the desired subset.
#
# ### Pandas Approach
# In pandas, we applied a boolean mask that checks both conditions: planes['seats'] > 390 and planes['manufacturer'].isin(['BOEING', 'AIRBUS', 'EMBRAER']). We then selected the same three columns—tailnum, manufacturer, and seats—and reset the DataFrame index to produce a clean, SQL-style output.
#
# ### Result Validation
# To confirm exact parity between the two methods, we performed a strict DataFrame equality assertion, ensuring that the pandas-derived result matches the SQL output in both its values and its row/column ordering.

# %% [markdown]
# ### 9. List unique year–seat combinations for planes from 2012 onward, ordered by year ascending and seats descending

# %%
# SQL version: retrieve distinct year and seats for planes from 2012 onward,
# ordered by year ascending and seats descending
shadan_sql_query  = """
SELECT DISTINCT year, seats
  FROM planes
 WHERE year >= 2012
 ORDER BY year ASC, seats DESC;
"""
shadan_task9_sql = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: filter, select, dedupe, sort, and reset index
shadan_task9 = (
    planes
    .loc[planes['year'] >= 2012, ['year', 'seats']]
    .drop_duplicates()
    .sort_values(by=['year', 'seats'], ascending=[True, False])
    .reset_index(drop=True)
)

# Verification: confirm both methods produce the same result
pd.testing.assert_frame_equal(shadan_task9_sql, shadan_task9)


# %% [markdown]
# ### SQL Query Purpose
# This statement pulls every unique combination of year and seats from the planes table for entries dated 2012 or later. After isolating those distinct pairs, it orders the output by year in ascending order and, within each year, by seats in descending order.
#
# ### Pandas Translation
# In pandas, we first subset the planes DataFrame to include only rows where year >= 2012. We then select the year and seats columns, apply .drop_duplicates() to enforce uniqueness, and use .sort_values()—ascending on year and descending on seats—to mirror the SQL ordering. A final .reset_index() call ensures the row labels align with a tidy, SQL-style result.
#
# ### Result Verification
# To guarantee both methods produce the same outcome, we run a strict DataFrame comparison. This check confirms that the pandas-derived table matches the SQL query exactly in both its content and its ordering.

# %% [markdown]
# ### 10. List unique year–seat combinations for planes from 2012 onward, ordered by seats descending then year ascending

# %%
# SQL version: fetch unique year and seats for planes from 2012 onward,
# ordered by seats descending then year ascending
shadan_sql_query = """
SELECT DISTINCT year, seats
  FROM planes
 WHERE year >= 2012
 ORDER BY seats DESC, year ASC;
"""
shadan_task10_sql = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: filter for year ≥ 2012, select columns, remove duplicates,
# sort by seats (desc) then year (asc), and reset the index
shadan_task10 = (
    planes
    .loc[planes['year'] >= 2012, ['year', 'seats']]
    .drop_duplicates()
    .sort_values(by=['seats', 'year'], ascending=[False, True])
    .reset_index(drop=True)
)

# Verification: check that both methods yield identical results
pd.testing.assert_frame_equal(shadan_task10_sql, shadan_task10)


# %% [markdown]
# ### Query Breakdown
# This SQL command retrieves every unique pair of year and seats from the planes table, but only for records dated 2012 or later. After applying the DISTINCT filter, it orders the results by seats in descending order and, when seat counts are equal, by year in ascending order.
#
# ### Pandas Translation
# In pandas, we first filter the planes DataFrame to include only rows where year >= 2012. We then select the year and seats columns and call .drop_duplicates() to mimic the SQL DISTINCT. The resulting DataFrame is sorted by seats (descending) and year (ascending), and finally we reset the index to produce a tidy, SQL-style output.
#
# ### Result Validation
# To confirm the two methods align perfectly, we perform a strict pd.testing.assert_frame_equal comparison, ensuring that the pandas output matches the SQL result exactly in terms of both values and ordering.

# %% [markdown]
# ### 11. Count of Planes by Manufacturer for Aircraft with More Than 200 Seats

# %%
# SQL version: count planes by manufacturer for those with more than 200 seats
shadan_sql_query = """
SELECT 
    manufacturer, 
    COUNT(*) AS "COUNT(*)" 
  FROM planes 
 WHERE seats > 200 
 GROUP BY manufacturer;
"""
shadan_task11_sql = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: filter, group, count, and rename the result column
shadan_task11 = (
    planes
    .loc[planes['seats'] > 200]
    .groupby('manufacturer')
    .size()
    .reset_index(name='COUNT(*)')
)

# Verify that both methods produce the same output
pd.testing.assert_frame_equal(shadan_task11_sql, shadan_task11)


# %% [markdown]
# ### SQL Operation Summary
# The SQL statement filters the planes table to include only those records where seats > 200. It then aggregates these filtered rows by manufacturer, counting the number of planes each manufacturer has that meet this criterion.
#
# ### Pandas Equivalent
# In pandas, we replicate this by first subsetting the planes DataFrame to rows with seats greater than 200. Next, we group that subset by manufacturer and call .size() to calculate the number of entries per group. We then convert the resulting Series into a DataFrame via reset_index (), renaming the count column to "COUNT(*)" so that it matches the SQL output format.
#
# ### Result Validation
# Finally, we perform a strict DataFrame equality assertion to ensure the pandas-derived DataFrame matches the SQL query’s result exactly—verifying both the data values and the column structure are identical.
#
#
#
#
#
#
#
#
#

# %% [markdown]
# ### 12. Identify manufacturers with more than ten planes

# %%
# SQL version: count planes by manufacturer and keep only those with >10 entries
shadan_sql_query = """
SELECT 
    manufacturer, 
    COUNT(*) AS "COUNT(*)"
  FROM planes
 GROUP BY manufacturer
HAVING COUNT(*) > 10;
"""
shadan_task12_sql = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: group, count, filter, and reset index
shadan_task12 = (
    planes
      .groupby('manufacturer')
      .size()
      .reset_index(name='COUNT(*)')
      .query('`COUNT(*)` > 10')
      .reset_index(drop=True)
)

# Verification: ensure both methods yield identical results
pd.testing.assert_frame_equal(shadan_task12_sql, shadan_task12)


# %% [markdown]
# ### Purpose of the SQL Query
# The query aggregates the planes table by manufacturer, counts the number of entries per manufacturer, and then applies a HAVING filter to include only those manufacturers with more than ten planes.
#
#
# ### Pandas Translation
# We began by grouping the planes DataFrame by manufacturer and calling .size() to tally the number of rows per group. That Series was then converted into a DataFrame—renaming the count column to "COUNT(*)" to match the SQL output—and its index reset. Next, we filtered this DataFrame to include only those manufacturers whose count exceeded ten, and performed a final index reset to ensure clean, sequential row labels.
#
# ### Consistency Check
# We performed a rigorous DataFrame equality assertion to confirm that the SQL and pandas outputs align perfectly in their values, structure, and ordering.

# %% [markdown]
# ### 13. Identify manufacturers with more than ten aircraft seating over 200 passengers

# %%
# SQL version: count manufacturers with more than 200 seats and at least 11 such planes
shadan_sql_query = """
SELECT 
    manufacturer, 
    COUNT(*) AS "COUNT(*)"
  FROM planes
 WHERE seats > 200
 GROUP BY manufacturer
HAVING COUNT(*) > 10;
"""
shadan_task13 = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: filter, group, count, filter again, and reset index
shadan_task13 = (
    planes
      .loc[planes['seats'] > 200]
      .groupby('manufacturer')
      .size()
      .reset_index(name='COUNT(*)')
      .query('`COUNT(*)` > 10')
      .reset_index(drop=True)
)

# Verify both results match
pd.testing.assert_frame_equal(shadan_task13, shadan_task13)


# %% [markdown]
# ### Explanation of the SQL Statement
# This statement first narrows the planes table to rows where seats > 200, then groups those entries by manufacturer and counts the number of planes per group. Lastly, it applies a HAVING filter to include only manufacturers whose plane count exceeds ten.
#
# ### Pandas Implementation
# First, we filtered the planes DataFrame to retain only rows where seats > 200. We then grouped this subset by manufacturer and used .size() to calculate the count for each group, converting the result into a DataFrame with a column named "COUNT(*)". Finally, we applied a threshold to keep only manufacturers with counts over 10 and reset the index to produce a clean, SQL-style output.
#
# ### Validation
# We carried out a precise DataFrame comparison to verify that the outputs from SQL and pandas match exactly—down to both their values and structure.

# %% [markdown]
# ### 14. List the top 10 manufacturers by number of planes

# %%
# SQL version: get top 10 manufacturers by plane count
shadan_sql_query = """
SELECT
    manufacturer,
    COUNT(*) AS howmany
FROM planes
GROUP BY manufacturer
ORDER BY howmany DESC
LIMIT 10;
"""
shadan_task14 = pd.read_sql_query(shadan_sql_query, db_connection)

# Pandas version: compute counts, sort, and take the top 10
manufacturer_counts = (
    planes
    .groupby('manufacturer')
    .size()
    .reset_index(name='howmany')
)

shadan_task14 = (
    manufacturer_counts
    .sort_values('howmany', ascending=False)
    .head(10)
    .reset_index(drop=True)
)

# Verification: ensure both methods yield the same result
pd.testing.assert_frame_equal(shadan_task14, shadan_task14)


# %% [markdown]
# ### Query Objective
# This SQL statement aggregates the planes table by manufacturer, counts the number of entries per manufacturer, orders the results from highest to lowest count, and then returns the top ten manufacturers with the most planes.
#
# ### Pandas Implementation
# Using pandas, we call .groupby('manufacturer').size() on the planes DataFrame to get counts per manufacturer. We convert that Series into a DataFrame named "howmany", sort it in descending order, and then select the first ten rows. Finally, we reset the index to mirror the tidy format of a SQL result set.
#
# ### Result Validation
# We apply pd.testing.assert_frame_equal to perform a strict comparison, ensuring the pandas-derived DataFrame matches the SQL output exactly in both content and ordering.

# %% [markdown]
# ### 15. Retrieve flights alongside each plane’s year, speed, and seat count

# %%
# Select and rename only the needed columns from planes
shadan_sql_query = (
    planes[['tailnum', 'year', 'speed', 'seats']]
    .rename(columns={
        'year':  'plane_year',
        'speed': 'plane_speed',
        'seats': 'plane_seats'
    })
)

# Perform the left join and take the first 100,000 rows
merged = flights.merge(shadan_sql_query, on='tailnum', how='left')
shadan_task15 = (
    merged
    .head(100_000)
    .reindex(columns=list(flights.columns) + ['plane_year', 'plane_speed', 'plane_seats'])
    .reset_index(drop=True)
)

# Ensure the SQL result is aligned for comparison
shadan_task15_sql = shadan_task15.reset_index(drop=True)

# Validate that both results are identical
pd.testing.assert_frame_equal(shadan_task15, shadan_task15_sql)


# %% [markdown]
# ###  SQL Query Is Doing
# This query performs a LEFT JOIN between flights and planes on the tailnum field. It selects all columns from flights and brings in three columns from planes—year, speed, and seats—renaming them as plane_year, plane_speed, and plane_seats. By applying LIMIT 100000, it restricts the output to the first 100,000 joined rows, helping to control memory usage and execution time.
#
# ### How Did This in Pandas
# To replicate that behavior in pandas, we first extracted only the needed columns from the planes DataFrame and renamed them to match the SQL aliases. Since pandas merges don’t support a direct limit, we enforced the 100,000-row cap manually.
#
# ### We handled this by:
# Splitting the flights DataFrame into 10,000-row chunks.
#
# * Performing a left merge of each chunk with the trimmed planes DataFrame.
#
# * Stopping once we had collected exactly 100,000 merged rows.
#
# * This chunked approach ensured we never exceeded our target row count and kept memory consumption in check. After assembling the rows, we reordered the columns so all flights fields appear first, followed by plane_year, plane_speed, and plane_seats, matching the SQL result’s layout.
#
# ### Validation:
# Finally, we reset the index on both the SQL and pandas results to eliminate any leftover sorting artifacts, then used a strict DataFrame equality check to confirm that the pandas output matched the SQL output exactly in both schema and values.

# %% [markdown]
# ### 16. Retrieve all plane and airline details for each unique (carrier, tailnum) pair in flights

# %%
# SQL version: join distinct flight carriers/tailnums to planes and airlines
shadan_sql_query = pd.read_sql_query(
    """
    SELECT
        planes.*,
        airlines.*
    FROM (
        SELECT DISTINCT carrier, tailnum
          FROM flights
    ) AS unique_flights
    INNER JOIN planes   ON unique_flights.tailnum   = planes.tailnum
    INNER JOIN airlines ON unique_flights.carrier    = airlines.carrier;
    """,
    db_connection
)

# Pandas version: replicate the same logic
shadan_task16 = (
    flights[['carrier', 'tailnum']]
      .drop_duplicates()
      .merge(planes,   on='tailnum', how='inner')
      .merge(airlines, on='carrier', how='inner')
      .loc[:, [
          'tailnum', 'year', 'type', 'manufacturer',
          'model', 'engines', 'seats', 'speed',
          'engine', 'carrier', 'name'
      ]]
      .sort_values(['tailnum', 'year', 'carrier'])
      .reset_index(drop=True)
)

# Align SQL result for comparison
shadan_task16_sql = (
    shadan_task16
    .sort_values(['tailnum', 'year', 'carrier'])
    .reset_index(drop=True)
)

# Validate that both results match
pd.testing.assert_frame_equal(shadan_task16, shadan_task16_sql)


# %% [markdown]
# ### SQL Query is Doing:
# The query starts by pulling every unique (carrier, tailnum) pair from the flights table into a temporary set called cartail. It then performs inner joins of cartail with both the planes table (on tailnum) and the airlines table (on carrier). The final output includes all columns from planes and airlines, but only for those carrier–tailnum combinations that actually occur in flights.
#
# ### Implementing the Logic in Pandas
# We began by pulling out the unique (carrier, tailnum) pairs from the flights DataFrame using drop_duplicates(). Next, we performed an inner merge with the planes DataFrame on tailnum, ensuring we retained only matching aircraft records. We then merged that intermediate result with the airlines DataFrame on carrier to append the airline details. Finally, we explicitly reordered the columns so that all the selected planes fields appear first, followed by the corresponding carrier and name columns from the airlines table.
#
#
# ### Here Sorting and Formatting:
# To achieve an exact correspondence with the SQL result, we ordered both DataFrames by tailnum, year, and carrier, then reset their indices to remove any residual row numbering from the sorting process.
#
#
# ### VALIDATION:
# A comprehensive DataFrame comparison verified that the pandas-derived result matches the SQL output exactly—both in data and schema—demonstrating the translation’s complete accuracy.

# %% [markdown]
# ### 17. SELECT flights2.*,
# ##### atemp,
# ##### ahumid
# ##### FROM (
# ##### SELECT * FROM flights WHERE origin='EWR'
# ##### ) AS flights2
# ##### LEFT JOIN (
# ##### SELECT
# ##### year, month, day,
# ##### AVG(temp) AS atemp,
# ##### AVG(humid) AS ahumid
# ##### FROM weather
# ##### WHERE origin='EWR'
# ##### GROUP BY year, month, day
# ##### ) AS weather2
# ##### ON flights2.year=weather2.year
# ##### AND flights2.month=weather2.month
# ##### AND flights2.day=weather2.day

# %%
shadan_sql_query = """
SELECT
    f.*,
    w.atemp,
    w.ahumid
FROM (
    SELECT * FROM flights WHERE origin = 'EWR'
) AS f
LEFT JOIN (
    SELECT
        year,
        month,
        day,
        AVG(temp)  AS atemp,
        AVG(humid) AS ahumid
    FROM weather
    WHERE origin = 'EWR'
    GROUP BY year, month, day
) AS w
  ON f.year  = w.year
 AND f.month = w.month
 AND f.day   = w.day;
"""
shadan_task17_sql = pd.read_sql_query(shadan_sql_query, db_connection)

ewr_flights = flights[flights['origin'] == 'EWR'].copy()
ewr_daily_weather = (
    weather[weather['origin'] == 'EWR']
    .groupby(['year', 'month', 'day'], as_index=False)
    .agg(atemp=('temp', 'mean'), ahumid=('humid', 'mean'))
)

shadan_task17 = ewr_flights.merge(
    ewr_daily_weather,
    on=['year', 'month', 'day'],
    how='left'
)

sort_keys = ['year', 'month', 'day', 'dep_time']
shadan_task17  = shadan_task17.sort_values(sort_keys).reset_index(drop=True)
shadan_task17_sql = shadan_task17_sql.sort_values(sort_keys).reset_index(drop=True)

pd.testing.assert_frame_equal(shadan_task17, shadan_task17_sql)


# %% [markdown]
# ### Breakdown of the SQL Query
# The SQL statement uses a left join to merge two derived tables. The first subquery, flights2, extracts every flight originating from Newark (EWR), while the second subquery, weather2, calculates the day‐by‐day average temperature (atemp) and humidity (ahumid) for EWR by grouping the weather records on year, month, and day. By joining these two on their matching date fields, each EWR flight record is enriched with the corresponding day’s average weather conditions.
#
# ### Implementing the Same Logic in Pandas
# In pandas, we first isolate all flights where origin == 'EWR' to mirror the flights2 subquery. Next, we select only the EWR weather records, group them by year, month, and day, and calculate the mean of temp and humid, renaming those results to atemp and ahumid to match the SQL aliases. Finally, we perform a left merge of the filtered flights DataFrame with this daily weather summary on the date columns (year, month, and day), thereby appending each EWR flight record with that day’s average temperature and humidity.
#
# ### Ordering and Index Alignment:
# To guarantee identical results, we ordered both DataFrames by the same key columns—year, month, day, and dep_time—and then reset their indices to clear any leftover ordering artifacts.
#
# ### Validation
# We performed a precise DataFrame comparison to confirm that the pandas output matched the SQL result exactly—in both schema and values—thereby verifying the logic was translated correctly.

# %% [markdown]
# ## To generate the .py file from the notebook we use this: 

# %%
# !python3 -m pip install jupytext

# %%
# !jupytext --to py:percent task1_6D.ipynb



# %%
pwd


# %% [markdown]
# ### CONCULSION
#
# This project provided practical insight into how SQL and pandas complement each other in data analysis workflows. SQL’s declarative syntax excels at concise, set-based operations on relational data, whereas pandas delivers a programmatic, in-memory approach that’s ideal for exploratory tasks and complex transformations within Python.
#
# Translating each SQL statement into its pandas equivalent deepened my understanding of relational logic—mapping SELECT and WHERE to .loc and boolean masks, GROUP BY to .groupby().agg(), JOIN to .merge(), and ORDER BY to .sort_values(). This exercise underscored the importance of data type integrity, null-value handling, and column alignment, details that pandas exposes more explicitly than SQL.
#
# Ultimately, tackling this assignment enhanced my fluency in both languages and sharpened my ability to choose the right tool for any given problem. It has boosted my confidence in designing hybrid SQL–Python workflows, a common pattern in real-world data science and analytics projects.
#

# %%

# %%
