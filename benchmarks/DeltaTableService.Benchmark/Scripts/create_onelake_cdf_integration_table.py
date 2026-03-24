# pyright: reportMissingImports=false, reportUndefinedVariable=false


TABLE_NAME = "delta_adbc_cdf_integration"


def print_table(table_name: str, label: str) -> None:
    print(f"\n{label}")
    spark.sql(f"SELECT id, name FROM {table_name} ORDER BY id").show(truncate=False)


spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

spark.sql(
    f"""
    CREATE TABLE {TABLE_NAME} (
        id INT,
        name STRING
    )
    USING DELTA
    TBLPROPERTIES (
        delta.enableChangeDataFeed = true,
        delta.minReaderVersion = 3,
        delta.minWriterVersion = 7,
        delta.feature.changeDataFeed = 'supported'
    )
    """
)

# Version 0: initial insert matches the local integration fixture.
spark.sql(
    f"""
    INSERT INTO {TABLE_NAME}
    VALUES
        (1, 'a'),
        (2, 'b')
    """
)
print_table(TABLE_NAME, "After version 0 insert:")

# Version 1: update id=2 -> b2.
spark.sql(f"UPDATE {TABLE_NAME} SET name = 'b2' WHERE id = 2")
print_table(TABLE_NAME, "After version 1 update:")

# Version 2: append id=3 -> c.
spark.sql(f"INSERT INTO {TABLE_NAME} VALUES (3, 'c')")
print_table(TABLE_NAME, "After version 2 append:")

print("\nCDF rows from version 1 (verification query):")
spark.sql(
    f"""
    SELECT id, name, _change_type, _commit_version, _commit_timestamp
    FROM table_changes('{TABLE_NAME}', 1)
    ORDER BY _commit_version, id, _change_type
    """
).show(truncate=False)

history = spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}")
print("\nDelta history:")
history.select("version", "timestamp", "operation").orderBy("version").show(
    truncate=False
)

print(
    f"Created CDF-enabled Delta table '{TABLE_NAME}'. "
    f"Use abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/{TABLE_NAME} "
    f"for ADBC OneLake integration tests."
)
