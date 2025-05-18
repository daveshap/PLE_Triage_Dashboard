import duckdb
import pandas as pd
import pathlib
import json
import sqlite3
import argparse
import sys


def examine_database(db_path, export_format=None):
    """Examine the contents of a DuckDB database file"""
    if not pathlib.Path(db_path).exists():
        print(f"Error: File {db_path} not found")
        return

    print(f"Examining database: {db_path}\n")

    try:
        # Connect to the database
        con = duckdb.connect(db_path, read_only=True)

        # Get list of tables
        tables = con.execute("SHOW TABLES").fetchall()
        print(f"Found {len(tables)} tables in database:")
        for i, table in enumerate(tables):
            print(f"  {i + 1}. {table[0]}")
        print()

        # If no tables, exit
        if not tables:
            print("No tables found in database.")
            return

        # Process each table
        for table_info in tables:
            table_name = table_info[0]
            print(f"===== TABLE: {table_name} =====")

            # Get detailed column info
            print("\nSchema:")
            schema_info = con.execute(f"PRAGMA table_info({table_name})").fetchall()
            for col in schema_info:
                print(f"  Column {col[0]}: {col[1]} ({col[2]}){' PRIMARY KEY' if col[5] else ''}")

            # Get row count
            row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"\nRow count: {row_count}")

            # Get all data if small table, otherwise sample
            if row_count <= 100:
                print(f"\nAll {row_count} rows:")
                df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            else:
                print(f"\nSample of data (first 10 rows):")
                df = con.execute(f"SELECT * FROM {table_name} LIMIT 10").fetchdf()

            # Print dataframe with improved formatting
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            pd.set_option('display.max_colwidth', 50)
            print(df)

            # Print basic statistics for numeric columns
            numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
            if numeric_cols:
                print("\nNumeric column statistics:")
                for col in numeric_cols:
                    stats = con.execute(f"""
                        SELECT 
                            MIN({col}), 
                            MAX({col}), 
                            AVG({col}), 
                            STDDEV({col}),
                            MEDIAN({col})
                        FROM {table_name}
                    """).fetchone()
                    print(f"  {col}:")
                    print(f"    Min: {stats[0]}")
                    print(f"    Max: {stats[1]}")
                    print(f"    Avg: {stats[2]}")
                    print(f"    StdDev: {stats[3]}")
                    print(f"    Median: {stats[4]}")

            # Categorical/text columns
            cat_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
            if cat_cols:
                print("\nCategorical/text column values:")
                for col in cat_cols:
                    unique_vals = con.execute(f"SELECT DISTINCT {col} FROM {table_name}").fetchall()
                    unique_values = [val[0] for val in unique_vals]
                    if len(unique_values) <= 20:  # Only show if not too many unique values
                        print(f"  {col}: {unique_values}")
                    else:
                        print(f"  {col}: {len(unique_values)} unique values (too many to display)")

            # Get potential relationships between tables (if more than one table)
            if len(tables) > 1:
                print("\nPotential Foreign Keys:")
                for other_table in [t[0] for t in tables if t[0] != table_name]:
                    for col in df.columns:
                        # This is a simplistic check - in a real system this would be more sophisticated
                        try:
                            count = con.execute(f"""
                                SELECT COUNT(*) FROM {table_name} t1
                                LEFT JOIN {other_table} t2 ON t1.{col} = t2.{col}
                                WHERE t2.{col} IS NOT NULL
                            """).fetchone()[0]
                            if count > 0:
                                print(f"  Possible join: {table_name}.{col} → {other_table}.{col}")
                        except:
                            pass  # Skip if error (e.g., column doesn't exist in other table)

            # Look for indexes
            try:
                indexes = con.execute(f"PRAGMA indexes").fetchall()
                if indexes:
                    print("\nIndexes:")
                    for idx in indexes:
                        print(f"  {idx}")
            except:
                pass  # Skip if error

            print("\n" + "=" * 50 + "\n")

            # Export if requested
            if export_format:
                export_data(con, table_name, export_format)

        # Close the connection
        con.close()

    except Exception as e:
        print(f"Error examining database: {str(e)}")


def export_data(con, table_name, export_format):
    """Export table data to various formats"""
    df = con.execute(f"SELECT * FROM {table_name}").fetchdf()

    if export_format == 'csv':
        file_name = f"{table_name}.csv"
        df.to_csv(file_name, index=False)
        print(f"Data exported to {file_name}")

    elif export_format == 'json':
        file_name = f"{table_name}.json"
        df.to_json(file_name, orient='records')
        print(f"Data exported to {file_name}")

    elif export_format == 'sqlite':
        file_name = f"{table_name}.sqlite"
        sqlite_con = sqlite3.connect(file_name)
        df.to_sql(table_name, sqlite_con, index=False, if_exists='replace')
        sqlite_con.close()
        print(f"Data exported to {file_name}")

    elif export_format == 'parquet':
        file_name = f"{table_name}.parquet"
        df.to_parquet(file_name, index=False)
        print(f"Data exported to {file_name}")

    elif export_format == 'excel':
        file_name = f"{table_name}.xlsx"
        df.to_excel(file_name, index=False)
        print(f"Data exported to {file_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Examine and export DuckDB database contents')
    parser.add_argument('db_path', nargs='?', default='triage.duckdb', help='Path to DuckDB database file')
    parser.add_argument('--export', choices=['csv', 'json', 'sqlite', 'parquet', 'excel'],
                        help='Export data to specified format')

    if len(sys.argv) == 1:
        # If no arguments, use defaults and don't show help
        args = parser.parse_args(['triage.duckdb'])
    else:
        args = parser.parse_args()

    examine_database(args.db_path, args.export)