"""Functions to insert data into a PostgreSQL database."""

import pandas as pd
import psycopg2


def clean_column_names(df):
    """
    Cleans the column names of a DataFrame by applying several transformations:
    - If a column name contains 'level', it keeps only the fourth part after splitting by '_'.
    - Replaces '#' with 'num' in column names.
    - Replaces '%' with '_percent' in column names.
    - Replaces '-' with '_' in column names.
    - Converts all column names to lowercase.

    Args:
        df (pd.DataFrame): The DataFrame whose column names need to be cleaned.

    Returns:
        pd.DataFrame: The DataFrame with cleaned column names.
    """
    try:
        df.columns = [
            col.split("_")[3] if "level" in col else col for col in df.columns
        ]
        df.columns = [
            col.replace("#", "num") if "#" in col else col for col in df.columns
        ]
        df.columns = [
            col.replace("%", "_percent") if "%" in col else col for col in df.columns
        ]
        df.columns = [
            col.replace("-", "_") if "-" in col else col for col in df.columns
        ]
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        print(f"Erreur : {e}")
        raise


def check_missing_cols(
    df: pd.DataFrame, table_name: str, cur: psycopg2.extensions.cursor
) -> list:
    """
    Checks for missing columns in a DataFrame compared to a database table.

    Parameters:
    df (pd.DataFrame): The DataFrame to check.
    table_name (str): The name of the table in the database.
    cur: The database cursor used to execute SQL queries.

    Returns:
    list: A list of column names that are in the DataFrame but not in the database table.
    """
    try:
        cur.execute(
            f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table_name}';
        """
        )
        columns_df = df.columns.tolist()
        columns_table = [col[0] for col in cur.fetchall()]

        s = set(columns_table)
        temp3 = [x for x in columns_df if x not in s]

        return temp3
    except Exception as e:
        print(f"Erreur dans le check des colonnes manquantes: {e}")
        raise


def add_columns(table_name: str, col: str, cur: psycopg2.extensions.cursor) -> None:
    """
    Adds a new column to an existing table in the database.

    Parameters:
    table_name (str): The name of the table to which the column will be added.
    col (str): The name of the new column to be added.
    cur: The database cursor object used to execute the SQL command.

    Returns:
    None

    Raises:
    Exception: If there is an error executing the SQL command.
    """
    try:
        cur.execute(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMN "{col}" VARCHAR;
        """
        )
        print(f"Colonnes : {col} ajoutée avec succès !")
    except Exception as e:
        print(f"Erreur : {e}")
        raise


def create_table_from_dataframe(
    df: pd.DataFrame, table_name: str, cur: psycopg2.extensions.cursor
) -> None:
    """
    Create a table in the PostgreSQL database based on the DataFrame's columns.

    Args:
        df (pd.DataFrame): The DataFrame whose columns will be used to create the table.
        table_name (str): The name of the table to be created.
        cur (psycopg2.extensions.cursor): The cursor object for PostgreSQL.
    """
    try:
        quoted_columns = [f'"{column}" VARCHAR' for column in df.columns.tolist()]
        columns_str = ", ".join(quoted_columns)
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_str});"
        cur.execute(create_table_query)
    except Exception as e:
        print(f"Erreur : {e}")
        raise


def create_table_url(
    cur: psycopg2.extensions.cursor, conn: psycopg2.extensions.connection
) -> None:
    """
    Creates a table named 'urls_match' in the database if it does not already exist.

    Args:
        cur (Any): The database cursor object used to execute SQL commands.
        conn (Any): The database connection object used to commit or rollback transactions.
    """
    try:
        create_table_url = 'CREATE TABLE IF NOT EXISTS urls_match ("url" VARCHAR);'
        cur.execute(create_table_url)
        conn.commit()
    except Exception as e:
        print(f"Erreur dans la création de la table urls_match: {e}")
        conn.rollback()
        raise


def insert_dataframe_to_table(df: pd.DataFrame, table_name: str, cur) -> None:
    """
    Inserts the data from a pandas DataFrame into a specified SQL table.

    Args:
        df (pd.DataFrame): The DataFrame containing the data to be inserted.
        table_name (str): The name of the SQL table where the data will be inserted.
        cur: The database cursor object used to execute the SQL commands.
    """
    try:
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query = f"""
        INSERT INTO {table_name} ({'"' + '", "'.join(df.columns) + '"'}) VALUES ({placeholders})
        """
        for row in df.itertuples(index=False, name=None):
            cur.execute(insert_query, row)
    except Exception as e:
        print(f"Erreur : {e}")
        raise


def insert_to_postgresql(
    dataframe: pd.DataFrame,
    table_name: str,
    cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
) -> None:
    """
    Insert a DataFrame into a PostgreSQL table.

    This function cleans the column names of the DataFrame, creates the table if it doesn't exist,
    checks for missing columns in the table and adds them if necessary, and inserts the DataFrame
    into the table.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be inserted.
        table_name (str): The name of the table in PostgreSQL.
        cur (psycopg2.extensions.cursor): The cursor object for PostgreSQL.
        conn (psycopg2.extensions.connection): The connection object for PostgreSQL.
    """
    try:
        dataframe = clean_column_names(dataframe)
        create_table_from_dataframe(dataframe, table_name, cur)

        missing_columns = check_missing_cols(dataframe, table_name, cur)
        if missing_columns:
            print(f"Voici les colonnes manquantes {missing_columns}")
            for col_missing in missing_columns:
                add_columns(table_name, col_missing, cur)
        insert_dataframe_to_table(dataframe, table_name, cur)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Erreur dans l'insertion du matchs: {e}")
