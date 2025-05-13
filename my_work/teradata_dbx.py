# COMMAND ----------
dbutils.widgets.text("schema", "dp_uedw_xm", "schemaname")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821

# COMMAND ----------
import datetime
import glob
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException


# --- Configuration Loading ---
def load_env_configs(
    table_name: str = "sandbox_dev.dbx_poc.environment",
) -> List[Dict[str, Any]]:
    """
    Loads environment-specific configurations from a Databricks table.

    This function queries the specified Databricks table to retrieve environment
    settings.  It handles potential errors and ensures the data is in the
    expected format.

    Args:
        table_name (str, optional): The name of the Databricks table.
            Defaults to "sandbox_dev.dbx_poc.environment".

    Returns:
        list: A list of dictionaries containing the environment configurations.
              Returns an empty list and logs an error if the table cannot be read.
    """
    try:
        # Get the existing Spark session
        spark = SparkSession.builder.getOrCreate()
        try:
            df = spark.table(table_name)
        except AnalysisException as e:
            print(
                f"❌  Error: Table '{table_name}' not found.  Please check the table name and that the table exists. {e}"
            )
            return []

        #  Collect the data as a list of dictionaries.
        data = df.collect()
        result = []
        for row in data:
            row_dict = row.asDict()
            trimmed_dict = {}
            for key, value in row_dict.items():
                if isinstance(value, str):
                    trimmed_dict[key.strip()] = value.strip()
                else:
                    trimmed_dict[key.strip()] = value
            result.append(trimmed_dict)
        return result

    except Exception as e:
        print(
            f"❌  Error loading environment configurations from {table_name}: {e}"
        )  # More specific error
        return []


# --- SQL Syntax Normalization ---
def normalize_casts(sql: str) -> str:
    """
    Normalizes Teradata's CAST syntax to be compatible with Databricks SQL.

    Teradata and Databricks may use slightly different syntax for type casting.  This function
    replaces Teradata's CAST syntax (e.g.,  `CAST(column AS VARCHAR(10))`) with the
    equivalent Databricks syntax (`CAST(column AS STRING)`).  This ensures that type
    conversions are handled correctly in the target environment.  The function handles
    common Teradata data types.

    Args:
        sql (str): The SQL string that may contain Teradata CAST syntax.

    Returns:
        str: The SQL string with CAST syntax converted to Databricks-compatible syntax.
             If no changes are needed, the original SQL string is returned.
    """
    sql = re.sub(
        r"CAST\((.*?) AS\s+DECIMAL\(\d+,\d+\)\)",
        r"CAST(\1 AS DECIMAL(18,0))",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"CAST\((.*?) AS\s+VARCHAR\(\d+\)\)",
        r"CAST(\1 AS STRING)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"CAST\((.*?) AS\s+INTEGER\)", r"CAST(\1 AS INT)", sql, flags=re.IGNORECASE
    )
    sql = re.sub(
        r"CAST\((.*?) AS\s+SMALLINT\)",
        r"CAST(\1 AS SMALLINT)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"CAST\((.*?) AS\s+CHAR\(\d+\)\)",
        r"CAST(\1 AS STRING)",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def normalize_current_timestamp(sql: str) -> str:
    """
    Normalizes Teradata's CURRENT_TIMESTAMP(0) to Databricks' CURRENT_TIMESTAMP.
    """
    return re.sub(
        r"\bCURRENT_TIMESTAMP\(0\)\b", "CURRENT_TIMESTAMP", sql, flags=re.IGNORECASE
    )


def evaluate_concatenation(expr: str, variable_map: Dict[str, str]) -> str:
    """
    Evaluates Teradata string concatenation expressions.

    Teradata uses the '||' operator for string concatenation, which is also supported in
    Databricks. However, this function goes a step further by resolving any environment
    variables embedded within the concatenated string.  For example, if the expression
    is  `'Database: ' || strSRCDB || ', Table: ' || strTableName`, this function will
    replace `strSRCDB` and `strTableName` with their actual values from the `variable_map`.
    This is essential for dynamic SQL where table or database names are constructed at runtime.

    Args:
        expr (str): The string expression containing the '||' concatenation operator.
        variable_map (dict): A dictionary mapping environment variable names (e.g., `strSRCDB`)
            to their corresponding values (e.g., `"my_source_db"`).

    Returns:
        str: The fully evaluated string, with all environment variables replaced by their values.
             Any surrounding quotes are removed, and double single quotes are corrected.
    """
    parts = re.split(
        r"\|\|", expr
    )  # Split the expression by the concatenation operator
    result = ""
    for part in parts:
        part = part.strip()  # Remove leading/trailing whitespace
        # Remove outer single or double quotes if they exist.  This handles cases where
        # the concatenated part is a string literal.
        if (part.startswith("'") and part.endswith("'")) or (
            part.startswith('"') and part.endswith('"')
        ):
            part = part[1:-1]
        # Replace environment variables with their values (case-insensitive).
        # This is the core logic for resolving dynamic SQL components.
        for var, val in variable_map.items():
            if part.lower() == var.lower():
                part = val
        result += part
    # Fix unbalanced quotes that can occur in Teradata (e.g., "''MyString''" becomes "'MyString'")
    result = re.sub(r"''", "'", result)
    return result


# --- SQL Cleanup and Extraction Utilities ---
def remove_multiline_comments(sql: str) -> str:
    """
    Removes multiline comments from a SQL string.

    Multiline comments in Teradata (and standard SQL) are enclosed in '/*' and '*/' sequences.
    This function uses a regular expression to find and remove these comments, including
    any newlines or other characters that may be present within the comment block.

    Args:
        sql (str): The SQL string that may contain multiline comments.

    Returns:
        str: The SQL string with all multiline comments removed.
    """
    return re.sub(
        r"/\*.*?\*/", "", sql, flags=re.DOTALL
    )  # DOTALL allows . to match newlines


def correct_select_keyword(sql: str) -> str:
    """
    Corrects the Teradata 'SEL' keyword to the standard SQL 'SELECT'.

    Teradata allows 'SEL' as a shorthand for 'SELECT'.  This function ensures that all
    SQL statements use the standard 'SELECT' keyword, which is required for broader
    compatibility.  The replacement is case-insensitive.

    Args:
        sql (str): The SQL string that might contain the 'SEL' keyword.

    Returns:
        str: The SQL string with all instances of 'SEL' replaced by 'SELECT'.
    """
    return re.sub(r"\bSEL\b", "SELECT", sql, flags=re.IGNORECASE)


def replace_teradata_operators(sql: str) -> str:
    """
    Replaces Teradata's non-standard comparison operators with standard SQL operators.

    Teradata uses operators like '.LT.', '.GT.', and '.EQ.' for less-than, greater-than,
    and equal-to comparisons, respectively.  This function replaces them with the standard
    SQL operators (<, >, and =).

    Args:
        sql (str): The SQL string that may contain Teradata comparison operators.

    Returns:
        str: The SQL string with the Teradata operators replaced.
    """
    return sql.replace(".LT.", "<").replace(".GT.", ">").replace(".EQ.", "=")


def extract_job_name_from_where(sql: str) -> Optional[str]:
    """
    Extracts the 'Env_JobName' from a Teradata SQL script.

    The 'Env_JobName' variable typically indicates the specific job or process that the
    SQL script is associated with.  This function searches for the assignment of this
    variable (e.g., `Env_JobName = 'MyJobName';`) and extracts the job name.
    This is crucial for looking up the correct environment configurations.

    Args:
        sql (str): The Teradata SQL script to examine.

    Returns:
        str or None: The extracted job name, or None if the 'Env_JobName' assignment
                     is not found in the script.  The extracted name is stripped of
                     any surrounding quotes and whitespace.
    """
    match = re.search(r"Env_JobName\s*=\s*'([^']+)'", sql, flags=re.IGNORECASE)
    return match.group(1).strip() if match else None


def has_sysexecsql_call(sql: str) -> bool:
    """
    Checks for the presence of a 'CALL EDW_Env.sysexecsql(:strSQLCmd);' statement.

    This statement is commonly used in Teradata stored procedures to execute dynamic SQL
    (i.e., SQL that is constructed and executed at runtime).  The presence of this call
    indicates that the script contains dynamic SQL that needs to be extracted and processed.

    Args:
        sql (str): The Teradata SQL script to check.

    Returns:
        bool: True if the 'sysexecsql' call is found, False otherwise.
    """
    return (
        re.search(
            r"^[ \t]*CALL[ \t]+EDW_Env\.sysexecsql\(:strSQLCmd\);",
            sql,
            flags=re.MULTILINE | re.IGNORECASE,
        )
        is not None
    )


def cleanup_sql(sql: str) -> str:
    """
    Removes unnecessary logging and housekeeping statements from the SQL.

    Teradata stored procedures often include statements for logging, error handling,
    and other administrative tasks that are not relevant to the core data transformation
    logic.  This function removes these statements to produce cleaner, more focused SQL.
    It removes statements like setting counter variables, logging step descriptions,
    setting return codes, and inserting/updating log tables.  It also collapses multiple
    consecutive newlines into a single newline for better readability.

    Args:
        sql (str): The Teradata SQL script to clean.

    Returns:
        str: The cleaned Teradata SQL script.
    """
    cleanup_patterns = [
        r"(?im)^\s*SET\s+intcntr\s*=\s*intcntr\s*;",  # Remove setting of counter variable
        r"(?im)^\s*SET\s+strSTEP\s*=\s*'Step'\s*\|\|.*$",  # Remove setting of step description
        r"(?im)^\s*SET\s+ReturnCode\s*=\s*SQLSTATE\s*;",  # Remove setting of return code
        r"(?is)INSERT\s+INTO\s+DP_MEDW\.[\s\S]*?\);",  # Remove logging inserts into DP_MEDW tables
        r"(?is)UPDATE\s+DP_MEDW\.[\s\S]*?WHERE\s+spName\s*=\s*.*?;",  # Remove logging updates in DP_MEDW tables
        r"(?im)^\s*CALL\s+EDW_Env\\.sp_InsertQuery\(.*\);",  # Remove calls to EDW_Env.sp_InsertQuery procedure
        r"(?im)^\s*CALL\s+EDW_Env\\.sysexecsql\(:strSQLCmd\);",
        # Remove calls to EDW_Env.sysexecsql (will be re-extracted)
        r"\n{3,}",  # Collapse multiple newlines
    ]
    for pattern in cleanup_patterns:
        sql = re.sub(pattern, "", sql).strip()
    return sql


def extract_dynamic_sql(sql: str) -> List[str]:
    """
    Extracts dynamic SQL statements from a Teradata SQL script.

    Dynamic SQL statements are those that are constructed at runtime, typically by assigning
    a SQL string to a variable (often named `strSQLCmd`).  This function identifies and
    extracts these assignments.

    Args:
        sql (str): The Teradata SQL script to examine.

    Returns:
        list: A list of strings, where each string represents a dynamic SQL statement
              extracted from the script.  The extracted string includes the complete
              SQL expression assigned to `strSQLCmd`.
    """
    sql_pattern = re.compile(
        r"SET\s+strSQLCmd\s*=\s*((?:.|\n)*?);", re.MULTILINE | re.IGNORECASE
    )
    return sql_pattern.findall(sql)


def process_dynamic_expression(
    raw_expr: str, variable_map: Dict[str, str]
) -> Optional[str]:
    """
    Processes a single dynamic SQL expression.

    This function takes a raw SQL expression (extracted from a `SET strSQLCmd = ...;` statement),
    evaluates any string concatenations and variable substitutions, normalizes the CAST
    syntax, and ensures that the statement ends with a semicolon.  It also filters out
    expressions that are clearly related to logging or utility functions and not actual
    SQL queries.

    Args:
        raw_expr (str): The raw dynamic SQL expression to process.
        variable_map (dict): A dictionary mapping environment variable names to their values.

    Returns:
        str or None: The processed SQL statement, ready for execution, or None if the
                     expression is determined to be a logging or utility statement.
    """
    if any(
        keyword.lower() in raw_expr.lower()
        for keyword in [
            "strStatusLog",
            "strCheckPointLog",
            "sqlCmd =",
            "sp_InsertQuery",
        ]
    ):
        return None  # Skip logging and utility related SQL blocks
    evaluated = evaluate_concatenation(
        raw_expr, variable_map
    )  # Resolve environment variables
    evaluated = normalize_casts(evaluated)  # Normalize CAST syntax
    evaluated = normalize_current_timestamp(evaluated)  # Normalize CURRENT_TIMESTAMP(0)

    # Exclude INSERT and UPDATE statements targeting DP_MEDW after evaluation
    if re.match(r"^\s*INSERT\s+INTO\s+DP_MEDW\.", evaluated, re.IGNORECASE) or re.match(
        r"^\s*UPDATE\s+DP_MEDW\.", evaluated, re.IGNORECASE
    ):
        return None

    return (
        evaluated + ";" if not evaluated.endswith(";") else evaluated
    )  # Ensure semicolon at the end


def write_cleaned_sql(
    output_file: str, input_filename: str, job_name: str, sql_statements: List[str]
) -> None:
    """
    Writes the cleaned and extracted SQL statements to an output file.

    This function generates a new file containing the processed SQL, with a header that
    includes metadata like the original input file name, the timestamp of processing,
    and the associated job name.  Each extracted SQL statement is numbered for clarity.

    Args:
        output_file (str): The path to the output file where the cleaned SQL will be written.
        input_filename (str): The name of the original Teradata SQL file.
        job_name (str): The name of the job associated with the SQL statements.
        sql_statements (list): A list of cleaned SQL statements to write to the file.
    """
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"-- Cleaned SQL extracted from: {os.path.basename(input_filename)}\n")
        f.write(
            f"-- Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        )
        f.write(
            f"-- Env JobName (from SQL): {job_name}\n\n"
        )  # Indicate where job name came from
        for idx, stmt in enumerate(sql_statements, 1):
            f.write(
                f"-- Statement {idx}\n{stmt}\n\n"
            )  # Number each extracted statement


# --- Core Logic for Processing a Single SQL File ---
def extract_sql(
    input_file: str, output_file: str, env_configs_full: List[Dict[str, Any]]
) -> Tuple[str, str, float]:
    """
    Extracts and cleans SQL statements from a single Teradata SQL file.

    This function orchestrates the entire process of reading a Teradata SQL file,
    cleaning it, extracting dynamic SQL, and writing the cleaned SQL to a new file.
    It handles error handling, logging, and status reporting.

    Args:
        input_file (str): The path to the Teradata SQL file to process.
        output_file (str): The path to the file where the cleaned SQL will be written.
        env_configs_full (list): A list of environment configurations.

    Returns:
        tuple: A tuple containing:
            - str: The name of the input file.
            - str: The processing status (e.g., 'Success', 'Skipped', 'Error').
            - float: The time taken to process the file, in seconds.
    """
    start_time = time.time()
    status = "Success"

    try:
        with open(input_file, "r", encoding="utf-8") as file:
            stored_procedure = file.read()  # Read the entire SQL file content

        # Perform a series of cleaning and normalization steps
        stored_procedure = remove_multiline_comments(stored_procedure)
        stored_procedure = correct_select_keyword(stored_procedure)
        stored_procedure = replace_teradata_operators(stored_procedure)

        env_job_name_from_sql = extract_job_name_from_where(stored_procedure)
        if not env_job_name_from_sql:
            status = "Skipped (No Env_JobName in WHERE clause)"
            return input_file, status, time.time() - start_time

        # Check for the presence of dynamic SQL. If not found, skip.
        if not has_sysexecsql_call(stored_procedure):
            status = "Skipped (No sysexecsql)"
            return input_file, status, time.time() - start_time

        # Load the specific environment configuration based on the extracted job name
        environment_map = None
        for env in env_configs_full:
            if (
                env["Env_JobName"].lower() == env_job_name_from_sql.lower()
                and env["Env_Name"].lower()
                == "prod"  # Assuming Env_Name is always 'PROD'
                and env["Env_Flag"] == "T"
            ):
                environment_map = env
                break

        if not environment_map:
            status = f"Skipped (No env mapping for JobName: {env_job_name_from_sql})"
            return input_file, status, time.time() - start_time

        stored_procedure = cleanup_sql(stored_procedure)
        dynamic_sql_expressions = extract_dynamic_sql(stored_procedure)

        # Create the variable map using the loaded environment configuration
        variable_map = {
            "strEnv": environment_map.get("Env_Name", ""),
            "strEnvJOBNAME": environment_map.get("Env_JobName", ""),
            "strSRCDB": environment_map.get("Env_Src", ""),
            "strTGTDB": environment_map.get("Env_Tgt", ""),
            "strLOADDB": environment_map.get("Env_LoadDB", ""),
            "strMACDB": environment_map.get("Env_MacDB", ""),
            "strWRKDB": environment_map.get("Env_WrkDB", ""),
            "strUTLDB": environment_map.get("Env_UtlDB", ""),
            "strVDB": environment_map.get("Env_VDB", ""),
        }

        cleaned_sql_statements = [
            stmt
            for expr in dynamic_sql_expressions
            if (stmt := process_dynamic_expression(expr, variable_map))
        ]

        # If no valid SQL statements were extracted, skip writing the output file.
        if not cleaned_sql_statements:
            status = "Skipped (No valid SQL)"
            return input_file, status, time.time() - start_time

        write_cleaned_sql(
            output_file, input_file, env_job_name_from_sql, cleaned_sql_statements
        )
        return input_file, status, time.time() - start_time

    except Exception as e:
        status = "Error"
        print(f"❌  Error processing {input_file}: {e}")
        return input_file, status, time.time() - start_time


# --- Batch Processing of All SQL Files in a Folder ---
def process_all_sql_files(
    input_base_folder: str,  # Changed to input_base_folder for clarity
    env_configs_full: List[Dict[str, Any]],
    max_workers: int = 10,
) -> None:
    """
    Processes all Teradata SQL files in a given input folder.

    This function orchestrates the batch processing of multiple SQL files.  It finds all
    '.sql' files in the specified input folder, creates a timestamped output folder to
    store the processed files, and then uses a thread pool to process the files in parallel.
    It collects the results from each file processing operation and prints a summary report.

    Args:
        input_folder (str): The path to the folder containing the Teradata SQL files.
        output_base_folder (str): The base directory where processed files will be saved.
            A new subfolder with a timestamp will be created within this directory.
        env_configs_full (list): A list of environment configurations.
        max_workers (int, optional): The maximum number of threads to use for parallel
            processing.  Defaults to 10.
    """
    # Search for 'input' directories recursively
    all_input_subdirectories = glob.glob(
        os.path.join(input_base_folder, "**", "input"), recursive=True
    )

    if not all_input_subdirectories:
        print(f"  No directories ending with '/input' found under {input_base_folder}")
        sys.exit(1)

    print(f"\n Found input directories: {all_input_subdirectories}")
    print(" Starting batch processing...")

    results = []  # List to store processing results
    timestamp_str = datetime.datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )  # Create timestamped string

    for input_folder in all_input_subdirectories:
        sql_files = glob.glob(os.path.join(input_folder, "*.csv"))
        if not sql_files:
            print(f"  No .csv files found in {input_folder}")
            continue  # Move to the next input directory

        # Construct the output directory path, preserving the structure and replacing "input" with "output_{timestamp}"
        relative_path = os.path.relpath(input_folder, input_base_folder)
        output_dir = os.path.join(
            input_base_folder, relative_path.replace("input", f"output_{timestamp_str}")
        )
        os.makedirs(output_dir, exist_ok=True)  # Create the output directory

        print(f"\n Processing files in: {input_folder}")
        print(f" Output will be written to: {output_dir}")

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit each SQL file for processing
            future_to_file = {
                executor.submit(
                    extract_sql,
                    sql_file,
                    os.path.join(
                        output_dir,
                        os.path.basename(sql_file).replace(".csv", "_cleaned.txt"),
                    ),
                    env_configs_full,
                ): sql_file
                for sql_file in sql_files
            }

            # Iterate over completed futures
            for future in as_completed(future_to_file):
                file, status, duration = future.result()
                results.append((file, status, duration))

    print("\n BATCH SUMMARY ")
    print(f"{'File':<70} {'Status':<30} {'Time (sec)':>10}")
    print("-" * 115)
    for file, status, duration in results:
        print(f"{os.path.basename(file):<70} {status:<30} {duration:>10.2f}")

    # Print the total processing time
    total_time = sum(duration for _, _, duration in results)
    print(f"\n Total Time Taken: {total_time:.2f} seconds")
    print(" Batch Completed Successfully.")


# --- Main Execution ---
if __name__ == "__main__":
    # Define the base directory where the Teradata export resides
    # env = dbutils.widgets.get("env")  # Get the env widget value
    # schema_name = dbutils.widgets.get("schema")  # Get the schema widget value
    base_export_path = f"/Workspace/Tenants/data-eng/Shared/sxm_poc/{schema}/"
    env_configs_full = load_env_configs()

    # Start the batch processing, providing the base export path
    process_all_sql_files(base_export_path, env_configs_full, max_workers=10)
