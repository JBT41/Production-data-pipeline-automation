#-----------------#
#    Packages     #
#-----------------#
import jpype
import logging
import csv
import urllib3
import requests
import os
import keyring
from datetime import datetime
import pandas as pd
from SMTP_Helper import send
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = Path(__file__).resolve().parents[1]
today = datetime.today().strftime("%Y-%m-%d")

LOG_DIR = BASE_DIR / os.environ["LOG_DIR"]
OUTPUT_DIR = BASE_DIR / os.environ["OUTPUT_DIR"]
RAW_DIR = BASE_DIR / os.environ["RAW_DIR"]
jt400_path = BASE_DIR / os.environ["JT400_JAR"]
LOG_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)

system = os.environ["SYSTEM"]
driver = "com.ibm.as400.access.AS400JDBCDriver"
url = f"jdbc:as400://{system};translate binary=true"
cred = keyring.get_credential("AS400", None)

if not cred:
    raise RuntimeError("Missing AS400 credentials")

user = cred.username
password = cred.password

if not jpype.isJVMStarted():
    jpype.startJVM(
        classpath=[str(jt400_path)]
    )

import jaydebeapi

logging.basicConfig(
    filename=LOG_DIR / "loader.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

data_url = "https://docs.google.com/spreadsheets/d/11diXmQ4fqPklkpOmaHA-UzLGgBITlKfhVZ2WjzUpkeA/gviz/tq?tqx=out:csv&gid=0"
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

STAGING_TABLE = "JAKET.MDWV"
LIVE_TABLE    = "JAKET.MDWV_LIVE"

def download_web_visits():

    output_file = RAW_DIR / f"web_Lead_data_{today}.csv"
    resp = requests.get(data_url, verify=False)
    resp.raise_for_status()

    lines = resp.text.split("\n")

    if len(lines) < 2:
        raise ValueError("Data missing from source, download aborted")

    output_file.write_text(resp.text, encoding="utf-8")
    return output_file


def clean_data(output_file):
    cleaned_data = []
    with output_file.open("r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row[0] == "#N/A":
                continue
            if row[1] is None or row[1].strip().lower() in ["<null>", "null", ""]:
                continue
            else:
                cleaned_data.append(row)
        return cleaned_data

def save_cleaned_data(cleaned_data, output_file):
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(cleaned_data)

def fiscal():
    """
    Fetch fiscal year/week info from the database

    Returns:
        tuple: (sdyr, sdwk) from the database if found
    """

    conn = None
    cur = None

    try:
        logging.info(f"Fiscal year/week retrieval initiated")
        try:
            logging.info(f"Connecting to database")
            conn = jaydebeapi.connect(driver, url, [user, password])
            cur = conn.cursor()
            logging.info(f"Connection established.")
        except Exception as conn_err:
            logging.error(f"Connection failed: {conn_err}")
            logging.exception(f"Stacktrace")
            raise

        try:
            logging.info(f"Executing SQL query... Fetching Fiscal period...")
            sql = """
                select
                SDYR,
                SDWK
                FROM cdlivdta.bcal
                where sddte = INTEGER(TO_CHAR(CURRENT DATE, 'YYYYMMDD'))
            """
            cur.execute(sql)
            row = cur.fetchone()
            logging.info(f"SQL executed successfully, fiscal info retrieved")
            logging.info(f"Fiscal week loaded - SDYR: {row[0]}, SDWK: {row[1]}")
        except Exception as sql_err:
            logging.error(f"SQL execution failed: {sql_err}")
            logging.exception("Stacktrace")
            raise

        if not row:
            msg = f"No fiscal year/week found for {today}"
            logging.error(msg)
            raise ValueError(msg)
        sdyr = row[0]
        sdwk = row[1]

        return sdyr, sdwk
    #--- Finally block designed to run no matter what
    finally:
        if cur is not None:
            try:
                logging.info(f"Attempting to Close cursor")
                cur.close()
                logging.info(f"Cursor closed successfully")
            except Exception as cur_close_error:
                logging.error(f"Failed to close cursor: {cur_close_error}")
                logging.exception(f"Stacktrace:")
                pass

        if conn is not None:
            try:
                logging.info(f"Attempting to Close connection to database")
                conn.close()
                logging.info(f"Connection closed")
            except Exception as conn_close_error:
                logging.error(f"Failed to close connection: {conn_close_error}")
                logging.exception("Stacktrace:")
                pass

def fetch_existing_records(table,sdyr,sdwk):
    """
    Fetch live records from the database for the current fiscal year/week

    :param sdyr:
    :param sdwk:


    """
    logging.info(f"Duplicate record check initiated")
    conn = None
    cur = None
    try:
        logging.info(f"Attempting to connect to the database")
        conn = jaydebeapi.connect(driver, url, [user, password])
        cur = conn.cursor()
        logging.info(f"Connection established.")
    except Exception as conn_err:
        logging.error(f"Connection failed: {conn_err}")
        logging.exception(f"Stacktrace")
        raise

    try:
        logging.info(f"Executing SQL query... Fetching live records...")
        sql = f"""
            SELECT 
            REGCDE,
            SMSITE,
            SDYR,
            SDWK,
            MDWVYW,
            MDWVHT,
            MDWVWV,
            CRTDA2,
            CRTTIM,
            CRTUSR,
            CRTFNC,
            UPDDA2,
            UPDTIM,
            UPDUSR,
            UPDFNC,
            UPDATE_IDENT
            FROM {table}
            WHERE SDYR = ?
            and SDWK = ?
        """
        cur.execute(sql,(sdyr,sdwk))
        rows = cur.fetchall()
        return(rows)


    except Exception as SQL_ERR:
        logging.error(f"SQL execution failed: {SQL_ERR}")
        raise

    finally:
        if cur is not None:
            try:
                logging.info(f"Attempting to close Cursor")
                cur.close()
                logging.info(f"Cursor closed")
            except Exception as cur_close_error:
                logging.error(f"Failed to close cursor: {cur_close_error}")
                logging.exception(f"Stacktrace:")
                pass
        if conn is not None:
            try:
                logging.info(f"Attempting to close connection to the database")
                conn.close()
                logging.info(f"Connection closed")
            except Exception as conn_close_error:
                logging.error(f"Failed to close connection: {conn_close_error}")
                logging.exception("Stacktrace:")
                pass

def load(table,sdyr,sdwk,cleaned_file):
    logging.info(f"Data load initiated")
    conn = None
    cur = None

    rows = 0
    inserted = 0

    now = datetime.now()
    crtda2 = int(now.strftime("%Y%m%d"))
    crttim = int(now.strftime("%H%M%S"))
    crtusr = "PYLOAD"
    crtfnc = "LOAD"

    sql = f"""
        insert INTO {table} (
            REGCDE, SMSITE, SDYR, SDWK,
            MDWVYW, MDWVHT, MDWVWV, CRTDA2,
            CRTTIM, CRTUSR, CRTFNC, UPDDA2,
            UPDTIM, UPDUSR, UPDFNC, UPDATE_IDENT 

            ) VALUES (
            ?, ?, ?, ?,
            0, 0, ?, ?,
            ?, ?, ?, 0,
            0, '', '', 1 
            )
            """
    try:
        logging.info(f"Connecting to database")
        conn = jaydebeapi.connect(driver, url, [user, password])
        cur = conn.cursor()
        logging.info(f"Connection established")
    except Exception as conn_err:
        logging.error(f"Connection failed: {conn_err}")
        raise

    try:
        logging.info(f"Loading dat...")
        with open(cleaned_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:

                #--- Check if there are any blank lines in the CSV
                #-- if there are, abort the load.
                if not row:
                    logging.error("Blank line detected in CSV. Data invalid. Load aborted.")
                    raise Exception("Blank line detected in CSV. Data invalid. Load aborted.")

                rows += 1
                regcde = row[0]
                smsite = row[1]
                mdwvwv = row[2]

                params = [
                    regcde,  # REGCDE
                    smsite,  # SMSITE
                    int(sdyr),  # SDYR from fiscal()
                    int(sdwk),  # SDWK from fiscal()
                    int(mdwvwv),  # MDWVWV from CSV
                    crtda2,  # CRTDA2
                    crttim,  # CRTTIM
                    crtusr,  # CRTUSR
                    crtfnc  # CRTFNC
                    ]
                try:
                    cur.execute(sql, params)
                    inserted += 1
                except Exception as sql_err:
                    logging.error(f"Row failed: {row}, error {sql_err}")
                    raise Exception(f"Row failed:{row}, error {sql_err}")

                #--- check if the number of rows loaded into the database is = to the number of rows in the CSV file
                #-- if not, then abort. otherwise commit the insert to the database
            if inserted != rows:
                logging.error(f"INSERT COUNT MISMATCH: Inserted {inserted} rows out of {rows} CSV lines. LOAD ABORTED")
                raise Exception (f"INSERT COUNT MISMATCH: Inserted {inserted} rows out of {rows} CSV lines. LOAD ABORTED")
            conn.commit()
            logging.info(f"Inserted {inserted} rows out of {rows} ")

    except Exception as sql_err:
        logging.error(f"SQL execution failed: {sql_err}")
        raise
    finally:
        if cur is not None:
            try:
                logging.info(f"Attempting to Close cursor")
                cur.close()
                logging.info(f"Cursor closed")
            except Exception as cur_close_error:
                logging.error(f"Failed to close cursor: {cur_close_error}")
                logging.exception("Stacktrace:")
                pass
            try:
                logging.info(f"Attempting to close connection to database")
                conn.close()
                logging.info(f"Connection closed")
            except Exception as conn_close_error:
                logging.error(f"Failed to close connection: {conn_close_error}")
                logging.exception("Stacktrace:")
                pass

def transform_staging_to_df(table,sdyr,sdwk):
    """
      Fetch staging records for a fiscal period and return a normalised DataFrame
      """
    conn = jaydebeapi.connect(driver, url, [user, password])
    cur = conn.cursor()


    sql = f"""

                    SELECT 
                    REGCDE,
                    SMSITE,
                    SDYR,
                    SDWK,
                    MDWVYW,
                    MDWVHT,
                    MDWVWV
                    FROM {table}
                    WHERE SDYR = ?
                    and SDWK = ?
                """
    cur.execute(sql,(sdyr,sdwk))
    rows = cur.fetchall()

    cur.close()
    conn.close()



    cols = [
        "REGCDE",
        "SMSITE",
        "SDYR",
        "SDWK",
        "MDWVYW",
        "MDWVHT",
        "MDWVWV"
    ]

    df = pd.DataFrame(rows, columns=cols)

    # Normalise CHAR columns (Db2 CHAR padding)
    df["SMSITE"] = df["SMSITE"].astype(str).str.strip()

    # Enforce numeric columns to match DB schema
    int_cols = ["REGCDE", "SDYR", "SDWK", "MDWVYW", "MDWVHT", "MDWVWV"]
    df[int_cols] = df[int_cols].astype(int)

    return df.sort_values(["REGCDE", "SMSITE"]).reset_index(drop=True)

def transform_csv_to_df(cleaned_file, sdyr, sdwk):

    df = pd.read_csv(
        cleaned_file,
        header=None,
        names=["REGCDE", "SMSITE", "MDWVWV"]
    )

    # Inject DB context columns
    df["SDYR"] = int(sdyr)
    df["SDWK"] = int(sdwk)
    df["MDWVYW"] = 0
    df["MDWVHT"] = 0

    # Reorder to match DB SELECT
    df = df[
        ["REGCDE", "SMSITE", "SDYR", "SDWK", "MDWVYW", "MDWVHT", "MDWVWV"]
    ]

    # Only strip CHAR columns
    df["SMSITE"] = df["SMSITE"].astype(str).str.strip()

    # Enforce numeric columns explicitly
    int_cols = ["REGCDE", "SDYR", "SDWK", "MDWVYW", "MDWVHT", "MDWVWV"]
    df[int_cols] = df[int_cols].astype(int)

    return df.sort_values(["REGCDE", "SMSITE"]).reset_index(drop=True)

def load_staging_to_live(staging_table,live_table,sdyr,sdwk):
    """
        Promote validated data from staging table to live table
        for a specific fiscal year/week.
        """

    sql = f"""
    insert into {live_table}
    select * from {staging_table}
    where sdyr = ?
    and sdwk = ?
    """

    conn = None
    cur = None
    try:
        logging.info(f"Moving data from {staging_table} to {live_table} for {sdyr}, {sdwk}")
        conn = jaydebeapi.connect(driver,url,[user,password])
        cur = conn.cursor()

        cur.execute(sql,(int(sdyr),int(sdwk)))
        conn.commit()

        rows_inserted = cur.rowcount

        if rows_inserted == 0:
            raise RuntimeError(
                f"No rows inserted into {live_table} for {sdyr} and {sdwk}"
            )

    except Exception as sql_err:
        logging.error(f"Failed to promote Staging to Live: {sql_err}")
        raise
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception as cur_close_error:
                logging.error(f"Failed to close cursor: {cur_close_error}")
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception as conn_close_error:
                logging.error(f"Failed to close connection: {conn_close_error}")
                pass

def main():
    sdyr = None
    sdwk = None
    expected_rows_uploaded = None
    start_time = datetime.now()
    try:
        logging.info("Starting ETL job")

        #--Download Data from google sheets, save, clean data , save --------#
        output_file = download_web_visits()
        cleaned_rows = clean_data(output_file)
        expected_rows_uploaded = len(cleaned_rows)
        cleaned_file = OUTPUT_DIR / output_file.name.replace(".csv", "_cleaned.csv")
        save_cleaned_data(cleaned_rows, cleaned_file)

        #-- Call Fiscal to fetch fiscal week from current date
        sdyr, sdwk = fiscal()

        #---AVOID DUPLICATES - Fetch existing records from Live, if returned cancel job
        validate_rows = fetch_existing_records(STAGING_TABLE,sdyr,sdwk)
        if validate_rows:
            msg = (
                f"Duplicate MDWV records found for SDYR:{sdyr}, SDWK:{sdwk}. LOAD ABORTED!"
            )
            raise ValueError(msg)

        #-- Load CSV into stg, re-query staging, convert stg, csv to df
        load(STAGING_TABLE,sdyr,sdwk,cleaned_file)
        staging_df = transform_staging_to_df(STAGING_TABLE,sdyr,sdwk)
        csv_df = transform_csv_to_df(cleaned_file, sdyr, sdwk)

        #--Compare staging df and csv df, if they do not match cancel job
        if not staging_df.equals(csv_df):
            raise ValueError("Staging data does not match source CSV")

        #--Load the data from staging to live, send success email
        load_staging_to_live(STAGING_TABLE,LIVE_TABLE,sdyr,sdwk)
        try:
            send(success=True,
                 sdyr=sdyr,
                 sdwk=sdwk,
                 rows_loaded = expected rows_uploaded
                )
        except Exception as success_error:
            logging.exception("Failed to send Success Email", exc_info=success_err)

except Exception as mail_err:
    logging.exception("Success email failed", exc_info=mail_err)


    # Exceptions move up and get caught here, notify via SMTP script failed.
    except Exception as err:
        try:
            # Send failure email notification
            send(
                success=False,
                error=err,
                sdyr = sdyr,
                sdwk = sdwk
            )
        # Ensure if the email send fails the exception is not confused with ETL failure
        except Exception as mail_err:
            logging.exception("failed to send mail", exc_info=mail_err)
        raise

    finally:
        if jpype.isJVMStarted():
            jpype.shutdownJVM()

if __name__ == "__main__":
    main()
