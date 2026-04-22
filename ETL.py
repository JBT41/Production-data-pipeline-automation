#-----------------#
#    Packages     #
#-----------------#

import jaydebeapi
import jpype
import logging
import csv
import urllib3
import requests
import os
from datetime import datetime
import pandas as pd
from SMTP_PLACEHOLDER import send   # <-- redacted placeholder

#-----------------#
# logging configs #
#-----------------#
logging.basicConfig(
    filename=r"C:\path\to\logs\loader.log",   # REDACTED
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

#---------------------#
# Environment configs #
#---------------------#

output_folder = r"C:\path\to\output"          # REDACTED
today = datetime.today().strftime("%Y-%m-%d")
jt400_path = r"C:\path\to\drivers\jt400.jar"  # REDACTED

system = "<DB_SYSTEM_HOST>"
user = "<DB_USERNAME>"
password = "<DB_PASSWORD>"

url = f"jdbc:as400://{system};translate binary=true"
driver = "com.ibm.as400.access.AS400JDBCDriver"

STAGING_TABLE = "<SCHEMA>.STAGING_TABLE"
LIVE_TABLE    = "<SCHEMA>.LIVE_TABLE"

data_url = "https://example.com/source.csv"   # REDACTED
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def download_web_visits():
    os.makedirs(output_folder, exist_ok=True)

    output_file = os.path.join(output_folder, f"web_lead_data_{today}.csv")
    resp = requests.get(data_url, verify=False)
    resp.raise_for_status()

    lines = resp.text.split("\n")

    if len(lines) >= 2:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(resp.text)
        return output_file
    else:
        raise ValueError("Data Missing from source, Download aborted")


def clean_data(output_file):
    cleaned_data = []
    with open(output_file, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if row[0] == "#N/A":
                continue
            if row[1] is None or row[1].strip().lower() in ["<null>", "null", ""]:
                continue
            cleaned_data.append(row)
    return cleaned_data


def save_cleaned_data(cleaned_data, output_file):
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(cleaned_data)


def fiscal():
    conn = None
    cur = None

    try:
        logging.info("Fiscal year/week retrieval initiated")
        conn = jaydebeapi.connect(driver, url, [user, password], jt400_path)
        cur = conn.cursor()

        sql = """
            SELECT
                COL_YEAR,
                COL_PERIOD
            FROM <SCHEMA>.FISCAL_CALENDAR
            WHERE DATE_KEY = INTEGER(TO_CHAR(CURRENT DATE, 'YYYYMMDD'))
        """
        cur.execute(sql)
        row = cur.fetchone()

        if not row:
            raise ValueError(f"No fiscal year/week found for {today}")

        return row[0], row[1]

    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                logging.exception("Failed to close cursor")

        if conn is not None:
            try:
                conn.close()
            except Exception:
                logging.exception("Failed to close connection")


def fetch_existing_records(table, sdyr, sdwk):
    logging.info("Duplicate record check initiated")
    conn = None
    cur = None

    try:
        conn = jaydebeapi.connect(driver, url, [user, password], jt400_path)
        cur = conn.cursor()

        sql = f"""
            SELECT *
            FROM {table}
            WHERE COL_YEAR = ?
              AND COL_PERIOD = ?
        """
        cur.execute(sql, (sdyr, sdwk))
        return cur.fetchall()

    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                logging.exception("Failed to close cursor")

        if conn is not None:
            try:
                conn.close()
            except Exception:
                logging.exception("Failed to close connection")


def load(table, sdyr, sdwk, cleaned_file):
    logging.info("Data load initiated")
    conn = None
    cur = None
    rows = 0
    inserted = 0

    now = datetime.now()
    crtda2 = int(now.strftime("%Y%m%d"))
    crttim = int(now.strftime("%H%M%S"))

    sql = f"""
        INSERT INTO {table} (
            COL_KEY_1,
            COL_KEY_2,
            COL_YEAR,
            COL_PERIOD,
            METRIC_A,
            METRIC_B,
            METRIC_C,
            CREATED_DATE,
            CREATED_TIME,
            CREATED_BY,
            CREATED_FUNCTION,
            UPDATED_DATE,
            UPDATED_TIME,
            UPDATED_BY,
            UPDATED_FUNCTION,
            BATCH_ID
        ) VALUES (
            ?, ?, ?, ?,
            0, 0, ?, ?,
            ?, 'ETLJOB', 'LOAD',
            0, 0, '', '', 1
        )
    """

    try:
        conn = jaydebeapi.connect(driver, url, [user, password], jt400_path)
        cur = conn.cursor()

        with open(cleaned_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    raise Exception("Blank line detected in CSV")

                rows += 1
                params = [
                    row[0],
                    row[1],
                    sdyr,
                    sdwk,
                    int(row[2]),
                    crtda2,
                    crttim
                ]
                cur.execute(sql, params)
                inserted += 1

        if inserted != rows:
            raise Exception("INSERT COUNT MISMATCH")

        conn.commit()
        logging.info(f"Inserted {inserted} rows out of {rows}")

    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                logging.exception("Failed to close cursor")

        if conn is not None:
            try:
                conn.close()
            except Exception:
                logging.exception("Failed to close connection")


def transform_staging_to_df(table, sdyr, sdwk):
    conn = jaydebeapi.connect(driver, url, [user, password], jt400_path)
    cur = conn.cursor()

    sql = f"""
        SELECT
            COL_KEY_1,
            COL_KEY_2,
            COL_YEAR,
            COL_PERIOD,
            METRIC_A,
            METRIC_B,
            METRIC_C
        FROM {table}
        WHERE COL_YEAR = ?
          AND COL_PERIOD = ?
    """
    cur.execute(sql, (sdyr, sdwk))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    cols = [
        "COL_KEY_1",
        "COL_KEY_2",
        "COL_YEAR",
        "COL_PERIOD",
        "METRIC_A",
        "METRIC_B",
        "METRIC_C"
    ]

    df = pd.DataFrame(rows, columns=cols)
    df["COL_KEY_2"] = df["COL_KEY_2"].astype(str).str.strip()

    int_cols = ["COL_KEY_1", "COL_YEAR", "COL_PERIOD", "METRIC_A", "METRIC_B", "METRIC_C"]
    df[int_cols] = df[int_cols].astype(int)

    return df.sort_values(["COL_KEY_1", "COL_KEY_2"]).reset_index(drop=True)


def transform_csv_to_df(cleaned_file, sdyr, sdwk):
    df = pd.read_csv(cleaned_file, header=None, names=["COL_KEY_1", "COL_KEY_2", "METRIC_C"])
    df["COL_YEAR"] = int(sdyr)
    df["COL_PERIOD"] = int(sdwk)
    df["METRIC_A"] = 0
    df["METRIC_B"] = 0

    df = df[
        ["COL_KEY_1", "COL_KEY_2", "COL_YEAR", "COL_PERIOD", "METRIC_A", "METRIC_B", "METRIC_C"]
    ]

    df["COL_KEY_2"] = df["COL_KEY_2"].astype(str).str.strip()
    df[["COL_KEY_1", "COL_YEAR", "COL_PERIOD", "METRIC_A", "METRIC_B", "METRIC_C"]] = \
        df[["COL_KEY_1", "COL_YEAR", "COL_PERIOD", "METRIC_A", "METRIC_B", "METRIC_C"]].astype(int)

    return df.sort_values(["COL_KEY_1", "COL_KEY_2"]).reset_index(drop=True)


def load_staging_to_live(staging_table, live_table, sdyr, sdwk):
    sql = f"""
        INSERT INTO {live_table}
        SELECT *
        FROM {staging_table}
        WHERE COL_YEAR = ?
          AND COL_PERIOD = ?
    """

    conn = None
    cur = None
    try:
        conn = jaydebeapi.connect(driver, url, [user, password], jt400_path)
        cur = conn.cursor()
        cur.execute(sql, (sdyr, sdwk))
        conn.commit()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def main():
    sdyr = None
    sdwk = None
    expected_rows_uploaded = None

    try:
        logging.info("Starting ETL job")

        output_file = download_web_visits()
        cleaned_rows = clean_data(output_file)
        expected_rows_uploaded = len(cleaned_rows)

        cleaned_file = output_file.replace(".csv", "_cleaned.csv")
        save_cleaned_data(cleaned_rows, cleaned_file)

        sdyr, sdwk = fiscal()

        validate_rows = fetch_existing_records(STAGING_TABLE, sdyr, sdwk)
        if validate_rows:
            raise ValueError(
                f"Duplicate records found for year={sdyr}, period={sdwk}"
            )

        load(STAGING_TABLE, sdyr, sdwk, cleaned_file)

        staging_df = transform_staging_to_df(STAGING_TABLE, sdyr, sdwk)
        csv_df = transform_csv_to_df(cleaned_file, sdyr, sdwk)

        if not staging_df.equals(csv_df):
            raise ValueError("Staging data does not match source CSV")

        load_staging_to_live(STAGING_TABLE, LIVE_TABLE, sdyr, sdwk)

        send(success=True, sdyr=sdyr, sdwk=sdwk, rows_loaded=expected_rows_uploaded)

    except Exception as err:
        try:
            send(success=False, error=err, sdyr=sdyr, sdwk=sdwk)
        except Exception:
            logging.exception("Failed to send failure notification")
        raise


if __name__ == "__main__":
    main()
