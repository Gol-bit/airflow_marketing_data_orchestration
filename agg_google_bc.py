import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import gspread
import json
import logging
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from datetime import datetime, timedelta


class RuBettersToBigQuery:
    def __init__(self):
        # 1) Postgres из Variable
        postgres_uri = Variable.get("postgresql_secret")
        logging.info(f"[Postgres] Connecting with URI: {postgres_uri}")
        self.engine = create_engine(postgres_uri)

        # 2) BigQuery client из Connection
        bq_hook = BigQueryHook(gcp_conn_id="google_sba_json")
        self.bigquery_client = bq_hook.get_client()
        logging.info("[BigQuery] client initialized successfully")

        # 3) gspread client из полного JSON в Variable
        raw_json = Variable.get("google_bq_secret")      # full {…} JSON
        creds_dict = json.loads(raw_json)
        self.gc = gspread.service_account_from_dict(creds_dict)
        logging.info("[Sheets] gspread client initialized successfully")

        # 4) BigQuery service-account для явных операций (если нужно)
        self.bigquery_creds = Credentials.from_service_account_info(creds_dict)
        # table ID
        self.table_id = "project.bd.table"



    def fetch_data(self):
        # Вычисляем даты
        start_date = (datetime.today() - timedelta(days=40)).strftime('%Y-%m-%d')
        end_date   = datetime.today().strftime('%Y-%m-%d')
        logging.info(f"Fetching data from PostgreSQL for period {start_date} to {end_date}...")

        def read_table_in_chunks(query, engine, name):
            chunks = []
            logging.info(f"  → Reading table {name} in chunks")
            for i, chunk in enumerate(pd.read_sql(query, engine, chunksize=10000)):
                logging.info(f"    chunk {i+1}: {chunk.shape}")
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True).drop_duplicates()
            logging.info(f"  → Total rows from {name}: {df.shape[0]}")
            return df

        df_unity = read_table_in_chunks(
            f"SELECT * FROM table WHERE day BETWEEN '{start_date}' AND '{end_date}'",
            self.engine,
            "raw_mint"
        )
        df_mint = read_table_in_chunks(
            f"SELECT * FROM table WHERE day BETWEEN '{start_date}' AND '{end_date}'",
            self.engine,
            "raw_mint"
        )
        df_af = read_table_in_chunks(
            f"SELECT * FROM table WHERE install_time BETWEEN '{start_date}' AND '{end_date}'",
            self.engine,
            "raw_mint"
        )

        # Преобразования для AF
        df_af['install_time'] = pd.to_datetime(df_af['install_time'], errors='coerce')
        df_af['event_time']   = pd.to_datetime(df_af['event_time'],   errors='coerce')
        df_af['install_af']   = (df_af['event_name'] == 'install').astype(int)
        month_start = df_af['install_time'].dt.to_period('M').dt.start_time
        month_end   = df_af['install_time'].dt.to_period('M').dt.end_time + timedelta(days=5)
        df_af = df_af[(df_af['event_time'] >= month_start) & (df_af['event_time'] <= month_end)]
        df_af['key_af'] = (
            df_af['install_time'].astype(str).fillna('') +
            df_af['campaign_af'].fillna('') +
            df_af['site_af'].fillna('')
        )
        df_af = df_af.groupby('key_af').agg({
            'install_time': 'first', 'campaign_af': 'first', 'site_af': 'first',
            'app_id': 'first', 'media_source': 'first', 'ad_type_af': 'first',
            'client': 'first', 'adset': 'first',
            'install_af': 'sum', 'reg_af': 'sum', 'bet_af': 'sum',
            'deposit_af': 'sum', 'deposit2_af': 'sum'
        }).reset_index()

        # Объединяем Unity+Mint с AF
        df_network = pd.concat([df_unity, df_mint], ignore_index=True)
        merged_df  = pd.merge(df_network, df_af,
                              left_on='key_network', right_on='key_af',
                              how='outer')

        # Приводим day
        merged_df['day'] = pd.to_datetime(merged_df['day'], errors='coerce')
        mask = merged_df['day'].isna()
        merged_df.loc[mask, 'day'] = pd.to_datetime(
            merged_df.loc[mask, 'install_time'], errors='coerce')
        logging.info("Data fetched and merged successfully.")
        return merged_df

    def fetch_payouts(self):
        logging.info("Fetching payouts from Google Sheets...")
        ss       = self.gc.open_by_key('...')
        ws       = ss.worksheet('payouts')
        data     = ws.get_all_values()
        df       = pd.DataFrame(data[1:], columns=data[0])
        df['payout'] = pd.to_numeric(df['payout'].replace('', pd.NA), errors='coerce')
        df['payout'] = df['payout'].fillna(0)
        df['month']  = df['month'].str[:3].str.lower()
        return df

    def process_cpa(self, df, df_payouts):
        logging.info("Calculating CPA...")
        payout_map = {
            (r['app_id'], r['media_source'], r['month']): r['payout']
            for _, r in df_payouts.iterrows()
        }
        df['month'] = df['day'].dt.strftime('%b').str.lower()
        df['cpa']   = df.apply(
            lambda r: payout_map.get((r['app_id'], r['media_source'], r['month']), 0),
            axis=1
        )
        return df

    def save_to_bigquery(self, merged_df):
        if merged_df.empty:
            logging.warning("No data to upload to BigQuery.")
            return

        logging.info("Uploading to BigQuery...")
        df_pay   = self.fetch_payouts()
        df_final = self.process_cpa(merged_df, df_pay)

        start_date = (datetime.today() - timedelta(days=40)).strftime('%Y-%m-%d')
        end_date   = datetime.today().strftime('%Y-%m-%d')
        delete_sql = f"""
            DELETE FROM `{self.table_id}`
            WHERE day BETWEEN '{start_date}' AND '{end_date}'
        """
        self.bigquery_client.query(delete_sql).result()
        job = self.bigquery_client.load_table_from_dataframe(
            df_final, self.table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()
        logging.info("Data successfully uploaded to BigQuery.")

    def run(self):
        try:
            merged_df = self.fetch_data()
            self.save_to_bigquery(merged_df)
        except Exception as e:
            logging.error(f"Run failed: {e}", exc_info=True)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    RuBettersToBigQuery().run()

