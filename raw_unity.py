import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from io import StringIO
import base64
import logging

from airflow.models import Variable


class UnitySitesFetcher:
    def __init__(self):
        # Загружаем секреты из Airflow Variables
        self.key_id = Variable.get("unity_api_key")
        self.secret_key = Variable.get("unity_secret_key")
        self.organization_id = Variable.get("unity_organization_id")

        # Берём URL БД из Airflow Variable
        self.db_url = Variable.get("postgresql_secret")

        # Подготовка движка SQLAlchemy
        self.engine = create_engine(self.db_url)

        logging.info("UnitySitesFetcher initialized with DB URL and Unity credentials")

    def get_encoded_credentials(self) -> str:
        credentials = f"{self.key_id}:{self.secret_key}"
        return base64.b64encode(credentials.encode()).decode()

    def fetch_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        # Unity API ожидает конец периода на следующий день
        end_date_unity = (
            datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")

        url = (
            f"https://services.api.unity.com/advertise/stats/v2/"
            f"organizations/{self.organization_id}/reports/acquisitions"
        )

        params = {
            "start": start_date,
            "end": end_date_unity,
            "scale": "day",
            "appIds": (
                "...,"
                "...,"
                "...,"
                "...,"
                "..."
            ),
            "metrics": "starts,clicks,installs,spend",
            "breakdowns": "app,campaign,sourceAppId",
        }

        headers = {
            "Authorization": f"Basic {self.get_encoded_credentials()}"
        }

        logging.info(f"Requesting Unity data {start_date} → {end_date_unity}")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            logging.error(f"Unity API error {response.status_code}: {response.text}")
            return pd.DataFrame()

        logging.info("Unity API request succeeded, parsing CSV")
        df = pd.read_csv(StringIO(response.text))

        # Приводим колонки к числовому типу
        for col in ("clicks", "installs", "spend"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            logging.warning("No Unity data to process")
            return df

        # Убираем 'nan' в строковых колонках и строим ключ
        df["source app id"] = df["source app id"].replace("nan", "").astype(str)
        df["key_network"] = (
            df["timestamp"].astype(str).fillna("")
            + df["campaign name"].astype(str).fillna("")
            + df["source app id"].astype(str).fillna("")
        )

        # Оставляем только нужные колонки и отфильтровываем по расходам
        df = df[
            [
                "key_network",
                "timestamp",
                "campaign name",
                "source app id",
                "starts",
                "clicks",
                "installs",
                "spend",
            ]
        ]
        df = df[df["spend"] > 0]

        # Добавляем сетевую метку
        df["network"] = "unity"
        df["site_name_network"] = "no_data_from_unity"

        # Переименование колонок
        df = df.rename(
            columns={
                "timestamp": "day",
                "campaign name": "campaign_network",
                "source app id": "site_network",
                "starts": "impressions",
                "installs": "conversions",
            }
        )

        return df.reset_index(drop=True)

    def save_to_database(self, df: pd.DataFrame, start_date: str, end_date: str):
        if df.empty:
            logging.info("No processed data to save")
            return

        logging.info(f"Saving Unity data to DB for {start_date} → {end_date}")
        with self.engine.begin() as conn:
            # Создание таблицы, если её нет
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_ru_unity_sites_liga_betboom_pari (
                    day DATE,
                    campaign_network TEXT,
                    site_network TEXT,
                    impressions NUMERIC,
                    clicks NUMERIC,
                    conversions NUMERIC,
                    spend NUMERIC,
                    network TEXT,
                    site_name_network TEXT,
                    key_network TEXT PRIMARY KEY
                );
                """
            )
            # Удаление старых записей
            conn.execute(
                f"""
                DELETE FROM raw_ru_unity_sites_liga_betboom_pari
                WHERE day BETWEEN '{start_date}' AND '{end_date}';
                """
            )
            # Вставка новых
            df.to_sql(
                "raw_unity",
                conn,
                if_exists="append",
                index=False,
            )
        logging.info(f"Inserted {len(df)} rows.")

    def fetch_and_save(self):
        start_date = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
        end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        df = self.fetch_data(start_date, end_date)
        if not df.empty:
            df = self.process_data(df)
            self.save_to_database(df, start_date, end_date)

        logging.info("UnitySitesFetcher: fetch_and_save completed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    fetcher = UnitySitesFetcher()
    fetcher.fetch_and_save()
