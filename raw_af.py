import requests
import pandas as pd
import io
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow.models import Variable


class AppsFlyerDataFetcher:
    def __init__(self):
        # Список приложений
        self.app_ids = [
            'app1',
            'app2',
            'app3',
            'app4'
        ]

        # Берём из Airflow Variables
        self.token_v2 = Variable.get("af_token_v2_1")

        # Заголовки для всех запросов
        self.headers = {
            "accept": "text/csv",
            "authorization": f"Bearer {self.token_v2}"
        }

        # Шаблоны URL
        self.url_template_events = (
            "https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/in_app_events_report/v5"
            "?from={start_date}&to={end_date}"
            "&maximum_rows=1000000"
            "&additional_fields=blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,"
            "custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,"
            "blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,"
            "rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,"
            "contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,"
            "blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,"
            "ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,engagement_type,"
            "gdpr_applies,ad_user_data_enabled,ad_personalization_enabled"
        )
        self.url_template_installs = (
            "https://hq1.appsflyer.com/api/raw-data/export/app/{app_id}/installs_report/v5"
            "?from={start_date}&to={end_date}"
            "&maximum_rows=1000000"
            "&additional_fields=blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,"
            "custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,"
            "blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,"
            "rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,"
            "contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,"
            "blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,"
            "ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,engagement_type,"
            "gdpr_applies,ad_user_data_enabled,ad_personalization_enabled"
        )

        # URL подключения к Postgres
        db_url = Variable.get("postgresql_sba_secret")
        self.engine = create_engine(db_url)

    def fetch_data(self, url_template: str, start_date: str, end_date: str) -> pd.DataFrame:
        df_combined = pd.DataFrame()
        for app_id in self.app_ids:
            url = url_template.format(app_id=app_id, start_date=start_date, end_date=end_date)
            resp = requests.get(url, headers=self.headers)
            if resp.status_code == 200:
                df = pd.read_csv(io.StringIO(resp.text))
                df_combined = pd.concat([df_combined, df], ignore_index=True)
                print(f"Data for {app_id} fetched successfully.")
            else:
                print(f"Failed to fetch data for {app_id}: {resp.status_code} — {resp.text}")
        return df_combined

    def process_and_save(self, start_date: str, end_date: str):
        # start_date и end_date передаются из DAG
        df_events   = self.fetch_data(self.url_template_events, start_date, end_date)
        df_installs = self.fetch_data(self.url_template_installs, start_date, end_date)

        # Склеиваем
        df_af = pd.concat([df_events, df_installs], ignore_index=True)

        df_af['client'] = None
        df_af.loc[df_af['app_id'].isin(['app1']), 'client'] = 'client1'
        df_af.loc[df_af['app_id'].isin(['app2']), 'client'] = 'client2'
        df_af.loc[df_af['app_id'].isin(['app3']), 'client'] = 'client3'

        df_af['event_name'] = df_af['event_name'].replace({
            'register': 'register',
            'conversionStep_[1]_success': 'register',
            'ftt': 'deposit',
            'ftd1': 'deposit'
        })
        valid = ['register', 'deposit', 'install']
        df_af = df_af[df_af['event_name'].isin(valid)]

        # Двоичные флаги
        for evt in ['register', 'deposit', 'install']:
            df_af[f"{evt}_binary"] = (df_af['event_name'] == evt).astype(int)

        # Оставляем только нужные поля
        df_af = df_af[[
            'appsflyer_id','client','app_id','event_name',
            'event_time','install_time','media_source','adset',
            'ad_type','campaign','site_id',
            'register_binary','deposit_binary'
        ]].copy()

        df_af.rename(columns={
            'ad_type': 'ad_type_af',
            'campaign': 'campaign_af',
            'site_id': 'site_af',
            'deposit_binary': 'deposit_af',
            'register_binary': 'reg_af',
            'install_binary': 'install_af'
        }, inplace=True)
        df_af['install_time'] = pd.to_datetime(df_af['install_time']).dt.date
        df_af['event_time']   = pd.to_datetime(df_af['event_time']).dt.date

        # Удаляем полные дубликаты
        df_af.drop_duplicates(inplace=True)

        # Сохраняем в Postgres
        with self.engine.begin() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw_af_table (
                  appsflyer_id TEXT,
                  client TEXT,
                  app_id TEXT,
                  event_name TEXT,
                  event_time DATE,
                  install_time DATE,
                  media_source TEXT,
                  adset TEXT,
                  ad_type_af TEXT,
                  campaign_af TEXT,
                  site_af TEXT,
                  reg_af INTEGER,
                  deposit_af INTEGER,
                );
            """)
            conn.execute(f"""
                DELETE FROM raw_af_table
                WHERE install_time BETWEEN '{start_date}' AND '{end_date}';
            """)
        df_af.to_sql(
            'raw_af_table',
            self.engine,
            if_exists='append',
            index=False
        )
        print(f"{len(df_af)} rows inserted into raw_af_table.")


if __name__ == "__main__":
    fetcher = AppsFlyerDataFetcher()
    fetcher.process_and_save()