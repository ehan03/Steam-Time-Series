# standard library imports
from datetime import datetime, timedelta, timezone
from functools import reduce
from json import loads

# third party imports
import pandas as pd
import requests
from fake_useragent import UserAgent

# local imports
from ..utils import BANDWIDTH_USE_DATA_PATH, SUPPORT_REQUESTS_DATA_PATH


class IngestionPipeline:
    """
    Class to handle ingestion of download and persist bandwidth usage and
    support requests data from Steam
    """

    def __init__(self):
        """
        Initialize the IngestionPipeline object
        """

        self.ua = UserAgent()

    def __get_newest_bandwidth_data(self) -> pd.DataFrame:
        """
        Get newest download bandwidth usage data from Steam
        """

        # Steam CDN is kind of cursed so the following is a workaround to get the
        # most recent data if available
        date_today_utc = datetime.now(timezone.utc).strftime("%m-%d-%Y")
        hour_now_utc = datetime.now(timezone.utc).strftime("%H")
        date_tomorrow_utc = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
            "%m-%d-%Y"
        )
        date_month_ago_utc = (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
            "%m-%d-%Y"
        )
        v1 = f"{date_today_utc}-{hour_now_utc}"
        v2 = date_today_utc
        v3 = date_tomorrow_utc
        v4 = date_month_ago_utc

        url = "https://cdn.akamai.steamstatic.com/steam/publicstats/contentserver_bandwidth_stacked.jsonp"
        candidates = [v1, v2, v3, v4]
        newest_df = None
        most_recent_timestamp = None
        for v in candidates:
            headers = {"User-Agent": self.ua.random}
            response = requests.get(url, params={"v": v}, headers=headers)
            startidx = response.text.find("(")
            endidx = response.text.find(")")
            data = loads(response.text[startidx + 1 : endidx])
            series_list = loads(data["json"])

            df_list = []
            for series in series_list:
                df_dict = {}
                region = series["label"]
                df_dict["Timestamp"] = [
                    pd.to_datetime(x[0], unit="ms") for x in series["data"]
                ]
                df_dict[region] = [int(x[1]) for x in series["data"]]
                df_list.append(pd.DataFrame(df_dict))

            df = reduce(
                lambda x, y: pd.merge(x, y, on="Timestamp", how="outer"), df_list
            )
            df = df.sort_values("Timestamp").reset_index(drop=True)
            last_timestamp = df["Timestamp"].max()

            if most_recent_timestamp is None or last_timestamp > most_recent_timestamp:
                most_recent_timestamp = last_timestamp
                newest_df = df
        assert newest_df is not None, "No data found"

        return newest_df

    def __get_newest_support_data(self) -> pd.DataFrame:
        """
        Get newest support requests data from Steam
        """

        unix_timestamp_utc = int(datetime.now(timezone.utc).timestamp())
        url = "https://store.steampowered.com/stats/supportdata.json"
        headers = {"User-Agent": self.ua.random}
        params = {"l": "english", "global_cache_version": unix_timestamp_utc}
        response = requests.get(url, headers=headers, params=params)

        series_list = response.json()
        df_list = []
        for series in series_list:
            df_dict = {}
            request_type = series["label"].replace("requests", "").strip()
            df_dict["Timestamp"] = [
                pd.to_datetime(x[0], unit="ms") for x in series["data"]
            ]
            df_dict[request_type] = [x[1] for x in series["data"]]
            df_list.append(pd.DataFrame(df_dict))

        df = reduce(lambda x, y: pd.merge(x, y, on="Timestamp", how="outer"), df_list)
        df = df.sort_values("Timestamp").reset_index(drop=True)

        return df

    def __merge_with_old(
        self, new_bandwidth_df: pd.DataFrame, new_support_df: pd.DataFrame
    ) -> None:
        """
        Merge new data with old data and save to disk
        """

        old_bandwidth_df = pd.read_csv(
            BANDWIDTH_USE_DATA_PATH, parse_dates=["Timestamp"]
        )
        end_timestamp_bandwidth_old = old_bandwidth_df["Timestamp"].max()
        start_timestamp_bandwidth_new = new_bandwidth_df["Timestamp"].min()
        end_timestamp_bandwidth_new = new_bandwidth_df["Timestamp"].max()
        if end_timestamp_bandwidth_new > end_timestamp_bandwidth_old:
            diff_minutes = (
                start_timestamp_bandwidth_new - end_timestamp_bandwidth_old
            ).total_seconds() // 60
            assert diff_minutes <= 10, "Data gap exists"

            new_bandwidth_df = new_bandwidth_df.loc[
                new_bandwidth_df["Timestamp"] > end_timestamp_bandwidth_old
            ]
            updated_bandwidth_df = pd.concat(
                [old_bandwidth_df, new_bandwidth_df], axis=0, ignore_index=True
            )
            updated_bandwidth_df.to_csv(BANDWIDTH_USE_DATA_PATH, index=False)

        old_support_df = pd.read_csv(
            SUPPORT_REQUESTS_DATA_PATH, parse_dates=["Timestamp"]
        )
        end_timestamp_support_old = old_support_df["Timestamp"].max()
        start_timestamp_support_new = new_support_df["Timestamp"].min()
        end_timestamp_support_new = new_support_df["Timestamp"].max()
        if (end_timestamp_support_new > end_timestamp_support_old) and (
            end_timestamp_support_new.hour == 7
        ):
            diff_days = (start_timestamp_support_new - end_timestamp_support_old).days
            assert diff_days <= 1, "Data gap exists"

            new_support_df = new_support_df.loc[
                new_support_df["Timestamp"] > end_timestamp_support_old
            ]
            updated_support_df = pd.concat(
                [old_support_df, new_support_df], axis=0, ignore_index=True
            )
            updated_support_df.to_csv(SUPPORT_REQUESTS_DATA_PATH, index=False)

    def run(self) -> None:
        bandwidth_df = self.__get_newest_bandwidth_data()
        support_df = self.__get_newest_support_data()
        self.__merge_with_old(bandwidth_df, support_df)
