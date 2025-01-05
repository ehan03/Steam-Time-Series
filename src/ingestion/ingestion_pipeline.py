# standard library imports
from datetime import datetime, timedelta, timezone
from functools import reduce
from json import loads
from typing import Optional

# third party imports
import pandas as pd
import requests
from fake_useragent import UserAgent

# local imports
from ..utils import BANDWIDTH_USE_DATA_PATH


class IngestionPipeline:
    """
    Class to handle ingestion of download and persist bandwidth usage and
    support requests data from Steam
    """

    def __init__(self) -> None:
        """
        Initialize the IngestionPipeline object
        """

        self.ua = UserAgent()
        self.regions_all = [
            "Central America",
            "Africa",
            "Middle East",
            "Oceania",
            "South America",
            "Russia",
            "Asia",
            "Europe",
            "North America",
        ]

    def __get_newest_bandwidth_data(self) -> Optional[pd.DataFrame]:
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
            regions_all_set = set(self.regions_all)
            regions_seen = set()
            for series in series_list:
                df_dict = {}
                region = series["label"]
                df_dict["Timestamp"] = [
                    pd.to_datetime(x[0], unit="ms") for x in series["data"]
                ]
                df_dict[region] = [int(x[1]) for x in series["data"]]
                regions_seen.add(region)
                df_list.append(pd.DataFrame(df_dict))

            # Make sure that all regions are present
            if regions_seen != regions_all_set:
                continue

            df = reduce(
                lambda x, y: pd.merge(x, y, on="Timestamp", how="outer"), df_list
            )
            df = df.sort_values("Timestamp").reset_index(drop=True)
            last_timestamp = df["Timestamp"].max()

            # Only keep the dataframe with the most recent timestamp
            if most_recent_timestamp is None or last_timestamp > most_recent_timestamp:
                most_recent_timestamp = last_timestamp
                newest_df = df

        return newest_df

    def __merge_with_old(self, new_bandwidth_df: pd.DataFrame) -> None:
        """
        Merge new data with old data and save to disk
        """

        # Update bandwidth data
        old_bandwidth_df = pd.read_csv(
            BANDWIDTH_USE_DATA_PATH, parse_dates=["Timestamp"]
        )
        end_timestamp_bandwidth_old = old_bandwidth_df["Timestamp"].max()
        start_timestamp_bandwidth_new = new_bandwidth_df["Timestamp"].min()
        end_timestamp_bandwidth_new = new_bandwidth_df["Timestamp"].max()

        if end_timestamp_bandwidth_new > end_timestamp_bandwidth_old:
            # Our data granularity is 10 minutes so we need consecutive data
            # points to be at most 10 minutes apart
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
            updated_bandwidth_df = (
                updated_bandwidth_df.set_index("Timestamp")
                .resample("10min")
                .asfreq()
                .reset_index()
            )
            updated_bandwidth_df[self.regions_all] = updated_bandwidth_df[
                self.regions_all
            ].astype("Int64")
            updated_bandwidth_df.to_csv(BANDWIDTH_USE_DATA_PATH, index=False)

    def run(self) -> None:
        """
        Run the ingestion pipeline
        """

        bandwidth_df = self.__get_newest_bandwidth_data()

        if bandwidth_df is not None:
            self.__merge_with_old(bandwidth_df)
        else:
            print("No new complete bandwidth data available")
