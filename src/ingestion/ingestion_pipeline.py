# standard library imports
from datetime import UTC, datetime, timedelta
from functools import reduce
from json import loads

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

    def __get_newest_bandwidth_data(self) -> pd.DataFrame | None:
        """
        Get newest download bandwidth usage data from Steam
        """

        # Steam CDN is kind of cursed so the following is a workaround to get the
        # most recent data if available
        date_today_utc = datetime.now(UTC).strftime("%m-%d-%Y")
        hour_now_utc = datetime.now(UTC).strftime("%H")
        date_tomorrow_utc = (datetime.now(UTC) + timedelta(days=1)).strftime("%m-%d-%Y")
        date_month_ago_utc = (datetime.now(UTC) - timedelta(days=30)).strftime(
            "%m-%d-%Y"
        )
        v1 = f"{date_today_utc}-{hour_now_utc}"
        v2 = date_today_utc
        v3 = date_tomorrow_utc
        v4 = date_month_ago_utc

        url = "https://cdn.akamai.steamstatic.com/steam/publicstats/contentserver_bandwidth_stacked.jsonp"
        candidates = [v1, v2, v3, v4]
        newest_df = None
        newest_key = None
        for v in candidates:
            headers = {"User-Agent": self.ua.random}
            response = requests.get(url, params={"v": v}, headers=headers)
            startidx = response.text.find("(")
            endidx = response.text.rfind(")")
            data = loads(response.text[startidx + 1 : endidx])
            series_list = loads(data["json"])

            # Take whatever regions Steam gives us, no fixed region list
            df_list = []
            for series in series_list:
                region = series["label"]
                df_list.append(
                    pd.DataFrame(
                        {
                            "Timestamp": [
                                pd.to_datetime(x[0], unit="ms") for x in series["data"]
                            ],
                            region: [int(x[1]) for x in series["data"]],
                        }
                    )
                )

            if not df_list:
                continue

            df = reduce(
                lambda x, y: pd.merge(x, y, on="Timestamp", how="outer"), df_list
            )
            df = df.sort_values("Timestamp").reset_index(drop=True)

            # Only keep the dataframe with the most recent timestamp (ties go to
            # whichever response has more regions)
            key = (df["Timestamp"].max(), len(df_list))
            if newest_key is None or key > newest_key:
                newest_key = key
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
        end_timestamp_bandwidth_new = new_bandwidth_df["Timestamp"].max()

        if end_timestamp_bandwidth_new > end_timestamp_bandwidth_old:
            new_bandwidth_df = new_bandwidth_df.loc[
                new_bandwidth_df["Timestamp"] > end_timestamp_bandwidth_old
            ]

            # concat keeps the old columns first and aligns by name. Regions that
            # are missing from the new data (e.g. Russia) become NA, and any
            # brand new regions get added as new columns.
            updated_bandwidth_df = (
                pd.concat(
                    [old_bandwidth_df, new_bandwidth_df], axis=0, ignore_index=True
                )
                .sort_values("Timestamp")
                .reset_index(drop=True)
            )
            value_cols = [c for c in updated_bandwidth_df.columns if c != "Timestamp"]
            updated_bandwidth_df[value_cols] = updated_bandwidth_df[value_cols].astype(
                "Int64"
            )

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
