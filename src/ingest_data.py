# standard library imports
import os
from datetime import datetime, timedelta, timezone
from functools import reduce
from json import loads

# third party imports
import pandas as pd
import requests
from fake_useragent import UserAgent

# local imports


def get_new_data() -> pd.DataFrame:
    """
    Get newest bandwidth usage data from Steam
    """

    date_today_utc = datetime.now(timezone.utc).strftime("%m-%d-%Y")
    hour_now_utc = datetime.now(timezone.utc).strftime("%H")
    date_tomorrow_utc = (datetime.now(timezone.utc) + timedelta(days=1)).strftime(
        "%m-%d-%Y"
    )
    v1 = f"{date_today_utc}-{hour_now_utc}"
    v2 = date_today_utc
    v3 = date_tomorrow_utc

    ua = UserAgent()
    url = "https://cdn.akamai.steamstatic.com/steam/publicstats/contentserver_bandwidth_stacked.jsonp"
    candidates = [v1, v2, v3]
    newest_df = None
    most_recent_timestamp = None
    for v in candidates:
        headers = {"User-Agent": ua.random}
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

        df = reduce(lambda x, y: pd.merge(x, y, on="Timestamp", how="outer"), df_list)
        df = df.sort_values("Timestamp").reset_index(drop=True)
        last_timestamp = df["Timestamp"].max()

        if most_recent_timestamp is None or last_timestamp > most_recent_timestamp:
            most_recent_timestamp = last_timestamp
            newest_df = df
    assert newest_df is not None, "No data found"

    return newest_df


def merge_with_old(new_df: pd.DataFrame) -> None:
    path = os.path.join(os.path.dirname(__file__), "..", "data", "bandwidths.csv")
    if not os.path.exists(path):
        new_df.to_csv(path, index=False)
        return

    old_df = pd.read_csv(path, parse_dates=["Timestamp"])

    end_timestamp_old = old_df["Timestamp"].max()
    start_timestamp_new = new_df["Timestamp"].min()
    diff_minutes = (start_timestamp_new - end_timestamp_old).total_seconds() // 60
    assert diff_minutes <= 10, "Data gap exists"

    new_df = new_df.loc[new_df["Timestamp"] > end_timestamp_old]
    df = pd.concat([old_df, new_df], axis=0, ignore_index=True, sort=False)

    df.to_csv(path, index=False)


if __name__ == "__main__":
    new_df = get_new_data()
    merge_with_old(new_df)
