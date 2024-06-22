# standard library imports
import os
from datetime import datetime, timezone
from functools import reduce
from json import loads

# third party imports
import pandas as pd
import requests
from fake_useragent import UserAgent

# local imports


def get_new_data() -> pd.DataFrame:
    url = "https://cdn.akamai.steamstatic.com/steam/publicstats/contentserver_bandwidth_stacked.jsonp"
    v = datetime.now(timezone.utc).strftime("%m-%d-%Y")
    ua = UserAgent()
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
        df_dict["Timestamp"] = [pd.to_datetime(x[0], unit="ms") for x in series["data"]]
        df_dict[region] = [int(x[1]) for x in series["data"]]
        df_list.append(pd.DataFrame(df_dict))

    df = reduce(lambda x, y: pd.merge(x, y, on="Timestamp", how="outer"), df_list)
    df = df.sort_values("Timestamp").reset_index(drop=True)

    return df


def merge_with_old(new_df: pd.DataFrame) -> None:
    path = os.path.join(os.path.dirname(__file__), "..", "data", "bandwidths.csv")
    if not os.path.exists(path):
        new_df.to_csv(path, index=False)
        return

    old_df = pd.read_csv(path, parse_dates=["Timestamp"])

    end_timestamp_old = old_df["Timestamp"].max()
    start_timestamp_new = new_df["Timestamp"].min()
    diff_minutes = (start_timestamp_new - end_timestamp_old).seconds // 60
    assert diff_minutes > 10, "Data gap exists"

    new_df = new_df.loc[new_df["Timestamp"] > end_timestamp_old]
    df = pd.concat([old_df, new_df], axis=0, ignore_index=True, sort=False)

    df.to_csv(path, index=False)


if __name__ == "__main__":
    new_df = get_new_data()
    merge_with_old(new_df)
