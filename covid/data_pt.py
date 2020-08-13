"""
This module contains all Portugal-specific data loading and data cleaning routines.
"""
import requests
import pandas as pd
import numpy as np

idx = pd.IndexSlice

def _group_distrito(group):
    old_date = group.iloc[0]["Data_Conc"]
    
    res = group.drop("Distrito", axis=1).sum()
    res["Data"] = old_date
    return res

def get_sinave_data(offset=2000):
    """ Gets the current daily CSV from COVIDTracking """

    dfs = []
    i = 0
    
    while True:
        url = (
            "https://services.arcgis.com/CCZiGSEQbAxxFVh3/ArcGIS/rest/services/"
            "COVID19_ConcelhosDiarios/FeatureServer/0/query?"
            "where=ConfirmadosAcumulado%3E0&objectIds="
            "&geometryType=esriGeometryEnvelope&inSR="
            "&spatialRel=esriSpatialRelIntersects"
            "&resultType=none"
            "&outFields=*"
            "&returnGeometry=false"
            "&featureEncoding=esriDefault"
            "&cacheHint=true"
            f"&resultOffset={i*offset}" # use offset to paginate
            f"&resultRecordCount={offset}" # (comment above)
            "&f=pjson"
        )
        
        r = requests.get(url)
        if r.status_code != 200:
            break
        
        try:
            response = json.loads(r.content.decode("utf-8"))
            records = [_["attributes"] for _ in response["features"]]
            if len(records) == 0:
                break
            dfs.append(pd.DataFrame(records))

        except Exception as e:
            break
        
        i += 1

    # concatenate results from the several queries
    data = pd.concat(dfs, axis=0)

    # remove potential duplicate rows
    data = data.groupby(["Concelho", "Data"]).last()

    pt_data = (
        concelhos_df[cols_to_keep]
        .groupby("Distrito")
        .apply(group_distrito)
        .reset_index()
    )

    return pt_data

def process_sinave_data(data: pd.DataFrame, run_date: pd.Timestamp):
    """ Processes raw COVIDTracking data to be in a form for the GenerativeModel.
        In many cases, we need to correct data errors or obvious outliers."""

    data["Data_Conc"] = pd.to_datetime(data["Data_Conc"], unit="ms")
    data = data.rename(columns={
        "Distrito": "region",
        "Data": "date",
        "ConfirmadosAcumulado": "positive",
        #WARNING: assuming daily diff in "waiting results" = daily diff in "total tests"!!!
        #         probably BAD ASSUMPTION!!
        "AguardarResultadosLab": "total", 
    })

    # Now work with daily counts. WARNING: see comment above!
    data = data.diff().dropna().clip(0, None).sort_index()

    # Zero out any rows where positive tests equal or exceed total reported tests
    zero_filter = (data.positive >= data.total)
    data.loc[zero_filter, :] = 0

    # TODO: outlier cleaning and such should go here. zero out bad rows.

    # At the real time of `run_date`, the data for `run_date` is not yet available!
    # Cutting it away is important for backtesting!
    return data.loc[idx[:, :(run_date - pd.DateOffset(1))], ["positive", "total"]]


def get_and_process_sinave_data(run_date: pd.Timestamp):
    """ Helper function for getting and processing COVIDTracking data at once """
    data = get_sinave_data()
    data = process_sinave_data(data, run_date)
    return data
