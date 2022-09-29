import glob
import os
import sys
import pandas as pd
import time
import requests
import ast
import datetime
from multiprocessing.pool import ThreadPool

COUNT_MULTIPROCESSING = 100

# print(sys.path)

from Ivanov_r.snap.tools_fspb.db import DbPostgress
from math import radians, cos, sin, asin, sqrt

EXCEL_NAME = f'same_market'


def time_curr():
    now = datetime.datetime.now()
    curdate = now.strftime("%H:%M:%S")
    return curdate


def haversine(lat1, lon1, lat2, lon2):
    """
    Вычисляет расстояние в километрах между двумя точками, учитывая окружность Земли.
    https://en.wikipedia.org/wiki/Haversine_formula
    """

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, (lon1, lat1, lon2, lat2))

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


def get_distance(lat1, lon1, lat2, lon2):
    url = f"http://routing.openstreetmap.de/routed-car/route/v1/driving/{lat1},{lon1};{lat2},{lon2}?overview=false&alternatives=false&steps=false"

    response = requests.request("GET", url, headers={}, data={})
    # print(time.time() - t)
    # print(response.text)

    j = response.text.replace('true', 'True').replace('false', 'False')
    j = ast.literal_eval(j)
    car_distance = j['routes'][0]['legs'][0]['distance'] / 1000.0

    return car_distance


def calculate(row, procent, cnt):
    start_time1 = time.time()
    distance_calculate = pd.DataFrame()

    uuid1, uuid2, r_dist, _, _ = row

    # uuid1, uuid2, r_dist = row

    lon1 = coords[coords['uuid'] == uuid1]['lon'].item()
    lat1 = coords[coords['uuid'] == uuid1]['lat'].item()

    lon2 = coords[coords['uuid'] == uuid2]['lon'].item()
    lat2 = coords[coords['uuid'] == uuid2]['lat'].item()

    # distance_new = haversine(lon1, lat1, lon2, lat2)

    url = f"http://routing.openstreetmap.de/routed-car/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false&alternatives=false&steps=false"

    try:
        # proxyDict = {
        #     "http": 'http://194.1.240.31:999',
        # }
        response = requests.request("GET", url, headers={}, data={}, timeout=2)
    except Exception as e:
        print(e)
        return

    j = response.text.replace('true', 'True').replace('false', 'False')
    j = ast.literal_eval(j)
    car_distance = j['routes'][0]['legs'][0]['distance'] / 1000.0


    map_url = f"http://map.project-osrm.org/?z=18&center={lat1}%2C{lon1}&loc={lat1}%2C{lon1}&loc={lat2}%2C{lon2}&hl=en&alt=0&srv=0"



    final = []
    final.extend([[uuid1, uuid2, r_dist, car_distance, map_url]])
    df_final = pd.DataFrame(final)
    distance_calculate = pd.concat([df_final, distance_calculate])
    if r_dist > car_distance:
        print("-------")
        print("ERROR:", r_dist, '>', car_distance)
        print(url)
        print(map_url)
        print(lon1, lat1, lon2, lat2)
        print("-------")

    print(
        f" Процент выполнения {procent}% /// count = {cnt} /// Время расчета = {str(datetime.timedelta(minutes=time.time() - start_time1))}")
    return distance_calculate

if __name__ == '__main__':
    start_time = time.time()

    print(f"Start ={time_curr()}")

    coords = pd.read_excel("сord_date.xlsx")

    distance = pd.DataFrame()

    while 1:
        rows = pd.read_excel("same_market.xlsx")

        rows = rows.loc[rows['car_distance'].isna()]

        if len(rows) == 0:
            print("Work DONE")
            break
        rows_final = rows.copy()
        rows = rows.values.tolist()

        max_row = len(rows) / 5
        cnt = 0
        print(F'Максимальное количество пересечений {int(max_row)}')

        catPool = ThreadPool(COUNT_MULTIPROCESSING)
        date_df = []
        distance = pd.DataFrame()

        for row in rows:
            cnt += len(row) / 5
            percent = cnt / max_row
            date_df.append(catPool.apply_async(calculate, (row, percent, cnt)))

        date_df = [r.get() for r in date_df]
        distance = pd.concat(date_df)

        catPool.close()
        catPool.join()

    distance = distance.rename(
        columns={0: "from_branch", 1: "to_branch", 2: "direct_distance", 3: "car_distance", 4: "map_url"})

    distance.to_excel(f'{EXCEL_NAME}.xlsx', index=False)
    print(F'Task is done Время расчета{str(datetime.timedelta(minutes=time.time() - start_time))}')

