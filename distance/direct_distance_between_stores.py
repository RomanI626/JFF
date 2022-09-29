import math
import os
import sys
from datetime import date

import numpy as np
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# если наш скрипт лежит глубоко (до 4 вложений), прописываем пути для поиска интерпретатору
[sys.path.append('../' * i) for i in range(5)]

# print(sys.path)

from Ivanov_r.snap.tools_fspb.db import DbPostgress
from math import radians, cos, sin, asin, sqrt

from haversine import Unit
import haversine as hs
import json

# FILE_PATH = r'' # Путь записи
EXCEL_NAME = f'same_market'  # Наименование

if __name__ == '__main__':

    rows = pd.read_excel("сord_date.xlsx")
    # просморт и проверка
    rows = rows.values.tolist()

    # шрубый расчет расстояний.
    max_distance = 100
    bank = {}
    list_data = []
    count = 0
    for uuid1, lon1, lat1 in rows:
        for uuid2, lon2, lat2 in rows:
            if uuid1 == uuid2:
                continue
            # distance = haversine(lon1, lat1, lon2, lat2)
            distance = hs.haversine((lat1, lon1), (lat2, lon2), unit=Unit.KILOMETERS)

            if distance < max_distance:
                list_data.append([uuid1, uuid2, distance, None, None])
                # if uuid1 not in bank:
                #     bank[uuid1]={uuid2:distance}
                #     count += 1
                # else:
                #     # if uuid2 not in bank[uuid1]:
                #     bank[uuid1] = {uuid2: distance}
                #     count += 1

    print("rows", len(list_data))

    df_distance = pd.DataFrame(list_data)
    df_distance = df_distance.rename(
        columns={0: "from_branch", 1: "to_branch", 2: "direct_distance", 3: "car_distance", 4: "map_url"})

    df_distance.to_excel(f'{EXCEL_NAME}.xlsx', index=False)

# table = pd.DataFrame(columns=['from_branch', 'to_branch', 'direct_distance', 'car_distance', 'map_url' ], data=list_data)

# db.write_table(table, "branches_distance", "reg")
# for row in rows:
#     print(row)
