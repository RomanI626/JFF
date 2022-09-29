from selenium import webdriver
import tqdm as tqdm
import pandas as pd
import folium

# мы установили этот параметр Pandas, чтобы сделать все 60 столбцов видимыми:
pd.set_option('display.max_columns', 60)

acnc = pd.read_excel('datadotgov_main.xlsx', keep_default_na=False)
acnc.head()

#Выбираем только нужные нам город
mel = acnc[acnc.Town_City.str.contains('melbourne', case=False)][['ABN', 'Charity_Legal_Name', 'Address_Line_1',
                                                                  'Address_Line_2', 'Address_Line_3', 'Town_City',
                                                                  'State', 'Postcode', 'Country',
                                                                  'Date_Organisation_Established',
                                                                  'Charity_Size']].copy()

mel.head()

#добавление столбца с полным адресом
mel['Full_Address'] = mel['Address_Line_1'].str.cat( mel[['Address_Line_2', 'Address_Line_3', 'Town_City']], sep=' ')
#Удаление строк
mel = mel[~mel.Full_Address.str.contains('po box', case=False)].copy()
#Замена / на пробел, т.к. URL будет не читаемый
mel.Full_Address = mel.Full_Address.str.replace('/', ' ')

#создаем для каждого адреся свои URL
mel['Url'] = ['https://www.google.com/maps/search/' + i for i in mel['Full_Address'] ]

Url_With_Coordinates = []

option = webdriver.ChromeOptions()
prefs = {'profile.default_content_setting_values': {'images': 2, 'javascript': 2}}
option.add_experimental_option('prefs', prefs)

driver = webdriver.Chrome("*.exe", options=option)

for url in tqdm(mel.Url[:10], leave=False):
    driver.get(url)
    Url_With_Coordinates.append(driver.find_element_by_css_selector('meta[itemprop=image]').get_attribute('content'))

driver.close()

mel['Url_With_Coordinates'] = Url_With_Coordinates
mel = mel[mel.Url_With_Coordinates.str.contains('&zoom=')].copy()
mel['lat'] = [ url.split('?center=')[1].split('&zoom=')[0].split('%2C')[0] for url in mel['Url_With_Coordinates'] ]
mel['long'] = [url.split('?center=')[1].split('&zoom=')[0].split('%2C')[1] for url in mel['Url_With_Coordinates'] ]

mel.head()


