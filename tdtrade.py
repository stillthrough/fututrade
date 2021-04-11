from tda import auth, client
from tda.orders import EquityOrderBuilder, Duration, Session
import pandas as pd
import numpy as np
import config
import json


#Auth Block - If token found, proceed. If not use Selenium to log in in Chrome
try:
    c = auth.client_from_token_file(config.tda_token_path, config.tda_api_key)
except FileNotFoundError:
    from selenium import webdriver
    chrome_path = r"C:\Users\Weili\AppData\Local\Chromium\chromedriver.exe"
    with webdriver.Chrome(executable_path = chrome_path) as driver:
        c = auth.client_from_login_flow(
            driver, config.tda_api_key #Consumer Key
            , config.tda_redirect_url #Redirect URL
            , config.tda_token_path) #token path

r = c.get_price_history('AAPL',
        period_type = client.Client.PriceHistory.PeriodType.YEAR,
        period = client.Client.PriceHistory.Period.TWENTY_YEARS,
        frequency_type = client.Client.PriceHistory.FrequencyType.DAILY,
        frequency = client.Client.PriceHistory.Frequency.DAILY)
assert r.status_code == 200, r.raise_for_status()
print(json.dumps(r.json(), indent=4))


