import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import streamlit as st
from constants import CONVERSION_COST_NCGL_NCGH
import constants
from algo_framework.modules.database.util import TZ, TimeRange
# from modules.database.util import TZ, TimeRange


# def st_get_dates(days_back=7):
#     tomorrow = datetime.today() + timedelta(days=1)
#     d_minus_x = datetime.today() - timedelta(days=days_back)
#     tomorrow = pd.to_datetime(datetime(tomorrow.year, tomorrow.month, tomorrow.day)).tz_localize(TZ)
#     d_minus_x = pd.to_datetime(datetime(d_minus_x.year, d_minus_x.month, d_minus_x.day)).tz_localize(TZ)
#     start_date = st.sidebar.date_input('Start date', value=d_minus_x, key=None)
#     start_date = pd.to_datetime(start_date).tz_localize(TZ)
#     end_date = st.sidebar.date_input('End date', value=tomorrow, key=None)
#     end_date = pd.to_datetime(end_date).tz_localize(TZ)
#     time_range = TimeRange(start_date, end_date)
#     return time_range

def calc_breakeven_spread(nom_lower, expected_x_trade, flex_up, converting):
    if converting > 0:
        conversion_cost = max(expected_x_trade, nom_lower-flex_up + expected_x_trade)*CONVERSION_COST_NCGL_NCGH
    elif converting<=0:
        conversion_cost = max(max(expected_x_trade+converting,0), nom_lower-flex_up + max(expected_x_trade+converting,0))*CONVERSION_COST_NCGL_NCGH
    return conversion_cost/expected_x_trade
    
def calculate_profit(x_trades, lowers, spreads, flex_up, converting):
    profs = []
    for i in range(len(x_trades)):
        profs.append((spreads[i]-calc_breakeven_spread(lowers[i], x_trades[i], flex_up, converting))*x_trades[i])
        flex_up = flex_up - lowers[i]-x_trades[i]
        if flex_up < 0:
            converting -= flex_up
            flex_up = 0
    return sum(profs)

ALL_ALGO_NAME = 'all_algos'
