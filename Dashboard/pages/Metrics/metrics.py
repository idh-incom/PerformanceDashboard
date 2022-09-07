# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 17:33:48 2022

@author: idh
"""

import pandas.io.sql as psql
import pandas as pd
import streamlit as st
import datetime as dt
import numpy as np
from streamlit_autorefresh import st_autorefresh

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine
from typing import cast

import warnings
import matplotlib.pyplot as plt
plt.style.use("ggplot")

APP_AUTHOR = 'IDH'
APP_NAME = 'daily_value_traded'
bi_sql_engine: Engine = cast(Engine, create_engine(
    URL.create(
        "mssql+pyodbc",
        username="sa_Algo",
        password="vqYJVXJBkE6n",
        host="inco-bisql.in-commodities.local",
        database="Inco",
        query={
            "driver": "ODBC Driver 17 for SQL Server",
            "autocommit": "True",
            "ApplicationIntent": "readonly",
            "App": f"Author: {APP_AUTHOR}, Team: GAS, Platform: Py, Solution: {APP_NAME}."
        },
    ),
    pool_size=3,
    max_overflow=5,
))

daymap = {0: "Mon", 1: "Tue", 2: "Wed",
          3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
indmap = {True: "Weekday", False: "Weekend"}


def fetch_traybot_volume_vals():
    query= f"""
            Select concat_ws('-', f.YearCet, f.MonthCet, f.DayCet) as [Date], f.Execution, sum(f.[Value]) as NominalValue, sum(f.[Volume]) as Volume from
            (SELECT
                  FORMAT(datepart(year, TradeTimestampCet), 'D4') as YearCet
                  , FORMAT(datepart(month, TradeTimestampCet), 'D2') as MonthCet
                  , FORMAT(datepart(day, TradeTimestampCet), 'D2') as DayCet
             --     ,[TraderId]
            	  ,abs(TotalPrice) as [Value]
            	  ,abs(TotalQuantityMWh) as [Volume]
            	  ,case when traderID in ('17688', '19118') then 'Traybot' else 'Manual' end as [Execution]
              FROM [pub].[GasTransactions]
              where bookId in (3001, 3002, 3015, 3017, 3027,3028,3029)
              and TradeType = 1
              and traderID in ('17485', --RRH
                           '17688', --TraYBOT,
                           '17736', --IST
                           '19118', --: "Traybot",
                           '19490', --: "VAS",
                           '21132', --: "MML",
                           '21566', --: "PKG",
                           '21755', --: "MLI",
                           '24924', --: "TLU",
                           '25215', --: "IDH",
                           '26457', --: "JPO",
                           '27338', --: "GBI"
                           '28081',--: "SWO"
                           '28638' --; TAA
                           )
            	and Currency != 'GBP') f  -- do not want to include NBP here for now. Too much effort
            group by YearCet, MonthCet, DayCet, Execution
            order by [Date], Execution
          """
    values = psql.read_sql_query(query,bi_sql_engine)
    return values

def plot_traybot_usage():
    fetch_traybot_usage_data = st.button("Fetch usage data")
    now = pd.to_datetime(dt.datetime.utcnow()).tz_localize("UTC").tz_convert("Europe/Berlin")
    if 'last_usage_refresh' not in st.session_state:
        st.session_state.last_usage_refresh = pd.to_datetime('2022-01-01').tz_localize("UTC").tz_convert("Europe/Berlin")
    conds = (fetch_traybot_usage_data, (np.array([x in st.session_state for x in ['usage_data', 'usage_ma', 'last_usage_refresh']]) == False).any(), (now - st.session_state.last_usage_refresh).days > 0)
    if any(conds):
        st.write("refreshing data, please wait...")
        df = fetch_traybot_volume_vals()
        df["Date"] = df["Date"].apply(pd.to_datetime)
        df2 = df.pivot(index='Date', columns='Execution', values=['NominalValue', "Volume"]).fillna(0)
        df2.index = df2.index.tz_localize("UTC").tz_convert("Europe/Berlin")
        df2["value_percentage_manual"] = df2.iloc[:,0]/(df2.iloc[:,:2].sum(axis=1))
        df2["value_percentage_traybot"] = df2.iloc[:,1]/(df2.iloc[:,:2].sum(axis=1))
        df2["volume_percentage_manual"] = df2.iloc[:,2]/(df2.iloc[:,2:4].sum(axis=1))
        df2["volume_percentage_traybot"] = df2.iloc[:,3]/(df2.iloc[:,2:4].sum(axis=1))
        moving_avg = df2.rolling(7).mean()
        moving_avg.columns = ["Manual value", "Traybot value", "Manual Volume",
                              "Traybot Volume", "Man_pct_value", "TB_pct_value",
                              "Man_pct_volume", "TB_pct_volume"]
        df2.columns = ["Manual value", "Traybot value", "Manual Volume",
                              "Traybot Volume", "Man_pct_value", "TB_pct_value",
                              "Man_pct_volume", "TB_pct_volume"]
        st.session_state.usage_data = df2.copy()
        st.session_state.usage_ma = moving_avg.copy()
        st.session_state.last_usage_refresh = pd.to_datetime(dt.datetime.utcnow()).tz_localize("UTC").tz_convert("Europe/Berlin")
    st.write("last refresh", st.session_state.last_usage_refresh.strftime("%Y-%m-%d %H:%M:%S"))
    fig, axs = plt.subplots(2,1, sharex=True)
    [st.session_state.usage_ma.loc["2021":].iloc[:,x:x+2].plot(ax=axs[i]) for i,x in enumerate([2,6])]
    fig.suptitle("7 day moving average of traybot usage (absolute/relative)")
    l,r = st.beta_columns(2)
    with l:
        st.pyplot(fig)
    with r:
        st.dataframe(st.session_state.usage_data.iloc[:,[2,3,6,7]].tail(15), height=800)

# a.to_csv("Daily_values_per_us_or_not.csv", index=False)
# a = pd.read_csv("Daily_values_per_us_or_not.csv")

def grab_trades_data(from_date):
    sq = f"""
    select s.Date, s.isOwnData, s.FirstSequenceItemName as [Venue], sum(val) as [Volume]
    from
    (SELECT a.*,
     	  CONVERT(date, a.DateTimeCet) as [Date]
     	  , (25 - datepart(hour, a.DateTimeCet)) % 24 + 1 as RoDHours
     	  , case when InstName like '%PEG%' then Volume
           when InstName like '%NBP%' then 29.3071*Volume
           when FirstSequenceItemID in (2,6,7) then Volume*24
     	  when FirstSequenceItemID = 1 then Volume*((25 - datepart(hour, a.DateTimeCet)) % 24 + 1)
     	  when FirstSequenceItemID = 4 then Volume*48
     	  else Volume
     	  end as [val]
     	  from
     	  (
     			SELECT [Action]
     			  ,[DateTime]
     			  ,[Price]
     			  ,[Volume]
     			  ,[IsMarketData]
     			  ,[IsOwnData]
     			  ,[InstID]
     			  ,[InstName]
     			  ,[FirstSequenceItemID]
                  , case when InstId in ('10000310', '10000312') then 'Hourlies' else [FirstSequenceItemName] end as [FirstSequenceItemName]
     			  ,[Unit]
     			  , cast(DateTime at TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time' as datetime) as DateTimeCet
    		  FROM [pub].[Trayport_Trades_Public]
      where DateTime > '{from_date}'
      --and SeqSpan = 'Single'
      and ((InstId in ('10000310', '10000312')) or (FirstSequenceItemID in (1, 2,4,6,7,30)))
      --and FirstSequenceItemId = 2
      ) a) s
      group by isOwnData, [Date], FirstSequenceItemName
    """
    df = psql.read_sql_query(sq, bi_sql_engine)
    return df

def plot_market_share_data():
    renew_trades_data = st.button("Fetch usage data")
    now = pd.to_datetime(dt.datetime.utcnow()).tz_localize("UTC").tz_convert("Europe/Berlin")
    if 'last_share_refresh' not in st.session_state:
        st.session_state.last_share_refresh = pd.to_datetime('2021-01-01').tz_localize("UTC").tz_convert("Europe/Berlin")
    conds = (renew_trades_data, (np.array([x in st.session_state for x in ['share_data', 'volume_data', 'last_share_refresh']]) == False).any(), (now - st.session_state.last_usage_refresh).total_seconds() > 300)
    if any(conds):
        if 'trades_data' not in st.session_state:
            st.session_state.trades_data = grab_trades_data(st.session_state.last_share_refresh)
        else:
            st.session_state.trades_data = pd.concat([st.session_state.trades_data,grab_trades_data(st.session_state.last_share_refresh)])
        
        st.write(st.session_state.trades_data)
        venues = np.array([x for x in st.session_state.trades_data.Venue.unique()])
        products_to_include = np.array([st.checkbox(x) for x in venues])
        a = st.session_state.trades_data.loc[st.session_state.trades_data.Venue.isin(products_to_include)]
        b = a.pivot(index='Date', columns = 'isOwnData', values='Value').reset_index()
        b.columns = ["Date","Market_value", "Our_value"]
        b["percentage_InCo"] = b["Our_value"]/b[["Market_value","Our_value"]].sum(axis=1)
        b.Date = b.Date.apply(pd.to_datetime)
        
        b["dayofweek"] = b.Date.dt.dayofweek
        b["weekday"] = b["dayofweek"] < 5
        b["weekday"] = b["weekday"].apply(lambda x: indmap[x])
        b["dayofweek"] = b["dayofweek"].apply(lambda x: daymap[x])

        b = b.set_index("Date")
        b = b.sort_index()
        b.percentage_InCo.plot()

        plt.title("Fraction of prompt market share In Commodities")
        plt.figure()
        c = b.groupby("dayofweek")["percentage_InCo"]
        c.plot(style='-*', legend=True)
        plt.title("Fraction of prompt market share In Commodities")
        
        
        plt.figure()
        d = b.groupby([pd.Grouper(freq='W-SUN'),
                        "weekday"])["percentage_InCo"].mean().unstack()
        d.index = d.index.date
        d.plot(kind='bar', legend=True)
        # plt.legend(["Weekday", "Weekend"])
        plt.title("Fraction of prompt market share In Commodities")


# # traders = pd.read_clipboard()
# uni_traders = set(traders["day trader"].values).union(set(traders["night trader"].values))

# for trader in uni_traders:
#     print("{} average market share in the weekend in last year: {}".format(
#         trader, round(traders.loc[(traders["day trader"] == trader) | (traders["night trader"] == trader) , "market share"].mean(),3)))

# for trader in uni_traders:
#     print("{} average market share in the weekend from winter start: {}".format(
#         trader, round(traders.iloc[:28].loc[(traders["day trader"] == trader) | (traders["night trader"] == trader) , "market share"].mean(),3)))


    
""" 
project 2:
    pull data per trader per day out of GasTransactions
    map onto bar chart  
    
project 3:
    cross-correlate market share with price movement.
"""