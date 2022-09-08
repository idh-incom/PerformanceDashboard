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

mkts = ("TTF", "TTF ICE", "THE", "THE H", "THE L", "ZTP", "ZTP L", "VTP DA", "VTP WD", "PEG", "ETF", "NBP ICE")
inst_names = ["TTF Hi Cal 51.6", "TTF Hi Cal 51.6 ICE", "THE", "THE Hi Cal", "THE Low Cal", "ZTP", "ZTP Low Cal",
              "CEGH VTP", "CEGH VTP Within Day", "PEG", "ETF", "NBP ICE", "THE L East (Hour)", "THE L West (Hour)"]
inst_map = {k: v for k,v in zip(mkts, inst_names)}
inst_ids = (10002806, 10002228, 10002148, 10002785, 10641458, 10002940, 10641476, 10641392,
          10641400,  10642951, 10641346, 10006025, 10000312, 10000310)
inst_id_map = {k: v for k,v in zip(inst_names, inst_ids)}

def calc_vwap(df):
    _volume = df.Quantity.sum()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        _vwap = ((df.Quantity*df.Price).sum()/_volume).round(2)
    return _vwap

def vwap_aggs(x):
    _vwap = calc_vwap(x)
    return pd.Series({"VWAP": _vwap})

def aggregate_frame_per_10_minutes(df, aggs):
    df = df.groupby(pd.Grouper(freq="10min")).apply(aggs)
    return df

def fetch_traybot_volume_vals():
    query= """
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

def grab_volume_data(from_date):
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
      and firstSequenceID in ('10000302', '10000314', '10410041')
      --and FirstSequenceItemId = 2
      and instId in {inst_ids}
      ) a) s
      group by isOwnData, [Date], FirstSequenceItemName
    """
    df = psql.read_sql_query(sq, bi_sql_engine)
    return df

def plot_market_share_data():
    renew_volume_data = st.button("Fetch volume data")
    now = pd.to_datetime(dt.datetime.utcnow()).tz_localize("UTC").tz_convert("Europe/Berlin")
    if 'last_share_refresh' not in st.session_state:
        st.session_state.last_share_refresh = pd.to_datetime('2021-01-01').tz_localize("UTC").tz_convert("Europe/Berlin")
    conds = (renew_volume_data, (np.array([x in st.session_state for x in ['share_data', 'volume_data', 'last_share_refresh']]) == False).any(), (now - st.session_state.last_share_refresh).total_seconds() > 300)
    if any(conds):
        if 'volume_data' not in st.session_state:
            st.session_state.volume_data = grab_volume_data(st.session_state.last_share_refresh)
        else:
            st.session_state.volume_data = pd.concat([st.session_state.volume_data,grab_volume_data(st.session_state.last_share_refresh)])
        st.session_state.last_share_refresh = pd.to_datetime(dt.datetime.utcnow()).tz_localize("UTC").tz_convert("Europe/Berlin")
    venues = np.array([x for x in st.session_state.volume_data.Venue.unique()])
    st.sidebar.write("Which products are included in the survey?")
    products_to_include = venues[np.array([st.sidebar.checkbox(x,True) for x in venues])]
    st.write(f"products included: {inst_id_map}")
    a = st.session_state.volume_data.loc[st.session_state.volume_data.Venue.isin(products_to_include)].groupby(['Date', 'isOwnData']).sum().round().reset_index()

    b = a.pivot(index='Date', columns = ['isOwnData'], values='Volume').reset_index()
    b.columns = ["Date","Market_volume", "Our_volume"]
    b["percentage_InCo"] = b["Our_volume"]/b[["Market_volume", "Our_volume"]].sum(axis=1)
    b.Date = b.Date.apply(pd.to_datetime)
    
    b["dayofweek"] = b.Date.dt.dayofweek
    b["weekday"] = b["dayofweek"] < 5
    b["weekday"] = b["weekday"].apply(lambda x: indmap[x])
    b["dayofweek"] = b["dayofweek"].apply(lambda x: daymap[x])

    b = b.set_index("Date")
    st.session_state.share_data = b.sort_index()
    st.session_state.share_data.index = st.session_state.share_data.index.tz_localize("UTC").tz_convert("Europe/Berlin").date

    # run_trade_fetch()
    date_def = pd.to_datetime(f"{now.year}-{now.month}-{now.day}")
    fdate = st.sidebar.date_input("From date (date is included)", value = date_def - dt.timedelta(days = 60))
    tdate = st.sidebar.date_input("To date (date itself not included)", value = date_def + dt.timedelta(days=1))
    
    b = b.loc[fdate:tdate]
    
    # c = b.groupby("dayofweek")["percentage_InCo"]
    d = b.groupby([pd.Grouper(freq='W-SUN'),
                    "weekday"])["percentage_InCo"].mean().unstack()
    b.index = b.index.tz_localize("UTC").tz_convert("Europe/Berlin").date
    
    d.index = d.index.tz_localize("UTC").tz_convert("Europe/Berlin").date
    
    fig0, ax0 = plt.subplots() #TODO add rot=45
    # fig1, ax1 = plt.subplots()
    fig2, ax2 = plt.subplots()
    
    b.percentage_InCo.plot(ax=ax0)
    for tick in ax0.get_xticklabels():
        tick.set_rotation(90)
    # c.plot(style='-*', legend=True, ax=ax1)
        
    d.plot(kind='bar', legend=True, ax=ax2)
    l,r = st.beta_columns((2,3))
    with l:
        st.write("**Fraction of prompt market share In Commodities**")
        st.pyplot(fig0)
        # st.pyplot(fig1)
        st.pyplot(fig2)
    with r:
        st.write("last refresh", st.session_state.last_share_refresh.strftime("%Y-%m-%d %H:%M:%S"))
        st.write(b)

def grab_trades_data(from_date):
    sq = f"""
    with cte_table AS
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
                  ,case when InstId in ('10000310', '10000312') then 'Hourlies' else [FirstSequenceItemName] end as [FirstSequenceItemName]
     			  ,cast(DateTime at TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time' as datetime) as DateTimeCet
                  , ROW_NUMBER() OVER (PARTITION BY TradeID
                                    , InstID
                                    , FirstSequenceID
                                    , FirstSequenceItemID
                        ORDER BY LastUpdate DESC
                        ) AS rn
    		  FROM [pub].[Trayport_Trades_Public]
      where DateTime > '{from_date}'
      --and SeqSpan = 'Single'
      and ((InstId in ('10000310', '10000312')) or (FirstSequenceItemID in (1, 2,4,6,7,30)))
      and firstSequenceID in ('10000302', '10000314', '10410041')
      --and FirstSequenceItemId = 2
      and instId in {inst_ids}
      ) SELECT a.Price,
           a.DateTime,
           a.Volume,
           a.IsOwnData,
           a.InstName,
           a.FirstSequenceItemName as [Product]
     	  , (25 - datepart(hour, a.DateTimeCet)) % 24 + 1 as RoDHours
     	  , case when InstName like '%PEG%' then Volume
           when InstName like '%NBP%' then 29.3071*Volume
           when FirstSequenceItemID in (2,6,7) then Volume*24
     	  when FirstSequenceItemID = 1 then Volume*((25 - datepart(hour, a.DateTimeCet)) % 24 + 1)
     	  when FirstSequenceItemID = 4 then Volume*48
     	  else Volume
     	  end as [Quantity]
           FROM cte_table a
			where rn = 1
            and a.Action not in ('Remove')
    """
    df = psql.read_sql_query(sq, bi_sql_engine)
    if df.shape[0] == 0:
        return
    df.DateTime = df.DateTime.apply(pd.to_datetime)
    df = df.set_index("DateTime")
    df.index = df.index.tz_localize("UTC").tz_convert("Europe/Berlin")
    return df

def plot_trade_data():
    renew_trades_data = st.button("Fetch trades data")
    now = pd.to_datetime(dt.datetime.utcnow()).tz_localize("UTC").tz_convert("Europe/Berlin")
    if 'last_trades_refresh' not in st.session_state:
        st.session_state.last_trades_refresh = pd.to_datetime('2022-09-01').tz_localize("UTC").tz_convert("Europe/Berlin")
    st.write("last refresh", st.session_state.last_trades_refresh.strftime("%Y-%m-%d %H:%M:%S"))
    conds = (renew_trades_data, (np.array([x in st.session_state for x in ['trades_data', 'own_trades', 'last_trades_refresh']]) == False).any(), (now - st.session_state.last_trades_refresh).total_seconds() > 300)
    if any(conds):
        if 'trades_data' not in st.session_state:
            st.session_state.trades_data = grab_trades_data(st.session_state.last_trades_refresh)
        else:
            st.session_state.trades_data = pd.concat([st.session_state.trades_data,grab_trades_data(st.session_state.last_trades_refresh)])
        st.session_state.own_trades = st.session_state.trades_data.loc[st.session_state.trades_data.IsOwnData == 1]
        st.session_state.last_trades_refresh = pd.to_datetime(dt.datetime.utcnow()).tz_localize("UTC").tz_convert("Europe/Berlin")
    instnameFilter = st.sidebar.multiselect("What Instruments?", st.session_state.trades_data.InstName.unique(), default=["THE EEX"])
    productFilter = st.sidebar.multiselect("What Products?", st.session_state.trades_data.Product.unique(), default = ["DA"])
    date_def = pd.to_datetime(f"{now.year}-{now.month}-{now.day}")
    fdate = pd.to_datetime(st.sidebar.date_input("Date", value = date_def - dt.timedelta(days = 1))).tz_localize("UTC").tz_convert("Europe/Berlin")
    start,finish = st.sidebar.slider("select start/stop hour", 0,24,(8,18),step=1)
    tdate = fdate + dt.timedelta(hours=finish)
    fdate = fdate + dt.timedelta(hours=start)
    frame = st.session_state.trades_data.loc[(fdate<=st.session_state.trades_data.index) & (st.session_state.trades_data.index < tdate)].copy()
    frame = frame.loc[(frame.InstName.isin(instnameFilter)) & (frame.Product.isin(productFilter))]
    st.write(frame.tail(1000))
    # [st.write(f.items()) for f in frame.groupby(["InstName", "Product"])]
    groups = frame.groupby(["InstName", "Product"])
    names = []
    frames = []
    for k,v in groups:
        names.append(' '.join(k))
        frames.append(v)
    vwap_frame = pd.concat([aggregate_frame_per_10_minutes(f, vwap_aggs) for f in frames], axis=1)#.fillna(method='pad')
    vwap_frame.columns = names
    # st.write(st.session_state.trades_data.tail(1000))
    st.write(vwap_frame)
    fig,ax = plt.subplots()
    vwap_frame.plot(ax=ax,drawstyle="steps-pre")
    for f in frames:
        g = f.loc[f.IsOwnData == 1]
        ax.scatter(g.index, g.Price, s=g.Quantity/240, marker='x')
    st.pyplot(fig)