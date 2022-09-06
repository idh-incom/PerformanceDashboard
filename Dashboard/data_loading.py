# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 18:14:05 2021

@author: idh
"""

"""
Module: data_loading
"""
import pyodbc as odbc
import pandas.io.sql as psql
from psycopg2 import connect
import pandas as pd
import datetime as dt
import streamlit as st
import time

from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Connection, Engine
from typing import cast

APP_AUTHOR = 'IDH'
APP_NAME = 'GAS:FlexTool'

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
            "App": f"Author: {APP_AUTHOR}, Team: Gas, Platform: Py, Solution: {APP_NAME}."
        },
    ),
    pool_size=3,
    max_overflow=5,
))

def _validate_input_types(inst_identifier, seq_identifier, item_identifier):
    if isinstance(inst_identifier, int) and isinstance(seq_identifier, int) and \
            (isinstance(item_identifier, int) or item_identifier is None):
        inst_id = inst_identifier
        seq_id = seq_identifier
    elif isinstance(inst_identifier, str) and isinstance(seq_identifier, str) and \
            (isinstance(item_identifier, str) or item_identifier is None):
        inst_id = find_instrument_id_and_name(inst_identifier)['InstID']
        seq_id = find_sequence_id_and_name(seq_identifier, inst_id)['SeqID']
    else:
        raise ValueError(f'You must use either all integers or all strings as identifiers! ' \
                            f'inst_identifier: {inst_identifier}, ' \
                            f'seq_identifier: {seq_identifier}, ' \
                            f'item_identifier: {item_identifier}')

    if item_identifier is not None:
        if isinstance(item_identifier, str):
            item_id = find_item_id_and_name(item_identifier, seq_id)['ItemID']
        else:
            item_id = item_identifier
    else:
        item_id = None

    return inst_id, seq_id, item_id

def load_instruments():
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        SELECT InstID
            ,InstName
            ,Type
            ,InstCode
            ,CurveID
        FROM [pub].[Trayport_Instruments]
       --<Queryinfo>QueryName: Trayport.Instruments CriticalQuery:TRUE</Queryinfo>"""
    df = psql.read_sql_query(query, bi_sql_engine)
    return df

def load_firstsequences(inst_name=None):
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        SELECT [InstID]
            ,[SeqID]
            ,[SeqName]
            ,[InstName]
            ,[SeqDisplayName]
            ,[CurveID]
        FROM [pub].[Trayport_Sequences]
        {f"WHERE InstName = '{inst_name}'" if inst_name is not None else ""}
       --<Queryinfo>QueryName: Trayport.FirstSequences CriticalQuery:TRUE</Queryinfo>"""
    df = psql.read_sql_query(query, bi_sql_engine)
    return df

def load_firstsequenceitems(inst_name, seq_name):
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        SELECT tsi.SeqID
            ,ts.SeqName
            ,tsi.ItemID
            ,tsi.OrderID
            ,tsi.ItemName
            ,tsi.TradingStart
            ,tsi.TradingEnd
            ,tsi.PeriodStart
            ,tsi.PeriodEnd
            ,tsi.PeriodWeight
            ,tsi.InsertedDateTimeUTC
            ,tsi.ModifiedDateTimeUTC
        FROM [pub].[Trayport_SequenceItems] tsi
        LEFT JOIN
            (
                SELECT [InstID]
                ,[SeqID]
                ,[SeqName]
                ,[InstName]
                ,[SeqDisplayName]
                ,[RowInsertedOrModifiedDateUTC]
                ,[CurveID]
                FROM [pub].[Trayport_Sequences]
                WHERE InstName = '{inst_name}'
                    AND SeqName = '{seq_name}'
            ) AS ts ON tsi.SeqID = ts.SeqID
        WHERE SeqName IS NOT NULL
       --<Queryinfo>QueryName: Trayport.FindFirstSequenceItem CriticalQuery:TRUE</Queryinfo>"""
    df = psql.read_sql_query(query, bi_sql_engine)
    return df


def _map_from_map_to(identifier, name_identifier, id_identifier):
    if isinstance(identifier, int):
        map_to = name_identifier
        map_from = id_identifier
    else:
        map_to = id_identifier
        map_from = name_identifier
    return map_to, map_from


def find_instrument_id_and_name(inst_identifier):
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    map_to, map_from = _map_from_map_to(inst_identifier, 'InstName', 'InstID')
    query = f"""
        SELECT {map_to}
        FROM [pub].[Trayport_Instruments]
        WHERE {f"{map_from} = '{inst_identifier}'"}
       --<Queryinfo>QueryName: Trayport.FindInstrument CriticalQuery:TRUE</Queryinfo>"""
    df = psql.read_sql_query(query, bi_sql_engine)
    try:
        return {map_to: df.iloc[0, 0], map_from: inst_identifier}
    except IndexError:
        return None


def find_sequence_id_and_name(seq_identifier, inst_id):
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    map_to, map_from = _map_from_map_to(seq_identifier, 'SeqName', 'SeqID')
    query = f"""
        SELECT {map_to}
        FROM [pub].[Trayport_Sequences]
        WHERE InstID = {inst_id}
            AND {f"{map_from} = '{seq_identifier}'"}
       --<Queryinfo>QueryName: Trayport.FindSequence CriticalQuery:TRUE</Queryinfo>"""
    df = psql.read_sql_query(query, bi_sql_engine)
    try:
        return {map_to: df.iloc[0, 0], map_from: seq_identifier}
    except IndexError:
        return None


def find_item_id_and_name(item_identifier, seq_id):
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    map_to, map_from = _map_from_map_to(item_identifier, 'ItemName', 'ItemID')
    query = f"""
        SELECT {map_to}
        FROM [pub].[Trayport_SequenceItems]
        WHERE SeqID = {seq_id}
            AND {f"{map_from} = '{item_identifier}'"}
       --<Queryinfo>QueryName: Trayport.FindItem CriticalQuery:TRUE</Queryinfo>"""
    df = psql.read_sql_query(query, bi_sql_engine)
    try:
        return {map_to: df.iloc[0, 0], map_from: item_identifier}
    except IndexError:
        return None


# Notice that we as default only use RouteID = 652 (House).
# Apparently financial trades are also included for RouteID = 904, so for now I assume that all orders are duplicated
def load_orders(inst_identifier, seq_identifier, item_identifier=None, fdate=None, tdate=None,
                select=['ItemID', 'PersistentOrderID', 'Action', 'DateTime', 'Price', 'Volume', 'Side', 'OrderDealt'],
                print_query=False):
    inst_id, seq_id, item_id = _validate_input_types(inst_identifier, seq_identifier, item_identifier)
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        SELECT {', '.join(select)}
        FROM [teamanalysis].[Trayport_Orders_Sequence]
        WHERE Action IN ('Insert', 'Update', 'Remove')
            AND InstID = {inst_id}
            AND SeqID = {seq_id}
            {f"AND ItemID = {item_id}" if item_identifier is not None else ""}
            AND RouteID = 652
            {f"AND DateTime >= '{fdate}'" if fdate is not None else ""}
            {f"AND DateTime <= '{tdate}'" if tdate is not None else ""}
        ORDER BY DateTime, PersistentOrderID
   --<Queryinfo>QueryName: Trayport.Orders CriticalQuery:TRUE</Queryinfo>"""
    if print_query:
        print(query)
    orders = psql.read_sql_query(query, bi_sql_engine)
    if len(orders) > 0:
        orders.DateTime = orders.DateTime.dt.tz_localize('UTC')
    return orders

# Notice that we as default only use RouteID = 652 (House).
# Apparently financial trades are also included for RouteID = 904,
# so for now I assume that all orders are duplicated
def load_orders_sequences(inst_identifier, seq_identifier, item_identifier=None, fdate=None, tdate=None,
                            select=["f_OrderID", "PersistentOrderID", "OrderID", "OldOrderID", "Action", "DateTime", "Price", "Volume", "Side", "OrderDealt"],
                            print_query=False):
    inst_id, seq_id, item_id = _validate_input_types(inst_identifier, seq_identifier, item_identifier)
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        DECLARE @RouteID int = 652
        ;
        WITH cte_recursive AS (
            SELECT OrderID         AS InitialOrderID,
                1                AS nLevel,
                OrderID          AS f_OrderID,
                RouteID,
                {', '.join([col for col in select if col not in ('f_OrderID', 'RouteID')])}
            FROM teamanalysis.Trayport_Orders_Sequence o
            WHERE Action IN ( 'Insert' )
                AND o.InstID = {inst_id}
                AND o.SeqID = {seq_id}
                {f"AND o.ItemID = {item_id}" if item_identifier is not None else ""}
                AND o.RouteID = @RouteID
            UNION ALL
            --  Recursive query
            SELECT o.OldOrderID AS InitialOrderID
                , r.nLevel + 1 AS nLevel
                , r.f_OrderID
                , o.RouteID
                , {', '.join(['o.'+col for col in select if col not in ('f_OrderID', 'RouteID')])}
            FROM teamanalysis.Trayport_Orders_Sequence     o
                INNER JOIN cte_recursive r ON o.OldOrderID = r.OrderID
            WHERE o.Action IN ( 'Update' )
            AND o.InstID = {inst_id}
                AND o.SeqID = {seq_id}
                {f"AND o.ItemID = {item_id}" if item_identifier is not None else ""}
            AND o.RouteID = @RouteID
        )
        -- Taking the Insert and Update recursive data, and adding the Remove data
        , cte_union AS (
        SELECT r.RouteID,
            r.nLevel,
            r.f_OrderID,
            {', '.join(['r.'+col for col in select if col not in ('f_OrderID', 'RouteID', 'nLevel')])}
        FROM cte_recursive r
        UNION ALL
        SELECT tro.RouteID
            , cu.nLevel + 1 AS nLevel
            , cu.f_OrderID
            , {', '.join(['tro.'+col for col in select if col not in ('f_OrderID', 'RouteID')])}
        FROM teamanalysis.Trayport_Orders_Sequence    tro
            LEFT JOIN cte_recursive cu ON tro.OrderID = cu.OrderID
        WHERE tro.Action = 'Remove'
            AND tro.InstID = {inst_id}
            AND tro.SeqID = {seq_id}
            {f"AND tro.ItemID = {item_id}" if item_identifier is not None else ""}
        AND tro.RouteID = @RouteID
        )
        -- Creating a Ranking table
        , cte_rank AS (
        SELECT 1        AS [Rank]
            , 'Insert' AS [Name]
        UNION ALL
        SELECT 2        AS [Rank]
            , 'Update' AS [Name]
        UNION ALL
        SELECT 3        AS [Rank]
            , 'Remove' AS [Name]
        )
        -- Joining Ranking table and Union table, so we can order by Ranking in the Union all table
        SELECT {', '.join(['c.'+col for col in select])}
        FROM cte_union         c
            LEFT JOIN cte_rank r ON c.Action = r.Name
        WHERE 1=1
        {f"AND c.Datetime >= '{fdate}'" if fdate is not None else ""}
        {f"AND c.Datetime <= '{tdate}'" if tdate is not None else ""}
        ORDER BY c.Datetime, c.f_OrderID, r.Rank
        OPTION (MAXRECURSION 0);
   --<Queryinfo>QueryName: Trayport.OrderSequences CriticalQuery:TRUE</Queryinfo>"""
    if print_query:
        print(query)
    orders = psql.read_sql_query(query, bi_sql_engine)
    if len(orders) > 0:
        orders.DateTime = orders.DateTime.dt.tz_localize('UTC')
    return orders

def load_orders_snapshot(inst_identifier, seq_identifier, item_identifier, adate):
    inst_id, seq_id, item_id = _validate_input_types(inst_identifier, seq_identifier, item_identifier)
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        DECLARE @InstID int = '{inst_id}'
        DECLARE @FirstSequenceID int = '{seq_id}'
        DECLARE @FirstSequenceItemID int = '{item_id}'
        DECLARE @RouteID int = 652
        DECLARE @ProbeTime DATETIME2(3) = '{adate}'
        ;
        WITH cte_recursive AS (
            SELECT OrderID          AS InitialOrderID
                , OrderID
                , RouteID
                , OldOrderID
                , Action
                , 1                AS nLevel
                , OrderID          AS f_OrderID
                , PersistentOrderID
                , DateTime
                , Price
                , Volume
                , Side
                , OrderDealt
        FROM pub.Trayport_Orders o
        WHERE Action IN ( 'Insert' )
        AND o.InstID = @InstID
        AND o.FirstSequenceID = @FirstSequenceID
        AND o.FirstSequenceItemID = @FirstSequenceItemID
        AND o.RouteID = @RouteID
        --AND o.PersistentOrderID = @PersistentOrderID
        AND o.DateTime <= @ProbeTime
        UNION ALL
        --  Recursive query
        SELECT o.OldOrderID AS InitialOrderID
                , o.OrderID
                , o.RouteID
                , o.OldOrderID
                , o.Action
                , r.nLevel + 1 AS nLevel
                , r.f_OrderID
                , o.PersistentOrderID
                , o.DateTime
                , o.Price
                , o.Volume
                , o.Side
                , o.OrderDealt
        FROM pub.Trayport_Orders     o
            INNER JOIN cte_recursive r ON o.OldOrderID = r.OrderID
        WHERE o.Action IN ( 'Update' )
        AND o.InstID = @InstID
        AND o.FirstSequenceID = @FirstSequenceID
        AND o.FirstSequenceItemID = @FirstSequenceItemID
        AND o.RouteID = @RouteID
        --AND o.PersistentOrderID = @PersistentOrderID
        AND o.DateTime <= @ProbeTime
        )
           -- Taking the Insert and Update recursive data, and adding the Remove data
           , cte_union AS (
        SELECT r.OrderID
            , r.RouteID
            , r.OldOrderID
            , r.Action
            , r.nLevel
            , r.f_OrderID
            , r.PersistentOrderID
            , r.DateTime
            , r.Price
            , r.Volume
            , r.Side
            , r.OrderDealt
        FROM cte_recursive r
        UNION ALL
        SELECT tro.OrderID
            , tro.RouteID
            , tro.OldOrderID
            , tro.Action
            , cu.nLevel + 1 AS nLevel
            , cu.f_OrderID
            , tro.PersistentOrderID
            , tro.DateTime
            , tro.Price
            , tro.Volume
            , tro.Side
            , tro.OrderDealt
        FROM pub.Trayport_Orders    tro
            LEFT JOIN cte_recursive cu ON tro.OrderID = cu.OrderID
        WHERE tro.Action = 'Remove'
        AND tro.InstID = @InstID
        AND tro.FirstSequenceID = @FirstSequenceID
        AND tro.FirstSequenceItemID = @FirstSequenceItemID
        AND tro.RouteID = @RouteID
        --AND tro.PersistentOrderID = @PersistentOrderID
        AND tro.DateTime <= @ProbeTime
        )
           -- Creating a Ranking table
           , cte_rank AS (
        SELECT 1        AS [Rank]
             , 'Insert' AS [Name]
        UNION ALL
        SELECT 2        AS [Rank]
             , 'Update' AS [Name]
        UNION ALL
        SELECT 3        AS [Rank]
             , 'Remove' AS [Name]
        )
        -- Joining Ranking table and Union table, so we can order by Ranking in the Union all table
        ,cte_removeAndGetLatest AS (
        SELECT c.OrderID
             , c.OldOrderID
             , c.Action
             , c.nLevel
             , c.f_OrderID
             , c.DateTime
             , c.PersistentOrderID
             , r.Rank
             , r.Name
            , c.Price
            , c.Volume
            , c.Side
            , c.OrderDealt
             ,ROW_NUMBER() OVER(PARTITION BY  c.f_OrderID ORDER BY c.DateTime DESC, r.[Rank] desc) AS RowN
        FROM cte_union         c
            LEFT JOIN cte_rank r ON c.Action = r.Name
            WHERE 1=1 AND NOT EXISTS (SELECT cte_union.f_OrderID FROM cte_union WHERE cte_union.Action = 'Remove' AND cte_union.f_OrderID = c.f_OrderID)
        --ORDER BY
        --    c.f_OrderID
        --  , r.Rank
        --  , c.DateTime
          )
          SELECT * FROM cte_removeAndGetLatest c
          WHERE 1=1
              AND c.RowN = 1
            AND c.Action != 'Remove'
          OPTION (MAXRECURSION 0);
   --<Queryinfo>QueryName: Trayport.OrderSnapshot CriticalQuery:TRUE</Queryinfo>"""
    orders = psql.read_sql_query(query, bi_sql_engine)
    if len(orders) > 0:
        orders.DateTime = orders.DateTime.dt.tz_localize('UTC')
    return orders


def load_instances():
    query = f"""FROM pub.Trayport_Trades_Public          tp
    LEFT JOIN pub.Trayport_Instruments   ti ON tp.InstID = ti.InstId
    LEFT JOIN pub.Trayport_Sequences     ts ON tp.InstID = ts.InstId AND tp.FirstSequenceID = ts.SeqId
    LEFT JOIN pub.Trayport_SequenceItems tsi ON tp.FirstSequenceID = tsi.SeqID AND tp.FirstSequenceItemID = tsi.ItemID
	LEFT JOIN pub.MapTrayportInstruments mp ON tp.InstID = mp.TrayportId
    --<Queryinfo>QueryName: Trayport.Instances CriticalQuery:TRUE</Queryinfo>"""
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    all_trades = psql.read_sql_query(query, bi_sql_engine)

    #if len(all_trades) > 0:
     #   all_trades.DateTime = pd.to_datetime(all_trades.DateTime).dt.tz_localize('UTC')
    return all_trades

def load_trades(inst_identifier, seq_identifier, item_identifier=None, fdate=None, tdate=None, print_query=False):
    global d
    inst_id, seq_id, item_id = _validate_input_types(inst_identifier, seq_identifier, item_identifier)
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        WITH cte_table AS (
            SELECT tp.DateTime
                , tp.Price
                , tp.Volume
                , tp.AggressorAction
                , tp.IsOwnData
                , tp.InstID
                , tp.InstName
                , tp.FirstSequenceID
                , tp.FirstSequenceItemID
                , tp.FirstSequenceItemName
                , tp.Action
                , tp.RouteID
                , tp.TradeID
                , si.TradingStart
                , si.TradingEnd
                , si.PeriodStart
                , si.PeriodEnd
                , si.PeriodWeight
                , tp.SeqSpan
                , ROW_NUMBER() OVER (PARTITION BY tp.TradeID
                                                --, tp.RouteID
                                                , tp.InstID
                                                , tp.FirstSequenceID
                                                , tp.FirstSequenceItemID
                                    ORDER BY tp.LastUpdate DESC
                                    ) AS rn
            FROM pub.[Trayport_Trades_Public]        AS tp
                LEFT JOIN pub.Trayport_SequenceItems si ON tp.FirstSequenceID = si.SeqID AND tp.FirstSequenceItemID = si.ItemID
            WHERE 1=1
                AND tp.Action IN( 'Query','Remove')
                AND tp.RouteID IN ( 652 )
                AND tp.InstID = {inst_id}
                {f"AND tp.FirstSequenceItemID = {item_id}" if item_id  is not None else ""}
                {f"AND tp.DateTime >= '{fdate}'" if fdate is not None else ""}
                {f"AND tp.DateTime <= '{tdate}'" if tdate is not None else ""}
            ), cte_remove AS (
            SELECT c.DateTime
                , c.Price
                , c.Volume
                , c.AggressorAction
                , c.IsOwnData
                , c.InstID
                , c.InstName
                , c.FirstSequenceID
                , c.FirstSequenceItemID
                , c.FirstSequenceItemName
                , c.Action
                , c.RouteID
                , c.TradeID
                , c.TradingStart
                , c.TradingEnd
                , c.PeriodStart
                , c.PeriodEnd
                , c.PeriodWeight
                , c.rn
                , c.SeqSpan
            FROM cte_table c
            WHERE  c.TradeID NOT IN
                        (
                            SELECT f.TradeID FROM cte_table AS f WHERE f.Action = 'Remove'
                        ))
                        SELECT c.DateTime
                            , c.Price
                            , c.Volume
                            , c.AggressorAction
                            , c.IsOwnData
                            , c.InstID
                            , c.InstName
                            , c.FirstSequenceID
                            , c.FirstSequenceItemID
                            , c.FirstSequenceItemName
                            , c.Action
                            , c.RouteID
                            , c.TradeID
                            , c.TradingStart
                            , c.TradingEnd
                            , c.PeriodStart
                            , c.PeriodEnd
                            , c.PeriodWeight
                            --, c.rn
                            , c.SeqSpan FROM cte_remove c
                        WHERE c.rn = 1
   --<Queryinfo>QueryName: Trayport.AlteredTrades CriticalQuery:TRUE</Queryinfo>"""
    if print_query:
        print(query)
    all_trades = psql.read_sql_query(query, bi_sql_engine)
    if len(all_trades) > 0:
        all_trades.DateTime = pd.to_datetime(all_trades.DateTime).dt.tz_localize('UTC')
    return all_trades

def load_spread_trades(a1, a2, fdate, tdate, period="DA"):
    global d
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    # st.write(area, instrument)
    query = f"""
        WITH cte_table AS (
            SELECT tp.DateTime
                , tp.Price
                , tp.Volume
                , tp.AggressorAction
                , tp.IsOwnData
                , tp.InstID
                , tp.InstName
                , tp.FirstSequenceItemName
                , tp.Action
                , tp.SeqSpan
                , ROW_NUMBER() OVER (PARTITION BY tp.TradeID
                                                , tp.InstID
                                                , tp.FirstSequenceID
                                                , tp.FirstSequenceItemID
                                    ORDER BY tp.LastUpdate DESC
                                    ) AS rn
            FROM pub.[Trayport_Trades_Public]        AS tp
                LEFT JOIN pub.Trayport_SequenceItems si ON tp.FirstSequenceID = si.SeqID AND tp.FirstSequenceItemID = si.ItemID
            WHERE 1=1
            AND tp.FirstSequenceItemName = '{period}'
            AND tp.InstName like '%{a1}%/%{a2}%'
                AND tp.Action IN( 'Query','Remove', 'Insert')
                AND tp.RouteID IN ( 652 )
                AND tp.SeqSpan in ( 'Single' )
                AND tp.DateTime >= '{fdate}'
                AND tp.DateTime < '{tdate}'
            )
            SELECT c.DateTime
                , c.Price
                , c.Volume
                , c.AggressorAction
                , c.IsOwnData
                , c.InstID
                , c.InstName
                , c.FirstSequenceItemName
                , c.Action
                , c.SeqSpan
            FROM cte_table c
			where rn = 1
           
   --<Queryinfo>QueryName: Tryport.Trades CriticalQuery:TRUE</Queryinfo>"""
    
    tt = psql.read_sql_query(query, bi_sql_engine)
    tt = tt.loc[~tt.InstName.str.contains("Baseload")]
    # tt = tt.loc[tt.PeriodWeight != 1]
    tt =tt.drop([col for col in ["Action", "rn", "SeqSpan", "RouteID", "TradingStart", "TradingEnd", "PeriodStart", "PeriodEnd", "FirstSequenceID", "FirstSequenceItemID", "InstID",
                 "AggressorAction"] if col in tt.columns], axis=1)
    tt.DateTime = pd.to_datetime(tt.DateTime).dt.tz_localize("CET")
    tt.DateTime = tt.DateTime + dt.timedelta(hours = 1) + tt.DateTime.apply(lambda x: x.dst())
    tt = tt.set_index("DateTime")

    return tt
    
def load_trades_longterm(area, fdate=None, tdate=None, period="DA", print_query=False):
    global d
    if area == 'ETF':
        area = 'Denmark ETF'
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    # st.write(area, instrument)
    query = f"""
        WITH cte_table AS (
            SELECT tp.DateTime
                , tp.Price
                , tp.Volume
                , tp.AggressorAction
                , tp.IsOwnData
                , tp.InstID
                , tp.InstName
                , tp.FirstSequenceItemName
                , tp.Action
                , tp.SeqSpan
                , ROW_NUMBER() OVER (PARTITION BY tp.TradeID
                                                , tp.InstID
                                                , tp.FirstSequenceID
                                                , tp.FirstSequenceItemID
                                    ORDER BY tp.LastUpdate DESC
                                    ) AS rn
            FROM pub.[Trayport_Trades_Public]        AS tp
                LEFT JOIN pub.Trayport_SequenceItems si ON tp.FirstSequenceID = si.SeqID AND tp.FirstSequenceItemID = si.ItemID
            WHERE 1=1
            AND tp.FirstSequenceItemName = '{period}'
            AND tp.InstName in('{area}', '{area} EEX', '{area} ICE')
                AND tp.Action IN( 'Query','Remove', 'Insert')
                AND tp.RouteID IN ( 652 )
                AND tp.SeqSpan in ( 'Single' )
                AND tp.DateTime >= '{fdate}'
                AND tp.DateTime < '{tdate}'
            )
            SELECT c.DateTime
                , c.Price
                , c.Volume
                , c.AggressorAction
                , c.IsOwnData
                , c.InstID
                , c.InstName
                , c.FirstSequenceItemName
                , c.Action
                , c.SeqSpan
            FROM cte_table c
			where rn = 1
            and c.Action not in ('Remove')
           
   --<Queryinfo>QueryName: Tryport.Trades CriticalQuery:TRUE</Queryinfo>"""
    if print_query:
        print(query)
    tt = psql.read_sql_query(query, bi_sql_engine)
    tt = tt.loc[~tt.InstName.str.contains("Baseload")]
    # tt = tt.loc[tt.PeriodWeight != 1]
    tt =tt.drop([col for col in ["Action", "rn", "SeqSpan", "RouteID", "TradingStart", "TradingEnd", "PeriodStart", "PeriodEnd", "FirstSequenceID", "FirstSequenceItemID", "InstID",
                 "AggressorAction"] if col in tt.columns], axis=1)
    tt.DateTime = pd.to_datetime(tt.DateTime).dt.tz_localize("CET")
    tt.DateTime = tt.DateTime + dt.timedelta(hours = 1) + tt.DateTime.apply(lambda x: x.dst())
    tt = tt.set_index("DateTime")
    return tt

def load_cap_trades(fdate=None, tdate=None):
    if fdate is None:
        now = dt.datetime.now()
        if now.hour > 2:
            fdate = dt.datetime(now.year, now.month, now.day, 6)
        else:
            fdate = dt.datetime(now.year, now.month, now.day, 6) - dt.timedelta(days=1)
        tdate = fdate + dt.timedelta(days=1)
    #connection = _data_connection_bisql(APP_AUTHOR,APP_NAME)
    query= f"""
            SELECT PeriodType
                  , TradeTimestampCet
                  , DeliveryBeginCet
                  , Quantity
                  , TotalPrice
                  , CapacityCategory
                  , NetworkPointName
                  , Direction
                  , Tso
              FROM [pub].[GasCapacities]
              WHERE Quantity > 0.001
              AND DeliveryBeginCET >= '{fdate}'
              AND DeliveryBeginCET < '{tdate}'
              order by DeliveryBeginCet desc
       --<Queryinfo>QueryName: Capacities.ShortTerm CriticalQuery:TRUE</Queryinfo>"""
    caps = psql.read_sql_query(query,bi_sql_engine)
    caps["TradeTimestampCet"] = pd.to_datetime(caps.TradeTimestampCet)
    caps["DeliveryBeginCet"] = pd.to_datetime(caps.DeliveryBeginCet)
    return caps

def load_cap_trades_lterm(fdate=None, tdate=None):
    if fdate is None:
        now = dt.datetime.now()
        if now.hour > 2:
            fdate = dt.datetime(now.year, now.month, now.day, 6)
        else:
            fdate = dt.datetime(now.year, now.month, now.day, 6) - dt.timedelta(days=1)
        tdate = fdate + dt.timedelta(days=1)
    #connection = _data_connection_bisql(APP_AUTHOR,APP_NAME)
    query= f"""
            SELECT PeriodType
                  , TradeTimestampCet
                  , DeliveryBeginCet
                  , DeliveryEndCet
                  , Quantity
                  , TotalPrice
                  , CapacityCategory
                  , NetworkPointName
                  , Direction
                  , Tso
              FROM [pub].[GasCapacities]
              WHERE Quantity > 0.001
              AND DeliveryBeginCET < '{tdate}'
              AND DeliveryEndCET >= '{tdate}'
              order by DeliveryBeginCet desc
       --<Queryinfo>QueryName: Capacities.Longterm CriticalQuery:TRUE</Queryinfo>"""
    caps = psql.read_sql_query(query,bi_sql_engine)
    caps["TradeTimestampCet"] = pd.to_datetime(caps.TradeTimestampCet)
    caps["DeliveryBeginCet"] = pd.to_datetime(caps.DeliveryBeginCet)
    return caps


def load_storage_contracts():
    query = """
    SELECT [GasStorageRateId]
          ,[GasStorageContractCode]
          ,[Direction]
          ,[WorkingVolume]
          ,[RateBeginCET]
          ,[RateEndCET]
          ,[CapacityCategory]
          ,[Quantity]
          ,[QuantityMeasure]
          ,[IsRateActive]

      FROM [pub].[GasStorageInjectionAndWithdrawalRate]
      where isRateActive = 1
   --<Queryinfo>QueryName: Storage.Contracts CriticalQuery:TRUE</Queryinfo>"""
    contracts = psql.read_sql_query(query, bi_sql_engine)
    return contracts

def load_StorageVariableCosts():
    now = dt.datetime.now()
    fdate = dt.datetime(now.year, now.month, now.day, 6) + dt.timedelta(days=1)
    query = f"""
        SELECT GasStorageContractCode
            , CostValidFrom
            , CostValidTo
            , Cost
            , Direction
            , RateType
        FROM [pub].[gasStorageVariableCosts]
        Where CostValidFrom <= '{fdate}'
        AND CostValidTo > '{fdate}'
   --<Queryinfo>QueryName: Storage.VariableCosts CriticalQuery:TRUE</Queryinfo>"""
    vcosts = psql.read_sql_query(query, bi_sql_engine)
    vcosts["CostDriver"] = vcosts["Direction"] + "-" + vcosts["RateType"]
    return vcosts


def load_storage_volume_restrictions():
    now = dt.datetime.now()
    fdate = dt.datetime(now.year, now.month, now.day, 6) + dt.timedelta(days=1)
    #connection = _data_connection_bisql(APP_AUTHOR,APP_NAME)
    query = f"""
            SELECT GasStorageContractCode
                , RestrictionValidFrom
                , RestrictionValidTo
                , MinimumVolumeInStore
                , MaximumVolumeInStore
            FROM [pub].[GasStorageVolumeRestriction]
            WHERE RestrictionValidFrom <= '{fdate}'
            AND RestrictionValidTo > '{fdate}'
   --<Queryinfo>QueryName: Storage.VolumeRestrictions CriticalQuery:TRUE</Queryinfo>"""
    restriction = psql.read_sql_query(query, bi_sql_engine)
    return restriction

def load_spot_prices(from_dt, to_dt, country):
    #connection = _data_connection_lilac(APP_AUTHOR, APP_NAME)
    query = f"""
        SELECT ValueDate
            ,Unit
            ,Currency
            ,PriceArea
            ,TradingVenue
            ,VariableSpecification
            ,Price
        FROM [Sandbox].[dbo].[vPowerPrices_DayAhead]
        where Country = '{country}'
            and TradingVenue = 'EPEXSPOT'
            and TimeGranularity = 'HOURS (1)'
            and ValueDate BETWEEN '{from_dt}' AND '{to_dt}'
        ORDER BY ValueDate ASC
       --<Queryinfo>QueryName: SpotPrices CriticalQuery:TRUE</Queryinfo>"""
    df = psql.read_sql_query(query, bi_sql_engine)
    return df

def load_storage_balancing():
    now = dt.datetime.now()
    fdate = dt.datetime(now.year, now.month, now.day, 6) + dt.timedelta(days=1)
    #connection = _data_connection_bisql(APP_AUTHOR,APP_NAME)
    query = f"""
            SELECT AreaOwn
            		, AreaCounterpart
            		, gb.GasBalancingContractCode as BookName
            		, Direction
            		, Product
            		, LotSize
            		, DeliveryBeginUTC as FeeValidFromUTC
            		, DeliveryEndUTC as FeeValidToUTC
            		, GasNetworkPointCode as NetworkPointName
            		, NominationDeadlineMinute as LeadTime

            FROM pub.GasBalancingContractTransactions gb
            LEFT JOIN pub.dimGasBalancingContract gbc ON gb.GasBalancingContractCode = gbc.GasBalancingContractCode AND gb.DeliveryBeginUTC = gbc.ContractBeginUTC AND gb.DeliveryEndUTC = gbc.ContractEndUTC
            WHERE DeliveryBeginUTC <= '{fdate}'
            AND DeliveryEndUTC > '{fdate}'
   --<Queryinfo>QueryName: Storage.Balancing CriticalQuery:TRUE</Queryinfo>"""
    restriction = psql.read_sql_query(query, bi_sql_engine)
    return restriction

def load_gastransactions(Areacpart, fdate=None, tdate=None):
    now = dt.datetime.now()
    if fdate is None:
        fdate = now + dt.timedelta(hours=6)
        fdate = dt.datetime(fdate.year, fdate.month, fdate.day, fdate.hour)
    coff = dt.datetime(now.year, now.month, now.day, now.hour)-dt.timedelta(days=1)
    #connection = _data_connection_bisql(APP_AUTHOR,APP_NAME)
    query = f"""
            SELECT AreaCounterpart
            , Counterpart
            , DeliveryBeginUtc
            , DeliveryEndUtc
            , DeliveryBeginCet
            , DeliveryEndCet
            , Quantity
            , TotalQuantityMWh
            , Price
            , Text
            , TradeTimeStampUtc
            , TradeTimeStampCet
            , Source
            FROM [pub].[GasTransactions]
            WHERE Price > 0
            AND Source = 'Trayport'
            AND Counterpart = 'EEX'
            AND TradeTimeStampCet > '{coff}'
            AND AreaCounterpart LIKE '{Areacpart}%'
            AND DeliveryBeginCet <= '{fdate}'
            {f"AND TradeTimeStampCet <= '{tdate}'" if tdate is not None else ""}
            order by DeliveryBeginCet desc
       --<Queryinfo>QueryName: Gas.Transactions CriticalQuery:TRUE</Queryinfo>"""
    transactions = psql.read_sql_query(query, bi_sql_engine)
    transactions["DeliveryBeginCet"] = pd.to_datetime(transactions["DeliveryBeginUtc"])
    transactions["DeliveryEndCet"] = pd.to_datetime(transactions["DeliveryEndUtc"])
    transactions["TradeTimeStampCet"] = pd.to_datetime(transactions["TradeTimeStampUtc"])
    # st.write(transactions)
    # st.write(time.tzname)
    # transactions["DeliveryBeginCet"] = transactions.DeliveryBeginUtc.dt.tz_localize("UTC")
    return transactions

# def test_tz():
#     #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
#     query = f"""
#     SELECT GETDATE()
#    --<Queryinfo>QueryName: FlexToolQuery CriticalQuery:TRUE</Queryinfo>"""
#     st.write(psql.read_sql_query(query, bi_sql_engine))
#     import struct
#     def handle_datetimeoffset(dto_value):
#         tup = struct.unpack("<6hI2h", dto_value)  # e.g., (2017, 3, 16, 10, 35, 18, 0, -6, 0)
#         tweaked = [tup[i] // 100 if i == 6 else tup[i] for i in range(len(tup))]
#         return "{:04d}-{:02d}-{:02d} {:02d}:{:02d}:{:02d}.{:07d} {:+03d}:{:02d}".format(*tweaked)

#     connection.add_output_converter(-155, handle_datetimeoffset)
#     query = f"""
#     SELECT cast(GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME)
#    --<Queryinfo>QueryName: FlexToolQuery CriticalQuery:TRUE</Queryinfo>"""
#     st.write(psql.read_sql_query(query, bi_sql_engine))

#     query = f"""
#     SELECT CAST(CAST(GETDATE() AS DATETIMEOFFSET) AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME) AS DateTimeDataType,
#             CAST(GETDATE() AS DATETIMEOFFSET) AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DateTimeOffsetDataType

#    --<Queryinfo>QueryName: FlexToolQuery CriticalQuery:TRUE</Queryinfo>"""
#     st.write(psql.read_sql_query(query, bi_sql_engine))

def load_storage_filling_level(storage):
    #connection = _data_connection_bisql(APP_AUTHOR,APP_NAME)
    query = f"""


        SELECT *,
               SUM([Delta_Volume_in_Storage]) OVER (ORDER BY [Date] ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Volume_in_Storage
        FROM
        (
        SELECT CASE
        		WHEN DATEPART(HOUR, igt.DeliveryBeginUtc AT TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time') >= 6 THEN DATEADD(DAY, 1, CAST(igt.DeliveryBeginUtc AT TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time' AS DATE))
        		ELSE CAST(igt.DeliveryBeginUtc AS DATE)
        		END AS [Date]
        		,SUM(DATEDIFF(HOUR, [DeliveryBeginUtc], [DeliveryEndUtc]) * -[Quantity]) AS Delta_Volume_in_Storage
        FROM pub.GasTransactions igt
        WHERE igt.Counterpart = '{storage}'
        	AND igt.TradeType = 1
        	AND igt.IsTradeActive = 1
        	AND igt.BookId>0
        GROUP BY CASE
                 WHEN DATEPART(HOUR, igt.DeliveryBeginUtc AT TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time') >= 6 THEN
                 DATEADD(
                 DAY,
                 1,
                 CAST(igt.DeliveryBeginUtc AT TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time' AS DATE)
                 )
                 ELSE
                 CAST(igt.DeliveryBeginUtc AS DATE)
                 END
        ) AS tmp
        ORDER BY [Date] desc
       --<Queryinfo>QueryName: storage.Filling CriticalQuery:TRUE</Queryinfo>"""
    volumes = psql.read_sql_query(query, bi_sql_engine)
    return volumes["Volume_in_Storage"][1], volumes["Delta_Volume_in_Storage"][1]

def load_latest_prisma_aucts():
    now = dt.datetime.now()
    reftime = now - dt.timedelta(hours=1)
    tmrw = now + dt.timedelta(days=1)
    if not now.hour < 3:
        fdate = dt.datetime(tmrw.year, tmrw.month, tmrw.day, 6 + round(time.timezone/3600))
    else:
        fdate = dt.datetime(now.year, now.month, now.day, 6 + round(time.timezone/3600))
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)
    query = f"""
        SELECT Product, NETWORKPOINT, TSO, MarketArea, DIRECTION, MARKETABLE, MARKETED, (MARKETABLE-MARKETED) as REMAINING, CATEGORY, DATEADD(hour, 1, RuntimeStart) as RuntimeStartCEST,
             CASE WHEN '{now.minute}' < 36 then '{now.hour}' + 2
                  ELSE '{now.hour}' + 3 end as AuctionClose, getUTCdate() as [DATE], RuntimeStart, startingprice
        FROM pub.[Prisma_Auctions]
		     WHERE RuntimeEnd = '{fdate}'
             AND DATEPART(HOUR, RuntimeStart) > datepart(hour,getutcdate()) + CASE WHEN '{now.minute}' < 36 then 1 ELSE 2 end
             --<Queryinfo>QueryName: prisma.LatestAuction CriticalQuery:TRUE</Queryinfo>"""
    aucts = psql.read_sql_query(query, bi_sql_engine)
    if aucts.shape[0] == 0:
        query = f"""
        SELECT Product, NETWORKPOINT, TSO, MarketArea, DIRECTION, MARKETABLE, MARKETED, (MARKETABLE-MARKETED) as REMAINING, CATEGORY, DATEADD(hour, 1, RuntimeStart) as RuntimeStartCEST,
             CASE WHEN '{now.minute}' < 36 then '{now.hour}' + 2
                  ELSE '{now.hour}' + 3 end as AuctionClose, getUTCdate() as [DATE], RuntimeStart, startingprice
        FROM pub.[Prisma_Auctions]
		     WHERE RuntimeEnd = '{fdate}'
             AND DATEPART(HOUR, RuntimeStart) > datepart(hour,getutcdate()) + CASE WHEN '{now.minute}' < 36 then 0 ELSE 1 end
             --<Queryinfo>QueryName: prisma.LatestAuction CriticalQuery:TRUE</Queryinfo>"""
        aucts = psql.read_sql_query(query, bi_sql_engine)
    aucts = aucts.loc[aucts.RuntimeStartCEST == aucts.RuntimeStartCEST.max()]
    aucts = aucts.drop(["MARKETABLE", "MARKETED", "AuctionClose", "DATE", "RuntimeStart"], axis=1)
    aucts.startingprice = (aucts.startingprice * 10 / ((25 - reftime.hour) % 24 + 1)).round(3)
    aucts.REMAINING /= 1000
    trans_aucts = aucts.loc[aucts.NETWORKPOINT.apply(lambda x: check_networkpoint_name(x))]
    stor_aucts = aucts.loc[aucts.NETWORKPOINT.apply(lambda x: check_storage_name(x))]
    return trans_aucts, stor_aucts


def check_networkpoint_name(nam):
    if "VIP-Belgium-NCG" in nam:
        return True
    if "VIP L Gaspool-NCG" in nam:
        return True
    if "VIP-TTF" in nam:
        return True
    if "VIP TTF" in nam:
        return True
    if "Oberkappel" in nam:
        return True
    if "Eynatten" in nam:
        return True
    if "Hilvarenbeek" in nam:
        return True
    if "Virtualys" in nam:
        return True
    if "Lampertheim" in nam:
        return True
    if "Obergailbach"in nam:
        return True
    if "Taisnieres B" in nam:
        return True
    if "Tarvisio" in nam:
        return True
    if "VIP Fr" in nam:
        return True
    return False

def check_storage_name(nam):
    if nam in ("Zone UGS EWE L-Gas",
               "Jemgum III",
               "Speicher Epe L",
               "Speicher Gronau-Epe L1",
               "Leer - Mooräcker - 3 (700096 Jemgum I UGS-E)",
               "Leer - Mooräcker - 4 (700096 Jemgum I UGS-A)",
               "Epe - III (UGS-E)",
               "Epe - IV (UGS-A)",
               "Etzel (Speicher ESE),Bitzenlander Weg 3",
               "Speicher Etzel ESE GASPOOL"):
        return True
    return False

def load_storage_position():
    from Storageposition_monsterquery import squery as mquery
    #connection = _data_connection_bisql(APP_AUTHOR, APP_NAME)

    storage_poses = psql.read_sql_query(mquery, bi_sql_engine)
    storage_poses = storage_poses.query("Type != '1_WGV'").query("Type != '5_IntrinsicValue_D'").query("Type != '5_IntrinsicValue_D-1'").query("Type != '6_ExtrinsicValue_D'").query("Type != '6_ExtrinsicValue_D-1'")
    return storage_poses

def load_UK_curves():
    query = f"""
    SELECT [CurveId]
      ,[ForecastDateTimeUTC]
      ,[ValueDateTimeUTC]
      ,[Value]
      FROM [raw].[Meteologica_TimeSeries] r
      where curveid in ( 1000220002, 1000220015, 1000220018 )
      and ValueDateTimeUTC > '2021-01-07'
      and DATEDIFF(HOUR,ForecastDateTimeUTC, ValueDateTimeUTC) < 48
      order by ValueDateTimeUTC
      --<Queryinfo>QueryName: Uk.LoadCurve CriticalQuery:TRUE</Queryinfo>"""
    weather_data = psql.read_sql_query(query, bi_sql_engine)
    grouped = weather_data.sort_values(["ValueDateTimeUTC", "ForecastDateTimeUTC"]).groupby(["CurveId", "ValueDateTimeUTC"]).agg(lambda x: x.iat[-1])
    agg_time = grouped.reset_index().pivot(index="ValueDateTimeUTC", columns="CurveId", values=["Value", "ForecastDateTimeUTC"])
    return agg_time


def load_tariffs():
    query = f"""
    /*<ToolInfo>Author: IDH;MLI, Team: Gas, Platform: Power BI, Solution: Pricing Assistant</ToolInfo>*/
    DECLARE @BeginDateCET AS DATE = '2020-10-01'--'|BeginDateCET|'/*'2020-10-01'*/
    DECLARE @EndDateCET AS DATE = '2023-10-01'--'|EndDateCET|'/*'2023-10-01'*/
    SET NOCOUNT ON
    DECLARE @BeginDateTimeUTC DATETIME = CAST(DATEADD(HOUR, 6, CAST(@BeginDateCET AS DATETIME)) AT TIME ZONE 'Romance Standard Time' AT TIME ZONE 'UTC' AS DATETIME)
    DECLARE @EndDateTimeUTC DATETIME = CAST(DATEADD(HOUR, 6, CAST(@EndDateCET AS DATETIME)) AT TIME ZONE 'Romance Standard Time' AT TIME ZONE 'UTC' AS DATETIME)
    DECLARE @Dates TABLE(
        DateTimeUTC DATETIME PRIMARY KEY CLUSTERED
    )
    -- This table adds all trades, split up by month. This can be used to calculate the PnL over closed positions.
    INSERT @Dates([DateTimeUTC])
    SELECT [ddtu].[DateTimeUTC]
    FROM [pub].[dimDateTimeUTC] AS [ddtu]
    WHERE [ddtu].[DateTimeUTC] >= @BeginDateTimeUTC
    AND [ddtu].[DateTimeUTC]< @EndDateCET
    AND DATEPART(DAY, [ddtu].[DateTimeCET]) = 1
    AND DATEPART(HOUR, [ddtu].[DateTimeCET]) = 6
    DECLARE @Curvemapping TABLE(
        [ID] INT
        ,[Area] NVARCHAR(50) NOT NULL
    )
    INSERT @Curvemapping([ID],[Area])
    SELECT 
    [s].[ID]
    ,[s].[Area]
    FROM(
        VALUES
            (1000300226, 'TTF')
            ,(1000300226, 'ETF')
            ,(1000300228, 'NCG')   
            ,(1000300228, 'NCG-H')    
            ,(1000300228, 'NCG-L')
            ,(1000300228, 'NCG-L')    
            ,(1000300229, 'GASPOOL')  
            ,(1000300229, 'GASPOOL-H')
            ,(1000300229, 'GASPOOL-L')
            ,(1000300227, 'NBP') 
            ,(1000300227, 'INT')        
            ,(1000300230, 'PEG')      
            ,(1000300231, 'CEGH-VTP') 
            ,(1000340436, 'CZ-VTP')   
            ,(1000300234, 'PSV')      
            ,(1000300233, 'ZEE')      
            ,(1000300232, 'ZTP-H') 
            ,(1000300232, 'ZTP-L')         
            ,(1000300228, 'THE')      
            ,(1000300228, 'THE-H')    
            ,(1000300228, 'THE-L')    
            ,(1000300228, 'THE-L')    
    ) AS [s]([ID],[Area])
    SELECT 
    [d].[DateTimeUTC] AT TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time' AS [DeliveryBeginCET]
    ,DATEADD(MONTH, 1, [d].[DateTimeUTC] AT TIME ZONE 'UTC' AT TIME ZONE 'Romance Standard Time') AS [DeliveryEndCET]
    ,CASE
        WHEN [igt].[GasNetworkPointCode] = 'VIP-THE-DK' THEN 'VIP DK-THE'
        ELSE [igt].[GasNetworkPointCode]
        END AS [GasnetworkpointCode]
    ,CASE 
        WHEN [igt].[GasPriceAreaOwn] = 'CH' AND [igt].[GasPriceAreaCounterpart] = 'PSV' THEN 'THE-H'
        WHEN [igt].[GasPriceAreaOwn] = 'CH' AND [igt].[GasPriceAreaCounterpart] = 'THE-H' THEN 'PSV'
        ELSE [igt].[GaspriceAreaOwn]
    END AS [GaspriceAreaOwn]
    ,CASE 
        WHEN [igt].[GasPriceAreaCounterpart] = 'CH' AND [igt].[GasPriceAreaOwn] = 'PSV' THEN 'THE-H'
        WHEN [igt].[GasPriceAreaCounterpart] = 'CH' AND [igt].[GasPriceAreaOwn] = 'THE-H' THEN 'PSV'
        ELSE [igt].[GasPriceAreaCounterpart]
    END AS [GasPriceAreaCounterpart]
    ,[igt].[TradingDirectionCode]
    ,[igt].[CounterPart]
    ,[igt].[CapacityCategoryCode]
    ,[igt].[PeriodTypeCode]
    ,[igt].[Period]
    ,[igt].[Tariff]
    ,[igt].[AdditionalFee]
    ,AVG([curves].[value]) AS [Price]
    ,CASE
        WHEN [igt].[Currency] = 'DKK' THEN ([igt].[Tariff] + [igt].[AdditionalFee]) * 0.13447228
        ELSE [igt].[Tariff] + [igt].[AdditionalFee]
    END AS [TotalFee]
    ,CASE
        WHEN [igt].[Currency] = 'DKK' THEN 'EUR'
        ELSE [igt].[Currency]
    END AS [Currency]
    ,[igt].[Unit]
    ,[igt].[Factor]
    ,CASE 
        WHEN [igt].[VariableFeeCurrency] = 'DKK/MWh' THEN [igt].[Variablefee] * 0.13447228
        WHEN [igt].[VariableFeeCurrency] = '% GasPrice' THEN [igt].[Variablefee] * AVG([curves].[value]) / 100
        ELSE [igt].[VariableFee]
    END AS [VariableFee]
    ,CASE 
        WHEN [igt].[VariableFeeCurrency] = 'DKK/MWh' THEN 'EUR/MWh'
        WHEN [igt].[VariableFeeCurrency] = '% GasPrice' THEN 'EUR/MWh'
        ELSE [igt].[VariableFeeCurrency]
    END AS [VariableFeeCurrency]
    ,[crv].[ID]
    FROM(
    SELECT 
    [t1].[GasNetworkPointCode]
    ,[t2].[GasPriceAreaOwn]
    ,[t2].[GasPriceAreaCounterpart]
    ,[t1].[TradingDirectionCode]
    ,[t1].[CounterPart]
    ,[t2].[CapacityCategoryCode]
    ,[t1].[TariffFactorBeginUTC] AS [TariffBeginUTC]
    ,[t1].[TariffFactorEndUTC] AS [TariffEndUTC]
    ,[t1].[PeriodTypeCode]
    ,[t1].[Period]
    ,[t1].[Factor] * [t2].[Tariff] as [Tariff]
    ,[t1].[Factor] * [t2].[AdditionalFee] AS [AdditionalFee]
    ,[t2].[Currency]
    ,[t2].[Unit]
    ,[t1].[Factor]
    ,[t1].[VariableFee]
    ,[t1].[VariableFeeCurrency]
    FROM [pub].[dimGasTariffFactor] as t1
    LEFT JOIN [pub].[dimGasTariff] as t2
    on t1.CounterPart=t2.CounterPart and t1.GasNetworkPointCode=t2.GasNetworkPointCode and t1.TradingDirectionCode=t2.TradingDirectionCode and t1.TariffFactorBeginCET>= t2.TariffBeginCET and t1.TariffFactorEndCET<=t2.TariffEndCET
    WHERE [t1].[GasNetworkPointCode] <> ''
    UNION ALL 
    SELECT 
    [t3].[GasNetworkPointCode]
    ,[t3].[GasPriceAreaOwn] 
    ,[t3].[GasPriceAreaCounterpart]
    ,[t3].[TradingDirectionCode]
    ,[t3].[CounterPart]
    ,[t3].[CapacityCategoryCode]
    ,[t3].[TariffBeginUTC]
    ,[t3].[TariffEndUTC]
    ,[t3].[PeriodTypeCode]
    ,[t3].[Period]
    ,[t3].[Tariff]
    ,[t3].[AdditionalFee]
    ,[t3].[Currency]
    ,[t3].[Unit]
    ,1 AS [Factor]
    ,AVG([t4].[VariableFee]) AS [Variablefee]
    ,[t4].[VariableFeeCurrency]
    FROM [pub].[dimGasTariff] [t3]
    LEFT JOIN [pub].[dimGasTariffFactor] [t4] ON [t3].[GasNetworkPointCode] = [t4].[GasNetworkPointCode] AND [t3].[Counterpart] = [t4].[Counterpart] 
    GROUP BY 
    [t3].[GasNetworkPointCode]
    ,[t3].[GasPriceAreaOwn] 
    ,[t3].[GasPriceAreaCounterpart]
    ,[t3].[TradingDirectionCode]
    ,[t3].[CounterPart]
    ,[t3].[CapacityCategoryCode]
    ,[t3].[TariffBeginUTC]
    ,[t3].[TariffEndUTC]
    ,[t3].[PeriodTypeCode]
    ,[t3].[Period]
    ,[t3].[Tariff]
    ,[t3].[AdditionalFee]
    ,[t3].[Currency]
    ,[t3].[Unit]
    ,[t4].[VariableFeeCurrency]
    ) AS [igt]
    LEFT JOIN @Dates AS [d] ON [d].[DateTimeUTC] >= [igt].[TariffBeginUTC] AND [igt].[TariffEndUTC]>[d].[DateTimeUTC] 
    LEFT JOIN @CurveMapping AS [crv] ON [crv].[Area] = [igt].[GasPriceAreaCounterpart]
    LEFT JOIN [pub].[TimeSeries1_v02] AS [curves] ON [curves].[ValueDateUTC] >= [d].[DateTimeUTC] AND DATEADD(MONTH, 1, [d].[DateTimeUTC]) > [curves].[ValueDateUTC] AND [crv].[ID] = [curves].[CurveId]
    WHERE ISNULL([GasPriceAreaOwn],'')<>''
    AND [curves].[IsLatestForecast] = 1
    AND [curves].[ForecastDateUTC] > '2021-10-01'
    AND [curves].[CurveId] IN (1000300226, 1000300227, 1000300228, 1000300229, 1000300230, 1000300231, 1000300232, 1000300233, 1000300234, 1000300235, 1000300236)
    AND ISNULL([GasPriceAreaCounterpart],'')<>''
    AND [GasNetworkPointCode] <> 'N/A'
    AND [GasNetworkPointCode] <> 'Bacton (BBL)'
    AND [GasNetworkPointCode] <> 'Bacton (IUK)'
    AND [GasNetworkPointCode] <> 'IZT'
    AND [GasNetworkPointCode] <> 'ZEEBRUGGE'
    AND [GasNetworkPointCode] <> 'Loenhout'
    AND [GasNetworkPointCode] <> 'UBERACKERN'
    AND [GasnetworkpointCode] <> 'Eynatten'
    GROUP BY
    [d].[DateTimeUTC] 
    ,[igt].[GasNetworkPointCode]
    ,[igt].[GasPriceAreaOwn]
    ,[igt].[GasPriceAreaCounterpart]
    ,[igt].[TradingDirectionCode]
    ,[igt].[CounterPart]
    ,[igt].[CapacityCategoryCode]
    ,[igt].[PeriodTypeCode]
    ,[igt].[Period]
    ,[igt].[Tariff]
    ,[igt].[AdditionalFee]
    ,[igt].[Currency]
    ,[igt].[Currency]
    ,[igt].[Unit]
    ,[igt].[Factor]
    ,[igt].[VariableFeeCurrency]
    ,[igt].[VariableFee]
    ,[crv].[ID]
    ,[igt].[TariffBeginUTC]
    ORDER BY [periodtypecode], [TariffBeginUTC], [GasNetworkPointCode]
"""
    rates = psql.read_sql(query,bi_sql_engine)
    return rates

def fetch_surcharges_costs(fdate,a):
    query = f"""SELECT [Id]
          ,[Direction]
          ,[GasType]
          ,[Category]
          ,[Country]
          ,[Marketable]
          ,[MarketableUnit]
          ,[MarketArea]
          ,[Marketed]
          ,[MarketedUnit]
          ,[NetworkPoint]
          ,[NetworkPointEic]
          ,[Product]
          ,[AuctionStart]
          ,[AuctionEnd]
          ,[RuntimeEnd]
          ,[RuntimeStart]
          ,[StartingPrice]
          ,[StartingPriceUnit]
          ,[State]
          ,[Surcharge]
          ,[SurchargeSplitFactor]
          ,[SurchargeUnit]
          ,[Tariff]
          ,[TariffUnit]
          ,[Tso]
          ,[RowInsertedOrModifiedDateUTC]
          ,[CurveID]
      FROM [pub].[Prisma_Auctions]
      where product = 'DAY'
      and auctionend > '{fdate}'
      and networkpoint like '%{a}%'
      """
    sp = psql.read_sql(query, bi_sql_engine)
    return sp


def nbp_trades():
    q = """WITH cte_table AS (
            SELECT tp.DateTime
                , tp.Price
                , tp.Volume
                , tp.AggressorAction
                , tp.IsOwnData
                , tp.InstID
                , tp.InstName
                , tp.FirstSequenceItemName
                , tp.Action
                , tp.SeqSpan
                , ROW_NUMBER() OVER (PARTITION BY tp.TradeID
                                                , tp.InstID
                                                , tp.FirstSequenceID
                                                , tp.FirstSequenceItemID
                                    ORDER BY tp.LastUpdate DESC
                                    ) AS rn
            FROM pub.[Trayport_Trades_Public]        AS tp
  where FirstSequenceItemID between 2 and 10
  and seqspan = 'Single'
  and instid in (10002071, 10006025, 10002317)
  and datetime > '2022-04-01'
                AND tp.Action IN( 'Query','Remove', 'Insert')
            )
            SELECT c.DateTime
                , c.Price
                , c.Volume
                , c.AggressorAction
                , c.IsOwnData
                , c.InstID
                , c.InstName
                , c.FirstSequenceItemName
                , c.Action
                , c.SeqSpan
            FROM cte_table c
			where rn = 1
            and c.Action not in ('Remove')
"""
    nt = psql.read_sql(q,bi_sql_engine)
    return nt
