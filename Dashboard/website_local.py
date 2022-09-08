import streamlit as st
from pathlib import Path
import sys
import matplotlib.pyplot as plt
import data_loading as dl
import numpy as np
import pandas as pd
from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide")
try:
    framework_path = Path.cwd().parents[1].as_posix()
    new_path = ['', framework_path]
    new_path.extend([path for path in sys.path if path])
    sys.path = new_path
except Exception as e:
    pass


from Dashboard.pages.Metrics.metrics import plot_traybot_usage, plot_market_share_data, plot_trade_data
page = st.sidebar.radio("Page selector", ["Traybot_usage", "Market_share", "Trades_scatter"], index=2)

    
if page == "Traybot_usage":
    plot_traybot_usage()
elif page == 'Market_share':
    plot_market_share_data()
elif page == "Trades_scatter":
    plot_trade_data()

else:
    st.write("TO BE IMPLEMENTED")
    #
    
    
    
    
    
    
