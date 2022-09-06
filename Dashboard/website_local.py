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


from UK_tool.pages.UK.UK_supply_demand_data import plot_UK_linepack
page = st.sidebar.radio("Page selector", ["UK"], index=0)

# st.session_state[""]

    

if page == "UK":
    plot_UK_linepack()
    detail = st.sidebar.radio("Do we need more detail on a subarea?", ("No", "storages", "LNG", "imports", "plants", "supply_demand_forecast", "supply_demand_progression"))
    choices = {"storages": "storages_hist", "LNG": "LNG_hist", "imports": "imports_hist", "plants": "plants_hist", "supply_demand_forecast": "fc_data_hist", "supply_demand_progression": "supdemdata_hist"}
    l,m,r = st.beta_columns(3)
    if detail != "No":
        df = st.session_state[choices[detail]].copy()
        cols = df.columns
        subseries = np.array([st.sidebar.checkbox(x,True) for x in cols])
        df2 = df[cols[subseries]]
        with l:
            fig4 = plt.figure()
            ax4 = fig4.add_subplot(1,1,1)
            df2.plot(ax=ax4)
            st.write(fig4)
        with r:
            st.dataframe(df2)
    with m:
        fig5 = plt.figure()
        ax5 = fig5.add_subplot(1,1,1)
        
        st.session_state["fpn_ccgt"].Value.plot(ax=ax5)
        plt.ylabel("GW")
        plt.title("CCGT FPN")
        st.write(fig5)

else:
    st.write("TO BE IMPLEMENTED")
    #