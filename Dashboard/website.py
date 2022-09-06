import streamlit as st
from pathlib import Path
import sys
st.set_page_config(layout="wide")
try:
    framework_path = Path.cwd().parents[1].as_posix()
    new_path = ['', framework_path]
    new_path.extend([path for path in sys.path if path])
    sys.path = new_path
except Exception as e:
    pass

from flex_tool.pages.required_spread_breakeven.breakeven import breakeven
from flex_tool.pages.show_flex.storages import capacity_bookings
page = st.sidebar.radio("Page selector", ["Single hour breakeven view", "Physical Effect View", "Optimization"], index=0)

if page == "Single hour view":
    breakeven()
elif page == "Physical Effect View":
    capacity_bookings()
else:
    st.write("TO BE IMPLEMENTED")