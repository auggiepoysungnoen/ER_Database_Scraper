"""
This page has been replaced by the Search Engine.
Auto-redirects to 00_Search_Engine.py after auth check.
"""
import sys
from pathlib import Path
import streamlit as st

st.set_page_config(page_title="Search Engine | Hickey Lab", page_icon="🔍")

sys.path.insert(0, str(Path(__file__).parent.parent))
from auth import check_password

if not check_password():
    st.stop()

st.switch_page("pages/00_Search_Engine.py")
