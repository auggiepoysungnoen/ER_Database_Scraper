"""
auth.py
=======
Bcrypt-based authentication gate for the Hickey Lab Genomics Search app.

Reads credentials from st.secrets["auth"] and manages session state
with an 8-hour timeout.
"""

import time

import bcrypt
import streamlit as st


_SESSION_TIMEOUT_SECONDS = 8 * 60 * 60  # 8 hours


def _is_session_valid() -> bool:
    """Return True if the user is authenticated and the session has not expired."""
    if not st.session_state.get("authenticated", False):
        return False
    auth_time = st.session_state.get("auth_time", 0)
    if time.time() - auth_time > _SESSION_TIMEOUT_SECONDS:
        st.session_state["authenticated"] = False
        return False
    return True


def check_password() -> bool:
    """Display a login form and return True if the user is authenticated.

    Uses bcrypt to verify the password against the hash stored in
    ``st.secrets["auth"]["password_hash"]``.  On success, sets
    ``st.session_state["authenticated"] = True`` and records the
    authentication timestamp.

    Returns
    -------
    bool
        True if the user is authenticated, False otherwise.
    """
    if _is_session_valid():
        return True

    # --- Custom CSS for the login form ---
    st.markdown(
        """
        <style>
        .login-container {
            max-width: 420px;
            margin: 80px auto 0 auto;
            padding: 2.5rem 2rem;
            border: 1px solid #e0e0e0;
            border-radius: 12px;
            background: #ffffff;
            box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        }
        .login-title {
            color: #012169;
            font-family: Arial, sans-serif;
            font-size: 1.6rem;
            font-weight: 700;
            text-align: center;
            margin-bottom: 0.2rem;
        }
        .login-subtitle {
            color: #555;
            font-family: Arial, sans-serif;
            font-size: 0.95rem;
            text-align: center;
            margin-bottom: 1.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<p class="login-title">Hickey Lab Genomics Search</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="login-subtitle">Department of Biomedical Engineering &middot; Duke University</p>',
        unsafe_allow_html=True,
    )

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign in", use_container_width=True)

    if submitted:
        expected_user = st.secrets["auth"]["username"]
        expected_hash = st.secrets["auth"]["password_hash"]
        if username == expected_user and bcrypt.checkpw(
            password.encode("utf-8"), expected_hash.encode("utf-8")
        ):
            st.session_state["authenticated"] = True
            st.session_state["auth_time"] = time.time()
            st.rerun()
        else:
            st.error("Invalid username or password.")

    st.markdown("</div>", unsafe_allow_html=True)
    return False
