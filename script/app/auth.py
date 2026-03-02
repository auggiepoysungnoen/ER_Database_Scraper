import bcrypt
import streamlit as st
from datetime import datetime, timedelta

SESSION_TIMEOUT_HOURS = 1

DUKE_BLUE = "#00539B"
DUKE_NAVY = "#012169"
DUKE_GOLD = "#B5A369"


def check_password() -> bool:
    """
    Renders a login form and validates credentials against st.secrets.
    Returns True if authenticated, False otherwise.
    Session expires after 1 hour (tracked in st.session_state).
    """
    # Check existing session
    if st.session_state.get("authenticated"):
        auth_time = st.session_state.get("auth_time")
        if auth_time and datetime.now() - auth_time < timedelta(hours=SESSION_TIMEOUT_HOURS):
            return True
        else:
            # Session expired — clear state
            st.session_state["authenticated"] = False
            st.session_state.pop("auth_time", None)
            st.warning("Your session has expired. Please log in again.")

    # Render login form
    _render_login_form()
    return False


def _render_login_form() -> None:
    """Render the Duke-branded centered login form."""
    st.markdown(
        f"""
        <style>
        .login-header {{
            text-align: center;
            color: {DUKE_BLUE};
            font-family: system-ui, sans-serif;
        }}
        .login-subheader {{
            text-align: center;
            color: {DUKE_NAVY};
            font-size: 0.95rem;
            margin-bottom: 1.5rem;
            font-family: system-ui, sans-serif;
        }}
        .login-divider {{
            border-top: 3px solid {DUKE_GOLD};
            margin: 0.5rem 0 1.5rem 0;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Center the form using columns for padding
    left_pad, center_col, right_pad = st.columns([1, 2, 1])

    with center_col:
        st.markdown(
            '<h2 class="login-header">HICKEY LAB</h2>', unsafe_allow_html=True
        )
        st.markdown(
            '<p class="login-subheader">Endometrial Receptivity Database &mdash; Duke University</p>',
            unsafe_allow_html=True,
        )
        st.markdown('<div class="login-divider"></div>', unsafe_allow_html=True)

        with st.form("login_form", clear_on_submit=False):
            username = st.text_input(
                "Username",
                placeholder="Enter username",
                autocomplete="username",
            )
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter password",
                autocomplete="current-password",
            )
            submitted = st.form_submit_button(
                "Sign In",
                use_container_width=True,
                type="primary",
            )

        if submitted:
            _validate_credentials(username, password)


def _validate_credentials(username: str, password: str) -> None:
    """Validate username/password against st.secrets and set session state."""
    try:
        expected_username = st.secrets["auth"]["username"]
        password_hash = st.secrets["auth"]["password_hash"]
    except KeyError:
        st.error(
            "Authentication configuration is missing. "
            "Please add [auth] username and password_hash to .streamlit/secrets.toml."
        )
        return

    username_ok = username == expected_username
    try:
        password_ok = bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        password_ok = False

    if username_ok and password_ok:
        st.session_state["authenticated"] = True
        st.session_state["auth_time"] = datetime.now()
        st.rerun()
    else:
        # Do not reveal which field is wrong
        st.error("Invalid credentials. Please try again.")
