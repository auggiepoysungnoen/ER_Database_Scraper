# API Keys and Credentials

> This file is a template guide only. Never commit actual keys to git.
> Store real keys in `.streamlit/secrets.toml` (gitignored).

---

## NCBI API Key (Required for GEO scraping)

Without a key: 3 requests/second limit.
With a key: 10 requests/second (strongly recommended for large runs).

**How to obtain:**
1. Create a free NCBI account: https://www.ncbi.nlm.nih.gov/account/
2. Go to Account Settings → API Key Management
3. Click "Create an API Key"
4. Copy the key into `secrets.toml`:

```toml
[ncbi]
api_key = "your_key_here"
```

Or set as environment variable:
```bash
export NCBI_API_KEY="your_key_here"
```

---

## Human Cell Atlas (No key required)

HCA public data does not require authentication.
Leave `token = ""` in `secrets.toml`.

---

## CELLxGENE Census (No key required)

The `cellxgene-census` package accesses public data without authentication.
First run downloads a large index (~2 GB) to `~/.cache/cellxgene_census/`.

---

## Streamlit App Password

Generate a bcrypt hash of your chosen password:

```bash
python -c "
import bcrypt
pw = input('Enter password: ').encode()
print(bcrypt.hashpw(pw, bcrypt.gensalt()).decode())
"
```

Paste the output into `secrets.toml`:

```toml
[auth]
username = "hickeylab"
password_hash = "$2b$12$..."
```

---

## Template: `.streamlit/secrets.toml`

```toml
[auth]
username = "hickeylab"
password_hash = "$2b$12$REPLACE_WITH_BCRYPT_HASH"

[ncbi]
api_key = "REPLACE_WITH_NCBI_KEY"

[hca]
token = ""
```
