"""
refresh_ctrader_token.py

Quick token refresh utility for Mempalace AI ↔ cTrader integration.

Usage:
  python scripts/refresh_ctrader_token.py

If refresh succeeds → both .env and token_state.json are updated.
If refresh fails   → prints the OAuth URL for manual re-authorization.
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

# ── Resolve paths ──────────────────────────────────────────────────
MEMPALAC_ROOT = Path(__file__).resolve().parent.parent
ROOT_ENV = MEMPALAC_ROOT / ".env"
TRADING_AI_ENV = MEMPALAC_ROOT / "trading_ai" / ".env"
ENV_READ_PRIORITY = (TRADING_AI_ENV, ROOT_ENV)
ENV_UPDATE_TARGETS = (ROOT_ENV, TRADING_AI_ENV)
DEXTER_TOKEN_STATE = Path(r"D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed\data\runtime\ctrader_token_state.json")

# ── Load current credentials from .env ─────────────────────────────
def load_env_value(key: str) -> str:
    for path in ENV_READ_PRIORITY:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped.startswith("#") or "=" not in stripped:
                continue
            k, _, v = stripped.partition("=")
            if k.strip() == key:
                return v.strip()
    return ""


def update_env_value(path: Path, key: str, new_value: str) -> bool:
    if not path.exists():
        return False
    lines = path.read_text(encoding="utf-8").splitlines()
    changed = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        k, _, _ = stripped.partition("=")
        if k.strip() == key:
            lines[i] = f"{key}={new_value}"
            changed = True
            break
    if not changed:
        lines.append(f"{key}={new_value}")
        changed = True
    if changed:
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return changed


def update_all_env_targets(key: str, new_value: str) -> list[Path]:
    updated: list[Path] = []
    for path in ENV_UPDATE_TARGETS:
        if update_env_value(path, key, new_value):
            updated.append(path)
    return updated


client_id = load_env_value("OpenAPI_ClientID")
client_secret = load_env_value("OpenAPI_Secreat") or load_env_value("OpenAPI_Secret")
refresh_token = load_env_value("CTRADER_OPENAPI_REFRESH_TOKEN")
redirect_uri = load_env_value("CTRADER_OPENAPI_REDIRECT_URI") or "http://localhost"

# Also check token state file for a newer refresh token
if DEXTER_TOKEN_STATE.exists():
    try:
        state = json.loads(DEXTER_TOKEN_STATE.read_text(encoding="utf-8"))
        state_refresh = str(state.get("refresh_token", "")).strip()
        if state_refresh:
            print(f"[INFO] Found refresh_token from Dexter state file (failures={state.get('consecutive_failures', '?')})")
            refresh_token = state_refresh
    except Exception as e:
        print(f"[WARN] Could not read Dexter state: {e}")

print(f"[INFO] Client ID:     {client_id[:12]}..." if client_id else "[ERROR] No Client ID found!")
print(f"[INFO] Client Secret: {client_secret[:8]}..." if client_secret else "[ERROR] No Client Secret found!")
print(f"[INFO] Refresh Token: {refresh_token[:12]}..." if refresh_token else "[WARN] No refresh token")
print(f"[INFO] Redirect URI:  {redirect_uri}")
print()

if not client_id or not client_secret:
    print("[FATAL] Missing client_id or client_secret in .env. Cannot proceed.")
    sys.exit(1)

# ── Try refreshing ─────────────────────────────────────────────────
try:
    from ctrader_open_api import Auth
except ImportError:
    print("[ERROR] ctrader_open_api not installed. Run: pip install ctrader-open-api")
    sys.exit(1)

auth = Auth(client_id, client_secret, redirect_uri)

if refresh_token:
    print("[INFO] Attempting token refresh...")
    try:
        result = auth.refreshToken(refresh_token)
        if isinstance(result, dict):
            new_access = str(result.get("accessToken", "")).strip()
            new_refresh = str(result.get("refreshToken", "")).strip()
            
            if new_access:
                print(f"[SUCCESS] New access_token: {new_access[:16]}...")
                print(f"[SUCCESS] New refresh_token: {new_refresh[:16]}...")
                
                # Update active Mempalac env files
                updated_access = update_all_env_targets("CTRADER_OPENAPI_ACCESS_TOKEN", new_access)
                updated_refresh = update_all_env_targets("CTRADER_OPENAPI_REFRESH_TOKEN", new_refresh)
                updated_alias_access = update_all_env_targets("OpenAPI_Access_token_API_key3", new_access)
                updated_alias_refresh = update_all_env_targets("OpenAPI_Refresh_token_API_key3", new_refresh)
                updated_paths = sorted(
                    {
                        str(path)
                        for path in (
                            updated_access
                            + updated_refresh
                            + updated_alias_access
                            + updated_alias_refresh
                        )
                    }
                )
                if updated_paths:
                    print(f"[OK] Updated env files: {', '.join(updated_paths)}")
                
                # Update Dexter token state
                if DEXTER_TOKEN_STATE.parent.exists():
                    DEXTER_TOKEN_STATE.write_text(json.dumps({
                        "access_token": new_access,
                        "refresh_token": new_refresh,
                        "last_refresh_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        "refresh_count": 1,
                        "consecutive_failures": 0,
                        "saved_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    }, indent=2), encoding="utf-8")
                    print("[OK] Updated Dexter token_state.json (reset failures to 0)")
                
                print("\n✅ Token refresh complete! Restart the Mempalace loop now.")
                sys.exit(0)
            else:
                error_desc = result.get("description", result.get("errorCode", "unknown"))
                print(f"[FAIL] Refresh returned empty accessToken: {error_desc}")
        else:
            print(f"[FAIL] Unexpected refresh response: {result}")
    except Exception as e:
        print(f"[FAIL] Refresh exception: {e}")

# ── Fallback: Print manual OAuth URL ───────────────────────────────
print()
print("=" * 70)
print("  MANUAL RE-AUTHORIZATION REQUIRED")
print("=" * 70)
print()
print("Your refresh token has expired. You must re-authorize via browser.")
print()

auth_url = (
    f"https://openapi.ctrader.com/apps/auth?"
    f"client_id={client_id}"
    f"&redirect_uri={redirect_uri}"
    f"&scope=trading"
)
print(f"1. Open this URL in your browser:\n   {auth_url}\n")
print("2. Log in and authorize the app")
print("3. Copy the 'code' parameter from the redirect URL")
print()

code = input("4. Paste the authorization code here: ").strip()
if not code:
    print("[ABORT] No code entered.")
    sys.exit(1)

print(f"\n[INFO] Exchanging authorization code for tokens...")
try:
    token_result = auth.getToken(code)
    if isinstance(token_result, dict):
        new_access = str(token_result.get("accessToken", "")).strip()
        new_refresh = str(token_result.get("refreshToken", "")).strip()
        
        if new_access:
            print(f"[SUCCESS] access_token:  {new_access[:16]}...")
            print(f"[SUCCESS] refresh_token: {new_refresh[:16]}...")
            
            updated_access = update_all_env_targets("CTRADER_OPENAPI_ACCESS_TOKEN", new_access)
            updated_refresh = update_all_env_targets("CTRADER_OPENAPI_REFRESH_TOKEN", new_refresh)
            updated_alias_access = update_all_env_targets("OpenAPI_Access_token_API_key3", new_access)
            updated_alias_refresh = update_all_env_targets("OpenAPI_Refresh_token_API_key3", new_refresh)
            updated_paths = sorted(
                {
                    str(path)
                    for path in (
                        updated_access
                        + updated_refresh
                        + updated_alias_access
                        + updated_alias_refresh
                    )
                }
            )
            if updated_paths:
                print(f"[OK] Updated env files: {', '.join(updated_paths)}")
            
            if DEXTER_TOKEN_STATE.parent.exists():
                DEXTER_TOKEN_STATE.write_text(json.dumps({
                    "access_token": new_access,
                    "refresh_token": new_refresh,
                    "last_refresh_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    "refresh_count": 0,
                    "consecutive_failures": 0,
                    "saved_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                }, indent=2), encoding="utf-8")
                print("[OK] Updated Dexter token_state.json")
            
            print("\n✅ Authorization complete! Restart the Mempalace loop now.")
        else:
            print(f"[ERROR] Token exchange failed: {token_result}")
    else:
        print(f"[ERROR] Unexpected response: {token_result}")
except Exception as e:
    print(f"[ERROR] Token exchange error: {e}")
    sys.exit(1)
