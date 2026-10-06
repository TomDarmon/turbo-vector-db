# /// script
# dependencies = [
#   "python-dotenv",
# ]
# ///

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


load_dotenv()

DEFAULT_SIGNOZ_URL = os.getenv("TV_SIGNOZ_URL", "http://127.0.0.1:3301")
DEFAULT_TIMEOUT_SECONDS = float(os.getenv("TV_SIGNOZ_TIMEOUT_SECONDS", "15"))
OUTPUT_DIR = Path(__file__).resolve().parent
SIGNOZ_API_TOKEN = os.getenv("TV_SIGNOZ_API_TOKEN")
SIGNOZ_EMAIL = os.getenv("TV_SIGNOZ_EMAIL")
SIGNOZ_PASSWORD = os.getenv("TV_SIGNOZ_PASSWORD")


@dataclass(frozen=True)
class ApiResponse:
    status: int
    data: Any | None
    body: str


def api_request(
    method: str,
    url: str,
    *,
    payload: Any | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> ApiResponse:
    request_headers = {"Accept": "application/json"}
    if headers:
        request_headers.update(headers)
    body_bytes: bytes | None = None
    if payload is not None:
        body_bytes = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url=url, method=method, headers=request_headers, data=body_bytes
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8")
            status = int(getattr(response, "status", 200))
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body = exc.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"request failed: {method} {url}: {exc}") from exc

    parsed: Any | None = None
    if body:
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None
    return ApiResponse(status=status, data=parsed, body=body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync generated Turbo Vector dashboards into local SigNoz."
    )
    parser.add_argument("--signoz-url", default=DEFAULT_SIGNOZ_URL)
    parser.add_argument("--dashboards-dir", default=str(OUTPUT_DIR))
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument("--email", default=SIGNOZ_EMAIL)
    parser.add_argument("--password", default=SIGNOZ_PASSWORD)
    return parser.parse_args()


def normalized_base_url(raw: str) -> str:
    return raw.rstrip("/")


def build_auth_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    if SIGNOZ_API_TOKEN:
        # For Personal Access Tokens (PATs), SigNoz usually expects 'signoz-access-token' header.
        # We also include 'Authorization: Bearer' for compatibility.
        headers["Authorization"] = f"Bearer {SIGNOZ_API_TOKEN}"
        headers["signoz-access-token"] = SIGNOZ_API_TOKEN
    return headers


def login_and_get_token(
    base_url: str, args: argparse.Namespace, headers: dict[str, str]
) -> dict[str, str]:
    if headers.get("Authorization") or headers.get("signoz-access-token"):
        return headers
    if not (args.email and args.password):
        return headers

    login_url = f"{base_url}/api/v2/sessions/email_password"
    response = api_request(
        "POST",
        login_url,
        payload={"email": args.email, "password": args.password},
        headers=headers,
        timeout_seconds=args.timeout_seconds,
    )
    if response.status not in {200, 201}:
        raise RuntimeError(
            f"failed to authenticate against SigNoz ({response.status}): "
            f"{response.body[:300]}"
        )
    data = response.data if isinstance(response.data, dict) else {}
    token_payload = data.get("data") if isinstance(data.get("data"), dict) else data
    token = ""
    if isinstance(token_payload, dict):
        for key in ("accessJwt", "token", "accessToken", "jwt"):
            value = token_payload.get(key)
            if isinstance(value, str) and value.strip():
                token = value.strip()
                break
    if not token:
        raise RuntimeError(
            "SigNoz login succeeded but no bearer token was found in response."
        )
    updated = dict(headers)
    updated["Authorization"] = f"Bearer {token}"
    return updated


def list_endpoint_candidates(base_url: str) -> list[str]:
    return [
        f"{base_url}/api/v1/dashboards",
        f"{base_url}/api/v2/dashboards",
    ]


def extract_dashboard_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    possible_lists: list[Any] = []
    if "data" in payload:
        possible_lists.append(payload["data"])
        if isinstance(payload["data"], dict):
            possible_lists.append(payload["data"].get("dashboards"))
            possible_lists.append(payload["data"].get("items"))
    possible_lists.append(payload.get("dashboards"))
    possible_lists.append(payload.get("items"))
    for candidate in possible_lists:
        if isinstance(candidate, list):
            result = [item for item in candidate if isinstance(item, dict)]
            if result:
                return result
    return []


def discover_dashboard_list_endpoint(
    base_url: str, headers: dict[str, str], timeout_seconds: float
) -> tuple[str, list[dict[str, Any]]]:
    last_error = "no dashboard endpoint candidates returned usable results"
    for url in list_endpoint_candidates(base_url):
        response = api_request(
            "GET", url, headers=headers, timeout_seconds=timeout_seconds
        )
        if response.status == 200:
            rows = extract_dashboard_rows(response.data)
            if rows is not None:
                return url, rows
            last_error = f"{url} returned 200 but had an unexpected response format"
            continue
        if response.status in {401, 403}:
            last_error = f"auth failed for {url} (status={response.status})"
            continue
        if response.status != 404:
            last_error = f"{url} returned status={response.status}: {response.body[:200]}"
    raise RuntimeError(
        f"{last_error}. Provide TV_SIGNOZ_API_TOKEN or TV_SIGNOZ_EMAIL/TV_SIGNOZ_PASSWORD."
    )


def dashboard_identity(row: dict[str, Any]) -> tuple[str, str]:
    dashboard_id = ""
    for key in ("id", "uuid", "dashboardId"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            dashboard_id = value.strip()
            break
    title = ""
    for key in ("title", "name"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            title = value.strip()
            break
    if not title:
        data = row.get("data")
        if isinstance(data, dict):
            nested_title = data.get("title")
            if isinstance(nested_title, str):
                title = nested_title.strip()
    return dashboard_id, title


def payload_variants(dashboard_payload: dict[str, Any]) -> list[dict[str, Any]]:
    wrapped = {
        "title": dashboard_payload.get("title", ""),
        "description": dashboard_payload.get("description", ""),
        "tags": dashboard_payload.get("tags", []),
        "data": dashboard_payload,
    }
    return [dashboard_payload, wrapped]


def try_create_dashboard(
    endpoint: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_seconds: float,
) -> bool:
    last_error = "no variants succeeded"
    for variant in payload_variants(payload):
        response = api_request(
            "POST",
            endpoint,
            payload=variant,
            headers=headers,
            timeout_seconds=timeout_seconds,
        )
        if response.status in {200, 201, 204}:
            # Validate that response is actual JSON (not HTML redirect to login)
            if isinstance(response.data, dict):
                # If response has error status, fail
                if response.data.get("status") == "error":
                    error_code = response.data.get("error", {}).get("code", "unknown")
                    error_msg = response.data.get("error", {}).get("message", "unknown error")
                    if error_code in {"unauthenticated", "unauthorized"}:
                        raise RuntimeError(
                            f"dashboard create auth failed: {error_msg}. "
                            f"Provide TV_SIGNOZ_API_TOKEN or TV_SIGNOZ_EMAIL/TV_SIGNOZ_PASSWORD."
                        )
                    last_error = f"API error ({error_code}): {error_msg}"
                    continue
                # Response is valid JSON, likely successful
                return True
            # Response is not JSON (likely HTML), check next variant
            last_error = f"received {response.status} but response is not JSON (got {type(response.data).__name__})"
            continue
        if response.status in {401, 403}:
            raise RuntimeError(
                f"dashboard create auth failed (status={response.status}) on {endpoint}. "
                f"Provide TV_SIGNOZ_API_TOKEN or TV_SIGNOZ_EMAIL/TV_SIGNOZ_PASSWORD."
            )
        last_error = f"HTTP {response.status}: {response.body[:100]}"
    raise RuntimeError(f"failed to create dashboard on {endpoint}: {last_error}")





def try_update_dashboard(
    endpoint: str,
    dashboard_id: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_seconds: float,
) -> bool:
    update_url = f"{endpoint.rstrip('/')}/{dashboard_id}"
    for method in ("PUT", "PATCH"):
        for variant in payload_variants(payload):
            response = api_request(
                method,
                update_url,
                payload=variant,
                headers=headers,
                timeout_seconds=timeout_seconds,
            )
            if response.status in {200, 201, 204}:
                return True
            if response.status in {401, 403}:
                raise RuntimeError(
                    f"dashboard update auth failed (status={response.status}) on {update_url}"
                )
    return False


def load_dashboard_payloads(dashboards_dir: Path) -> list[tuple[Path, dict[str, Any]]]:
    files = sorted(dashboards_dir.glob("dashboard-*.json"))
    if not files:
        raise RuntimeError(f"no dashboard-*.json files found in {dashboards_dir}")
    payloads: list[tuple[Path, dict[str, Any]]] = []
    for path in files:
        with path.open(encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise RuntimeError(f"{path} does not contain a JSON object payload")
        title = payload.get("title")
        if not isinstance(title, str) or not title.strip():
            raise RuntimeError(f"{path} is missing a non-empty dashboard title")
        payloads.append((path, payload))
    return payloads


def sync_dashboards(args: argparse.Namespace) -> None:
    base_url = normalized_base_url(args.signoz_url)
    dashboards_dir = Path(args.dashboards_dir).resolve()
    headers = build_auth_headers()
    headers = login_and_get_token(base_url, args, headers)

    list_endpoint, existing_rows = discover_dashboard_list_endpoint(
        base_url, headers, args.timeout_seconds
    )
    existing_by_title: dict[str, str] = {}
    for row in existing_rows:
        dashboard_id, title = dashboard_identity(row)
        if title:
            existing_by_title[title] = dashboard_id

    created = 0
    updated = 0
    payloads = load_dashboard_payloads(dashboards_dir)
    for path, payload in payloads:
        title = str(payload["title"]).strip()
        dashboard_id = existing_by_title.get(title, "")
        if dashboard_id:
            if try_update_dashboard(
                list_endpoint, dashboard_id, payload, headers, args.timeout_seconds
            ):
                print(f"updated dashboard: {title} ({path.name})")
                updated += 1
                continue
            raise RuntimeError(
                f"failed to update dashboard '{title}' at endpoint {list_endpoint}"
            )

        if try_create_dashboard(list_endpoint, payload, headers, args.timeout_seconds):
            print(f"created dashboard: {title} ({path.name})")
            created += 1
            continue
        raise RuntimeError(
            f"failed to create dashboard '{title}' at endpoint {list_endpoint}"
        )

    print(
        f"dashboard sync complete (created={created}, updated={updated}, total={len(payloads)})"
    )


def main() -> int:
    args = parse_args()
    try:
        sync_dashboards(args)
    except Exception as exc:  # noqa: BLE001
        print(f"dashboard sync failed: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
