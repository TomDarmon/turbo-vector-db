from __future__ import annotations

import argparse
import json
import shutil
import subprocess
from pathlib import Path
from urllib import request


REPO_ROOT = Path(__file__).resolve().parents[2]
PYTHON_DIR = REPO_ROOT / "python"
DEFAULT_OPENAPI_URL = "http://127.0.0.1:8080/openapi.json"
DEFAULT_OPENAPI_JSON_PATH = PYTHON_DIR / "openapi" / "turbo-vector.openapi.json"
DEFAULT_SDK_OUTPUT_PATH = PYTHON_DIR / "sdk"
DEFAULT_GENERATOR_CONFIG = PYTHON_DIR / "openapi-generator-config.yaml"
DEFAULT_GENERATOR_IMAGE = "openapitools/openapi-generator-cli:latest"
DEFAULT_GENERATOR_NAME = "python-pydantic-v1"
GLOBAL_PROPERTIES = "apiTests=false,modelTests=false,apiDocs=false,modelDocs=false"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=("Regenerate the Python SDK from turbo-vector API OpenAPI spec.")
    )
    parser.add_argument(
        "--openapi-url",
        default=DEFAULT_OPENAPI_URL,
        help="OpenAPI URL to fetch when --openapi-file is not provided.",
    )
    parser.add_argument(
        "--openapi-file",
        default=None,
        help="Existing OpenAPI JSON file path to use instead of fetching from --openapi-url.",
    )
    parser.add_argument(
        "--openapi-json-output",
        default=str(DEFAULT_OPENAPI_JSON_PATH),
        help="Path where the resolved OpenAPI JSON should be written.",
    )
    parser.add_argument(
        "--sdk-output-dir",
        default=str(DEFAULT_SDK_OUTPUT_PATH),
        help="Directory where the generated SDK will be written.",
    )
    parser.add_argument(
        "--generator-config",
        default=str(DEFAULT_GENERATOR_CONFIG),
        help="OpenAPI Generator YAML config file path.",
    )
    parser.add_argument(
        "--generator-image",
        default=DEFAULT_GENERATOR_IMAGE,
        help="Docker image for OpenAPI Generator CLI.",
    )
    parser.add_argument(
        "--no-clean",
        action="store_true",
        help="Do not remove the existing SDK output directory before generation.",
    )
    return parser.parse_args()


def resolve_openapi(args: argparse.Namespace, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.openapi_file:
        source_path = Path(args.openapi_file).expanduser().resolve()
        raw = source_path.read_text(encoding="utf-8")
    else:
        with request.urlopen(args.openapi_url, timeout=15) as response:
            if response.status != 200:
                raise RuntimeError(
                    f"failed to fetch OpenAPI spec: {response.status} {response.reason}"
                )
            raw = response.read().decode("utf-8")

    parsed = json.loads(raw)
    output_path.write_text(json.dumps(parsed, indent=2) + "\n", encoding="utf-8")
    return output_path


def ensure_docker_available() -> None:
    if shutil.which("docker") is None:
        raise RuntimeError(
            "docker is required to run OpenAPI Generator. Install Docker and ensure it is on PATH."
        )
    subprocess.run(
        ["docker", "info"],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def to_container_path(path: Path) -> str:
    resolved = path.expanduser().resolve()
    try:
        relative = resolved.relative_to(REPO_ROOT)
    except ValueError as exc:
        raise ValueError(
            f"path must be inside repository root ({REPO_ROOT}): {resolved}"
        ) from exc
    return f"/local/{relative.as_posix()}"


def generate_sdk(
    openapi_path: Path,
    sdk_output_dir: Path,
    config_path: Path,
    generator_image: str,
    clean: bool,
) -> None:
    if clean and sdk_output_dir.exists():
        shutil.rmtree(sdk_output_dir)

    openapi_container_path = to_container_path(openapi_path)
    output_container_path = to_container_path(sdk_output_dir)
    config_container_path = to_container_path(config_path)

    cmd = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{REPO_ROOT}:/local",
        generator_image,
        "generate",
        "-i",
        openapi_container_path,
        "-g",
        DEFAULT_GENERATOR_NAME,
        "-o",
        output_container_path,
        "--config",
        config_container_path,
        "--global-property",
        GLOBAL_PROPERTIES,
    ]
    subprocess.run(cmd, cwd=REPO_ROOT, check=True)

    # Keep the checked-in SDK output focused on generated client code.
    metadata_dir = sdk_output_dir / ".openapi-generator"
    if metadata_dir.exists():
        shutil.rmtree(metadata_dir)
    metadata_ignore = sdk_output_dir / ".openapi-generator-ignore"
    if metadata_ignore.exists():
        metadata_ignore.unlink()


def main() -> int:
    args = parse_args()
    openapi_output = Path(args.openapi_json_output).expanduser().resolve()
    sdk_output = Path(args.sdk_output_dir).expanduser().resolve()
    config_path = Path(args.generator_config).expanduser().resolve()

    if not config_path.exists():
        raise FileNotFoundError(f"generator config does not exist: {config_path}")

    ensure_docker_available()
    openapi_path = resolve_openapi(args, openapi_output)
    generate_sdk(
        openapi_path=openapi_path,
        sdk_output_dir=sdk_output,
        config_path=config_path,
        generator_image=args.generator_image,
        clean=not args.no_clean,
    )
    print(f"wrote OpenAPI spec: {openapi_path}")
    print(f"updated SDK output: {sdk_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
