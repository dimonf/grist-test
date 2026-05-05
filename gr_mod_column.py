from __future__ import annotations

import os
import sys
from pathlib import Path

import yaml
from pygrister.api import GristApi
from requests import HTTPError


CONFIG_FILE = Path(__file__).with_name("gr_columns.yaml")


def make_grist_api() -> GristApi:
    api_key = os.environ.get("GRIST_API_KEY")
    doc_id = os.environ.get("GRIST_DOC_ID")
    home = os.environ.get("GRIST_SELF_MANAGED_HOME", "http://localhost:8484").rstrip("/")
    single_org = os.environ.get("GRIST_SELF_MANAGED_SINGLE_ORG", "Y")
    team_site = os.environ.get("GRIST_TEAM_SITE", "")

    missing = [
        name
        for name, value in (("GRIST_API_KEY", api_key), ("GRIST_DOC_ID", doc_id))
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    if single_org != "Y" and not team_site:
        raise RuntimeError("GRIST_TEAM_SITE is required when GRIST_SELF_MANAGED_SINGLE_ORG=N")

    return GristApi(
        config={
            "GRIST_API_KEY": api_key,
            "GRIST_DOC_ID": doc_id,
            "GRIST_SELF_MANAGED": "Y",
            "GRIST_SELF_MANAGED_HOME": home,
            "GRIST_SELF_MANAGED_SINGLE_ORG": single_org,
            "GRIST_TEAM_SITE": team_site or "docs",
            "GRIST_RAISE_ERROR": "Y",
            "GRIST_SAFEMODE": "N",
        }
    )


def resolve_table_id(api: GristApi, wanted: str) -> str:
    _status, tables = api.list_tables()
    wanted_lower = wanted.casefold()
    for table in tables:
        if isinstance(table, str) and table.casefold() == wanted_lower:
            return table
        if not isinstance(table, dict):
            continue
        for key in ("id", "tableId", "name", "title"):
            value = table.get(key)
            if isinstance(value, str) and value.casefold() == wanted_lower:
                return value
    raise RuntimeError(f"Could not find table: {wanted}")


def find_column(api: GristApi, table_id: str, column_id: str) -> dict | None:
    _status, columns = api.list_cols(table_id)
    for column in columns:
        if isinstance(column, dict) and column.get("id") == column_id:
            return column
        if isinstance(column, str) and column == column_id:
            return {"id": column}
    return None


def column_payload(column_id: str, fields: dict) -> dict:
    return {"id": column_id, "fields": fields}


def load_column_specs(config_file: Path = CONFIG_FILE) -> list[dict]:
    with config_file.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise RuntimeError(f"{config_file} must contain a YAML mapping")
    tables = config.get("tables")
    if not isinstance(tables, list) or not tables:
        raise RuntimeError(f"{config_file} must define at least one tables entry")

    specs = []
    for table_index, table in enumerate(tables, start=1):
        if not isinstance(table, dict):
            raise RuntimeError(f"tables entry {table_index} must be a table")

        table_name = table.get("table_name")
        if not isinstance(table_name, str) or not table_name:
            raise RuntimeError(f"tables entry {table_index} must define table_name")

        columns = table.get("columns")
        if not isinstance(columns, list) or not columns:
            raise RuntimeError(
                f"tables entry {table_index} must define at least one columns entry"
            )

        for column_index, column in enumerate(columns, start=1):
            if not isinstance(column, dict):
                raise RuntimeError(
                    f"columns entry {column_index} in {table_name} must be a table"
                )

            column_id = column.get("column_id")
            fields = column.get("fields")
            if not isinstance(column_id, str) or not column_id:
                raise RuntimeError(
                    f"columns entry {column_index} in {table_name} must define column_id"
                )
            if not isinstance(fields, dict) or not fields:
                raise RuntimeError(
                    f"columns entry {column_index} in {table_name} must define fields"
                )

            specs.append(
                {
                    "table_name": table_name,
                    "column_id": column_id,
                    "fields": fields,
                }
            )
    return specs


def add_or_update_column(
    api: GristApi, table_id: str, column_id: str, fields: dict
) -> str:
    existing = find_column(api, table_id, column_id)
    payload = column_payload(column_id, fields.copy())
    try:
        if existing is None:
            api.add_cols(table_id, [payload])
            return "added"
        api.update_cols(table_id, [payload])
        return "updated"
    except HTTPError as exc:
        response = getattr(api.apicaller, "response", None)
        response_text = getattr(response, "text", "") if response is not None else ""
        raise RuntimeError(
            f"Could not add/update {column_id} on {table_id}: {exc}; "
            f"response={response_text}; payload={payload!r}"
        ) from exc


def main() -> int:
    try:
        api = make_grist_api()
        for spec in load_column_specs():
            table_id = resolve_table_id(api, spec["table_name"])
            action = add_or_update_column(
                api, table_id, spec["column_id"], spec["fields"]
            )
            print(f"{action} column {spec['column_id']} in table {table_id}")
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
