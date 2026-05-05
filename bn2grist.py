from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import beanquery
from pygrister.api import GristApi
from requests import HTTPError


TRANSACTIONS_TABLE = "transactions"
POSTINGS_TABLE = "postings"
RECORD_BATCH_SIZE = 500
POSTINGS_COLUMN_ORDER = (
    "transaction_id",
    "loc",
    "account",
    "sub",
    "currency",
    "usd",
    "number",
)
GRIST_TABLES_META = "_grist_Tables"
GRIST_COLUMNS_META = "_grist_Tables_column"
GRIST_VIEW_SECTIONS_META = "_grist_Views_section"
GRIST_SECTION_FIELDS_META = "_grist_Views_section_field"


@dataclass(frozen=True)
class ImportBundle:
    transactions: list[dict]
    postings: list[dict]


@dataclass(frozen=True)
class TableIds:
    transactions: str
    postings: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Beancount transactions and postings into Grist."
    )
    parser.add_argument("bean_file", type=Path, help="Path to a Beancount file.")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing rows from the target tables before uploading.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and print a summary without calling the Grist API.",
    )
    parser.add_argument(
        "--preview",
        type=int,
        default=3,
        help="How many rows per table to print in dry-run mode.",
    )
    return parser.parse_args()


def posting_number(number: Decimal | None) -> str | None:
    if number is None:
        return None
    if isinstance(number, Decimal):
        return format(number, "f")
    return str(number)


def grist_date_value(value) -> list[object]:
    timestamp = int(datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp())
    return ["d", timestamp]


def grist_choice_list(values: list[str]) -> list[object]:
    return ["L", *values]


def transform_entries(bean_file: Path) -> ImportBundle:
    conn = beanquery.connect(f"beancount:{bean_file}")
    errors = conn.errors
    if errors:
        rendered = "\n".join(str(error) for error in errors)
        raise RuntimeError(f"Beancount reported parsing errors:\n{rendered}")

    transactions_cur = conn.execute(
        """
        SELECT
          id,
          date,
          payee,
          narration,
          flag,
          tags
        FROM entries
        WHERE type = 'transaction'
        ORDER BY date, id
        """
    )
    transactions: list[dict] = []
    for tr_id, date_value, payee, narration, flag, tags in transactions_cur.fetchall():
        tag_values = sorted(tags or ())
        transactions.append(
            {
                "date": grist_date_value(date_value),
                "payee": payee or "",
                "narration": narration or "",
                "tags": grist_choice_list(tag_values),
                "flag": flag or "",
                "tr_id": str(tr_id),
            }
        )

    postings_cur = conn.execute(
        """
        SELECT
          id,
          grep("/[^/]+$", location) AS loc,
          account,
          any_meta('sub') AS sub,
          number,
          currency,
          NUMBER(CONVERT(COST(position), "USD", date)) AS usd
        FROM postings
        ORDER BY date, id, account, number
        """
    )
    postings = [
        {
            "transaction_id": str(tr_id),
            "loc": loc or "",
            "account": account,
            "sub": sub or "",
            "currency": currency or "",
            "number": posting_number(number) or "",
            "usd": posting_number(usd) or "",
        }
        for tr_id, loc, account, sub, number, currency, usd in postings_cur.fetchall()
    ]

    return ImportBundle(transactions=transactions, postings=postings)


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


def list_table_entries(api: GristApi) -> list[dict]:
    _status, tables = api.list_tables()
    return tables


def resolve_table_id(tables: list[dict], wanted: str) -> str | None:
    wanted_lower = wanted.casefold()
    for table in tables:
        if isinstance(table, str):
            if table.casefold() == wanted_lower:
                return table
            continue
        for key in ("id", "tableId", "name", "title"):
            value = table.get(key)
            if isinstance(value, str) and value.casefold() == wanted_lower:
                return str(value)
    return None


def ensure_tables(api: GristApi) -> TableIds:
    tables = list_table_entries(api)
    transactions_id = resolve_table_id(tables, TRANSACTIONS_TABLE)
    postings_id = resolve_table_id(tables, POSTINGS_TABLE)

    if transactions_id is None:
        created_ids = api.add_tables(
            [
                {
                    "id": TRANSACTIONS_TABLE,
                    "columns": [
                        {"id": "date", "type": "Date"},
                        {"id": "payee", "type": "Text"},
                        {"id": "narration", "type": "Text"},
                        {"id": "tags", "type": "ChoiceList"},
                        {"id": "flag", "type": "Text"},
                        {"id": "tr_id", "type": "Text"},
                    ],
                }
            ]
        )
        transactions_id = created_ids[1][0] if created_ids[1] else None
        if transactions_id is None:
            tables = list_table_entries(api)
            transactions_id = resolve_table_id(tables, TRANSACTIONS_TABLE)

    if transactions_id is None:
        raise RuntimeError("Could not find transactions table after creating it")

    if postings_id is None:
        created_ids = api.add_tables(
            [
                {
                    "id": POSTINGS_TABLE,
                    "columns": [
                        {
                            "id": "transaction_id",
                            "type": f"Ref:{transactions_id}",
                        },
                        {"id": "loc", "type": "Text"},
                        {"id": "account", "type": "Text"},
                        {"id": "sub", "type": "Choice"},
                        {"id": "currency", "type": "Text"},
                        {"id": "usd", "type": "Numeric"},
                        {"id": "number", "type": "Numeric"},
                    ],
                }
            ]
        )
        postings_id = created_ids[1][0] if created_ids[1] else None
        if postings_id is None:
            tables = list_table_entries(api)
            postings_id = resolve_table_id(tables, POSTINGS_TABLE)

    if postings_id is None:
        raise RuntimeError("Could not find postings table after creating it")

    ensure_columns(
        api,
        transactions_id,
        {
            "date": {"type": "Date", "label": "date"},
            "payee": {"type": "Text", "label": "payee"},
            "narration": {"type": "Text", "label": "narration"},
            "tags": {
                "type": "ChoiceList",
                "label": "tags",
                "widgetOptions": {"choices": []},
            },
            "flag": {"type": "Text", "label": "flag"},
            "tr_id": {"type": "Text", "label": "tr_id"},
        },
    )
    ensure_columns(
        api,
        postings_id,
        {
            "transaction_id": {
                "type": f"Ref:{transactions_id}",
                "label": "transaction id",
            },
            "loc": {"type": "Text", "label": "loc"},
            "account": {"type": "Text", "label": "account"},
            "sub": {
                "type": "Choice",
                "label": "sub",
                "widgetOptions": {"choices": []},
            },
            "currency": {"type": "Text", "label": "currency"},
            "usd": {
                "type": "Numeric",
                "label": "usd",
                "widgetOptions": {"numMode": "decimal", "decimals": 2},
            },
            "number": {
                "type": "Numeric",
                "label": "number",
                "widgetOptions": {"numMode": "decimal", "decimals": 2},
            },
        },
    )
    ensure_column_order(api, postings_id, POSTINGS_COLUMN_ORDER)
    ensure_view_section_field_order(api, postings_id, POSTINGS_COLUMN_ORDER)

    return TableIds(transactions=transactions_id, postings=postings_id)


def ensure_columns(api: GristApi, table_id: str, required: dict[str, dict]) -> None:
    _status, columns = api.list_cols(table_id)
    existing: dict[str, dict | str] = {}
    for column in columns:
        if isinstance(column, str):
            existing[column] = column
        elif isinstance(column, dict):
            column_id = column.get("id")
            if column_id:
                existing[str(column_id)] = column

    for column_id, fields in required.items():
        if column_id not in existing:
            call_column_api(
                api,
                table_id,
                "add",
                [{"id": column_id, "fields": column_api_fields(fields)}],
            )

    columns_to_update = []
    for column_id, wanted_fields in required.items():
        current = existing.get(column_id)
        if not isinstance(current, dict):
            continue

        current_fields = (
            current.get("fields", {}) if isinstance(current.get("fields"), dict) else {}
        )
        current_type = current_fields.get("type") or current.get("type")
        wanted_type = wanted_fields.get("type")
        patch_fields: dict[str, object] = {}
        if wanted_type and current_type != wanted_type:
            patch_fields["type"] = wanted_type

        for field_name in ("widgetOptions",):
            if (
                field_name in wanted_fields
                and current_fields.get(field_name) != wanted_fields[field_name]
            ):
                patch_fields[field_name] = wanted_fields[field_name]

        current_formula = current_fields.get("formula")
        current_is_formula = current_fields.get("isFormula", current.get("isFormula"))
        if current_is_formula or current_formula:
            patch_fields["formula"] = ""
            patch_fields["isFormula"] = False

        if patch_fields:
            columns_to_update.append({"id": column_id, "fields": patch_fields})

    for column_update in columns_to_update:
        call_column_api(api, table_id, "update", [column_update])


def ensure_column_order(
    api: GristApi, table_id: str, ordered_column_ids: tuple[str, ...]
) -> None:
    _status, columns = api.list_cols(table_id)
    existing: dict[str, dict] = {}
    for column in columns:
        if not isinstance(column, dict):
            continue
        column_id = column.get("id")
        if isinstance(column_id, str):
            existing[column_id] = column

    updates = []
    for index, column_id in enumerate(ordered_column_ids):
        column = existing.get(column_id)
        if column is None:
            continue
        current_fields = (
            column.get("fields", {}) if isinstance(column.get("fields"), dict) else {}
        )
        wanted_parent_pos = float(index + 1)
        if current_fields.get("parentPos") != wanted_parent_pos:
            updates.append({"id": column_id, "fields": {"parentPos": wanted_parent_pos}})

    for update in updates:
        call_column_api(api, table_id, "update", [update])


def ensure_view_section_field_order(
    api: GristApi, table_id: str, ordered_column_ids: tuple[str, ...]
) -> None:
    table_ref = find_grist_table_ref(api, table_id)
    if table_ref is None:
        return

    col_refs_by_id = find_grist_column_refs(api, table_ref)
    ordered_col_refs = [
        col_refs_by_id[column_id]
        for column_id in ordered_column_ids
        if column_id in col_refs_by_id
    ]
    if not ordered_col_refs:
        return

    sections = [
        record
        for record in list_hidden_records(api, GRIST_VIEW_SECTIONS_META)
        if normalize_ref(record.get("tableRef")) == table_ref
    ]
    if not sections:
        return

    section_ids = {record["id"] for record in sections}
    section_fields = [
        record
        for record in list_hidden_records(api, GRIST_SECTION_FIELDS_META)
        if normalize_ref(record.get("parentId")) in section_ids
    ]
    updates: list[dict] = []
    for section_id in section_ids:
        fields = [
            record
            for record in section_fields
            if normalize_ref(record.get("parentId")) == section_id
        ]
        if not fields:
            continue

        ordered_fields = sorted(fields, key=lambda record: record.get("parentPos", 0))
        known_by_col_ref = {
            normalize_ref(record.get("colRef")): record
            for record in ordered_fields
            if normalize_ref(record.get("colRef")) in ordered_col_refs
        }
        wanted_fields = [
            known_by_col_ref[col_ref]
            for col_ref in ordered_col_refs
            if col_ref in known_by_col_ref
        ]
        wanted_ids = {record["id"] for record in wanted_fields}
        wanted_fields.extend(
            record for record in ordered_fields if record["id"] not in wanted_ids
        )

        for index, record in enumerate(wanted_fields):
            wanted_parent_pos = float(index + 1)
            if record.get("parentPos") != wanted_parent_pos:
                updates.append({"id": record["id"], "parentPos": wanted_parent_pos})

    if updates:
        api.update_records(GRIST_SECTION_FIELDS_META, updates)


def find_grist_table_ref(api: GristApi, table_id: str) -> int | None:
    wanted = table_id.casefold()
    for record in list_hidden_records(api, GRIST_TABLES_META):
        current = record.get("tableId")
        if isinstance(current, str) and current.casefold() == wanted:
            return int(record["id"])
    return None


def find_grist_column_refs(api: GristApi, table_ref: int) -> dict[str, int]:
    refs: dict[str, int] = {}
    for record in list_hidden_records(api, GRIST_COLUMNS_META):
        if normalize_ref(record.get("parentId")) != table_ref:
            continue
        col_id = record.get("colId")
        if isinstance(col_id, str):
            refs[col_id] = int(record["id"])
    return refs


def list_hidden_records(api: GristApi, table_id: str) -> list[dict]:
    _status, records = api.list_records(table_id, hidden=True)
    return records


def normalize_ref(value) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, list) and len(value) == 2 and isinstance(value[1], int):
        return value[1]
    return None


def call_column_api(
    api: GristApi, table_id: str, operation: str, columns: list[dict]
) -> None:
    try:
        if operation == "add":
            api.add_cols(table_id, columns)
        elif operation == "update":
            api.update_cols(table_id, columns)
        else:
            raise ValueError(f"Unsupported column operation: {operation}")
    except HTTPError as exc:
        response = getattr(api.apicaller, "response", None)
        response_text = getattr(response, "text", "") if response is not None else ""
        column_ids = ", ".join(str(column.get("id")) for column in columns)
        raise RuntimeError(
            f"Could not {operation} Grist column(s) {column_ids} on {table_id}: "
            f"{exc}; response={response_text}; payload={columns!r}"
        ) from exc


def column_api_fields(fields: dict) -> dict:
    return {
        key: value
        for key, value in fields.items()
        if key in {"type", "widgetOptions", "formula", "isFormula", "parentPos"}
    }


def fetch_raw_records(api: GristApi, table_id: str) -> list[dict]:
    doc_id, server = api.configurator.select_params("", "")
    url = f"{server}/docs/{doc_id}/tables/{table_id}/records"
    _status, response = api.apicaller.apicall(url)
    return response["records"]


def clear_table(api: GristApi, table_id: str) -> None:
    rows = fetch_raw_records(api, table_id)
    row_ids = [row["id"] for row in rows]
    if row_ids:
        api.delete_rows(table_id, row_ids)


def batched(values: list[dict], size: int = RECORD_BATCH_SIZE):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def add_records_batched(
    api: GristApi, table_id: str, records: list[dict], noparse: bool = False
) -> list[int]:
    row_ids: list[int] = []
    for batch in batched(records):
        row_ids.extend(api.add_records(table_id, batch, noparse=noparse)[1])
    return row_ids


def load_transactions(
    api: GristApi, table_id: str, transactions: list[dict], replace: bool
) -> dict[str, int]:
    if replace:
        clear_table(api, table_id)

    existing_rows = fetch_raw_records(api, table_id)
    id_to_row_id = {
        row["fields"]["tr_id"]: row["id"]
        for row in existing_rows
        if isinstance(row.get("fields", {}).get("tr_id"), str)
    }
    new_transactions = [row for row in transactions if row["tr_id"] not in id_to_row_id]
    if new_transactions:
        new_row_ids = add_records_batched(
            api, table_id, new_transactions, noparse=True
        )
        for tx, row_id in zip(new_transactions, new_row_ids, strict=True):
            id_to_row_id[tx["tr_id"]] = row_id

    return id_to_row_id


def load_postings(
    api: GristApi,
    table_id: str,
    postings: list[dict],
    transaction_row_ids: dict[str, int],
    replace: bool,
) -> None:
    if replace:
        clear_table(api, table_id)

    existing_keys: set[tuple[int, str, str, str, str, str, str]] = set()
    if not replace:
        existing_rows = fetch_raw_records(api, table_id)
        existing_keys = {
            (
                row["fields"]["transaction_id"],
                row["fields"].get("loc", ""),
                row["fields"].get("account", ""),
                row["fields"].get("sub", ""),
                row["fields"].get("currency", ""),
                str(row["fields"].get("number", "")),
                str(row["fields"].get("usd", "")),
            )
            for row in existing_rows
            if isinstance(row.get("fields", {}).get("transaction_id"), int)
        }

    grist_postings = []
    for posting in postings:
        tx_id = posting["transaction_id"]
        row_id = transaction_row_ids.get(tx_id)
        if row_id is None:
            raise RuntimeError(f"Missing transaction row id for transaction {tx_id}")
        dedupe_key = (
            row_id,
            posting["loc"],
            posting["account"],
            posting["sub"],
            posting["currency"],
            str(posting["number"]),
            str(posting["usd"]),
        )
        if dedupe_key in existing_keys:
            continue
        grist_postings.append(
            {
                "transaction_id": row_id,
                "loc": posting["loc"],
                "account": posting["account"],
                "sub": posting["sub"],
                "currency": posting["currency"],
                "number": posting["number"],
                "usd": posting["usd"],
            }
        )
        existing_keys.add(dedupe_key)

    if grist_postings:
        add_records_batched(api, table_id, grist_postings)


def run_import(args: argparse.Namespace) -> int:
    bundle = transform_entries(args.bean_file)

    if args.dry_run:
        preview = {
            "transactions_count": len(bundle.transactions),
            "postings_count": len(bundle.postings),
            "transactions_preview": bundle.transactions[: args.preview],
            "postings_preview": bundle.postings[: args.preview],
        }
        print(json.dumps(preview, indent=2))
        return 0

    api = make_grist_api()
    table_ids = ensure_tables(api)
    transaction_row_ids = load_transactions(
        api, table_ids.transactions, bundle.transactions, replace=args.replace
    )
    load_postings(
        api,
        table_ids.postings,
        bundle.postings,
        transaction_row_ids,
        replace=args.replace,
    )

    print(
        json.dumps(
            {
                "transactions_uploaded": len(bundle.transactions),
                "postings_uploaded": len(bundle.postings),
                "replace": args.replace,
            },
            indent=2,
        )
    )
    return 0


def main() -> int:
    args = parse_args()
    try:
        return run_import(args)
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
