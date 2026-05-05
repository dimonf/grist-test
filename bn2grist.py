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


TRANSACTIONS_TABLE = "transactions"
POSTINGS_TABLE = "postings"


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
          account,
          number,
          currency
        FROM postings
        ORDER BY date, id, account, number
        """
    )
    postings = [
        {
            "transaction_id": str(tr_id),
            "account": account,
            "currency": currency or "",
            "number": posting_number(number) or "",
        }
        for tr_id, account, number, currency in postings_cur.fetchall()
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
                        {"id": "account", "type": "Text"},
                        {"id": "currency", "type": "Text"},
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
            "tags": {"type": "ChoiceList", "label": "tags", "widgetOptions": {"choices": []}},
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
            "account": {"type": "Text", "label": "account"},
            "currency": {"type": "Text", "label": "currency"},
            "number": {"type": "Numeric", "label": "number"},
        },
    )

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

    missing_columns = [
        {"id": column_id, "fields": fields}
        for column_id, fields in required.items()
        if column_id not in existing
    ]
    if missing_columns:
        api.add_cols(table_id, missing_columns)

    columns_to_update = []
    for column_id, wanted_fields in required.items():
        current = existing.get(column_id)
        if not isinstance(current, dict):
            continue

        current_fields = current.get("fields", {}) if isinstance(current.get("fields"), dict) else {}
        current_type = current_fields.get("type") or current.get("type")
        wanted_type = wanted_fields.get("type")
        patch_fields: dict[str, object] = {}
        if wanted_type and current_type != wanted_type:
            patch_fields["type"] = wanted_type

        current_formula = current_fields.get("formula")
        current_is_formula = current_fields.get("isFormula", current.get("isFormula"))
        if current_is_formula or current_formula:
            patch_fields["formula"] = ""
            patch_fields["isFormula"] = False

        if patch_fields:
            columns_to_update.append({"id": column_id, "fields": patch_fields})

    if columns_to_update:
        api.update_cols(table_id, columns_to_update)


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
        new_row_ids = api.add_records(table_id, new_transactions, noparse=True)[1]
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

    existing_keys: set[tuple[int, str, str, str]] = set()
    if not replace:
        existing_rows = fetch_raw_records(api, table_id)
        existing_keys = {
            (
                row["fields"]["transaction_id"],
                row["fields"].get("account", ""),
                row["fields"].get("currency", ""),
                str(row["fields"].get("number", "")),
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
            posting["account"],
            posting["currency"],
            str(posting["number"]),
        )
        if dedupe_key in existing_keys:
            continue
        grist_postings.append(
            {
                "transaction_id": row_id,
                "account": posting["account"],
                "currency": posting["currency"],
                "number": posting["number"],
            }
        )
        existing_keys.add(dedupe_key)

    if grist_postings:
        api.add_records(table_id, grist_postings)


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
