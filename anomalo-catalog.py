try:
    import argparse
    import os
    import sys

    import dotenv

    if dotenv.load_dotenv(".env", verbose=True):
        print("Loaded environment variables from `.env`")

    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        sys.path.insert(0, os.getcwd())

    from anomalo_api import AnomaloClient
except Exception as x:
    raise Exception(
        "Please install required packages with `pip install -r requirements.txt`"
    ) from x

import traceback
from typing import Sequence

from adapters.base_adapter import AnomaloCatalogAdapter


AVAILABLE_ADAPTERS = {a.__name__: a for a in AnomaloCatalogAdapter.adapters()}


def get_arg_parser():
    parser = argparse.ArgumentParser(
        description="Sync Anomalo check metadata with your data catalog."
    )

    parser.add_argument(
        "--catalogs",
        action="store_true",
        dest="list_catalogs",
        help="List available catalog integrations",
    )

    parser.add_argument(
        "--catalog", type=str, choices=AVAILABLE_ADAPTERS.keys(), help="Catalog type"
    )

    parser.add_argument(
        "--list-anomalo-organizations",
        action="store_true",
        dest="list_orgs",
        help="List available Anomalo organizations",
    )
    parser.add_argument(
        "--anomalo-organization-id",
        type=int,
        default=None,
        dest="anomalo_organization_id",
        help="Anomalo organization ID (default: use last organization accessed by API key's user)",
    )

    parser.add_argument(
        "--warehouse-name",
        type=str,
        default=None,
        dest="warehouse_name",
        help="Only sync tables from the named Anomalo data source (aka warehouse)",
    )
    parser.add_argument(
        "--warehouse-id",
        type=int,
        default=None,
        dest="warehouse_id",
        help="Only sync tables from the Anomalo data source (aka warehouse) with this id",
    )

    parser.add_argument(
        "--update-table-description",
        action="store_true",
        dest="update_table_description",
        help="Update the table's description field with Anomalo metadata (default: disabled)",
    )
    parser.add_argument(
        "--no-update-labels",  # Inverse name for disabling the flag
        action="store_false",
        dest="update_labels",
        help="Disable applying labels to monitored assets in the catalog (default: enabled)",
    )
    parser.add_argument(
        "--no-update-aspect",  # Inverse name for disabling the flag
        action="store_false",
        dest="update_aspect",
        help="Disable updating the Anomalo custom Aspect in the catalog (default: enabled)",
    )
    parser.add_argument(
        "--no-update-endorsement",  # Inverse name for disabling the flag
        action="store_false",
        dest="update_endorsement",
        help="Disable applying endorsement to monitored assets in the catalog (default: enabled)",
    )
    parser.add_argument(
        "--force-update-typedefs",
        action="store_true",
        dest="force_update_typedefs",
        help="Force re-registration of catalog metadata type definitions (default: disabled)",
    )

    return parser


def main(cli_args: Sequence[str] = None):
    args = get_arg_parser().parse_args(cli_args)

    client = AnomaloClient(args.anomalo_organization_id)

    if args.list_orgs:
        print("Available Anomalo organizations:")
        for org in client.api_client.get_all_organizations():
            print(f"  {org['id']:>4}: {org['name']}")
        exit(0)

    if args.list_catalogs:
        print(f"Available catalogs: {', '.join(AVAILABLE_ADAPTERS.keys())}")
        exit(0)

    if not args.catalog:
        print(
            "--catalog <catalog_name> argument required; use --catalogs to list available options"
        )
        exit(3)

    adapter = AVAILABLE_ADAPTERS[args.catalog](args)
    adapter.configure()

    print(
        f"Reading warehouse list from Anomalo deployment HOST={client.api_client.host} ORGANIZATION_ID={client.organization_id} ..."
    )
    warehouses = client.get_warehouses()["warehouses"]

    updated_table_count = 0
    error_table_count = 0
    for wh in warehouses:
        if args.warehouse_name and wh["name"] != args.warehouse_name:
            continue
        if args.warehouse_id and wh["id"] != args.warehouse_id:
            continue
        if not adapter.include_warehouse(wh):
            print(f"Skipping unsupported data source `{wh['name']}` ({wh['id']})...")
            continue

        print(
            f"Processing configured tables in data source `{wh['name']}` ({wh['id']})..."
        )
        configured_tables = client.get_configured_tables(warehouse_id=wh["id"])
        print(
            f"Publishing DQ status to {len(configured_tables)} configured tables in data source `{wh['name']}` ({wh['id']})..."
        )
        for t in configured_tables:
            table_summary = client.get_table_summary(t)
            try:
                if adapter.update_catalog_asset(wh, table_summary):
                    updated_table_count += 1
                else:
                    error_table_count += 1
            except Exception as e:
                print(traceback.format_exc())
                error_table_count += 1

    print(
        f"\n\nFINISHED SYNC. Updated {updated_table_count} tables, failed to sync {error_table_count} tables.\n"
    )


if __name__ == "__main__":
    main()
