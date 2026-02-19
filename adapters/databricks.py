import os
import time

import requests
from anomalo_api import AnomaloTableSummary

from adapters.base_adapter import AnomaloCatalogAdapter


class databricks(AnomaloCatalogAdapter):
    def configure(self):
        super().configure()
        self._dbx_warehouse_id = self._get_or_throw("DATABRICKS_WAREHOUSE_UID")
        auth_method = os.environ.get("DATABRICKS_AUTH_METHOD", "token")

        if auth_method == "sdk":
            # Databricks SDK: auto-detects auth when running inside Databricks.
            # For external use, set DATABRICKS_HOST and DATABRICKS_TOKEN env vars.
            from databricks.sdk import WorkspaceClient
            self._workspace_client = WorkspaceClient()
            self._dbx_rooturl = None
            self._dbx_api_token = None
        elif auth_method == "token":
            # Explicit token: set DATABRICKS_HOSTNAME and DATABRICKS_ACCESS_TOKEN.
            hostname = self._get_or_throw("DATABRICKS_HOSTNAME")
            self._dbx_rooturl = hostname if hostname.startswith("https://") else "https://" + hostname
            self._dbx_api_token = self._get_or_throw("DATABRICKS_ACCESS_TOKEN")
            self._workspace_client = None
        else:
            raise ValueError(
                f"Unknown DATABRICKS_AUTH_METHOD '{auth_method}'. Supported: 'token', 'sdk'"
            )

    def _get_metastore_name(self, warehouse) -> str:
        if warehouse["warehouse_type"] != "databricks":
            return None

        dbx_name = warehouse["name"]
        if "-" in dbx_name:
            return dbx_name.split("-", 1)[1]
        if "_" in dbx_name:
            return dbx_name.split("_", 1)[1]

        print(
            f"Databricks data source must use naming convention `NICKNAME-CATALOG STORE NAME` or `NICKNAME_CATALOG STORE NAME` for integration to work"
        )
        return None

    def include_warehouse(self, warehouse) -> bool:
        return self._get_metastore_name(warehouse) != None

    def update_catalog_asset(
        self, warehouse: dict[str, str], table_summary: AnomaloTableSummary
    ) -> bool:
        metastore_name = self._get_metastore_name(warehouse)

        dbx_fqn = metastore_name + "." + table_summary.table_full_name
        print(f"  Updating asset: {dbx_fqn}")

        markdown = table_summary.get_status_text(dialect="markdown").strip()
        self._comment(dbx_fqn, markdown)

        tags_to_apply = table_summary.get_tags_to_apply()
        tags_to_remove = table_summary.get_tags_to_remove()
        self._set_tags(dbx_fqn, tags_to_apply)
        self._delete_tags(dbx_fqn, tags_to_remove)

        return True

    def _get_existing_comment(self, fqtable: str) -> str:
        if self._workspace_client:
            return self._workspace_client.tables.get(fqtable).comment or ""
        else:
            response = requests.get(
                self._dbx_rooturl + "/api/2.1/unity-catalog/tables/" + fqtable,
                headers={"Authorization": "Bearer " + self._dbx_api_token},
            )
            response.raise_for_status()
            return response.json().get("comment", "") or ""

    def _comment(self, fqtable: str, markdown: str):
        if self._args.overwrite_table_comment:
            sql = f"COMMENT ON TABLE {fqtable} IS '" + markdown.replace("'", "''") + "'"
            self._run_sql(sql)
            return

        ANOMALO_HEADER = "**Anomalo Data Quality Checks**"
        ANOMALO_SEPARATOR = "\n\n---\n\n"

        try:
            existing_comment = self._get_existing_comment(fqtable)
        except Exception as e:
            print(f"    WARNING: Could not fetch existing comment: {e}")
            existing_comment = ""

        if existing_comment.startswith(ANOMALO_HEADER):
            # Replace the existing Anomalo block, preserve anything after the separator
            if ANOMALO_SEPARATOR in existing_comment:
                user_content = existing_comment.split(ANOMALO_SEPARATOR, 1)[1]
                new_comment = markdown + ANOMALO_SEPARATOR + user_content
            else:
                new_comment = markdown
        elif existing_comment:
            new_comment = markdown + ANOMALO_SEPARATOR + existing_comment
        else:
            new_comment = markdown

        sql = f"COMMENT ON TABLE {fqtable} IS '" + new_comment.replace("'", "''") + "'"
        self._run_sql(sql)

    def _set_tags(self, fqtable: str, tags: list[str]):
        if not tags:
            return
        formatted_tags = ", ".join([f"'{t}' = 'y'" for t in tags])
        self._run_sql(f"ALTER TABLE {fqtable} SET TAGS ({formatted_tags})")

    def _delete_tags(self, fqtable: str, tags: list[str]):
        if not tags:
            return
        formatted_tags = ", ".join([f"'{t}'" for t in tags])
        self._run_sql(f"ALTER TABLE {fqtable} UNSET TAGS ({formatted_tags})")

    def _run_sql(self, sql: str):
        if self._workspace_client:
            return self._workspace_client.statement_execution.execute_statement(
                statement=sql,
                warehouse_id=self._dbx_warehouse_id,
                wait_timeout="30s",
            )
        else:
            payload = {
                "statement": sql,
                "wait_timeout": "5s",
                "warehouse_id": self._dbx_warehouse_id,
            }
            headers = {
                "Accept": "application/json",
                "Authorization": "Bearer " + self._dbx_api_token,
            }
            response = requests.post(
                self._dbx_rooturl + "/api/2.0/sql/statements/",
                json=payload,
                headers=headers,
            )
            response.raise_for_status()
            statement_id = response.json()["statement_id"]

            time.sleep(3)

            response = requests.get(
                self._dbx_rooturl + "/api/2.0/sql/statements/" + statement_id,
                headers=headers,
            )
            response.raise_for_status()
            return response.json()
