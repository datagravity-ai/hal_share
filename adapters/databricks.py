from anomalo_api import AnomaloTableSummary

from adapters.base_adapter import AnomaloCatalogAdapter


class databricks(AnomaloCatalogAdapter):
    def configure(self):
        super().configure()
        from databricks.sdk import WorkspaceClient
        self._dbx_warehouse_id = self._get_or_throw("DATABRICKS_WAREHOUSE_UID")
        # WorkspaceClient auto-detects auth when running inside Databricks.
        # For external use, set DATABRICKS_HOST and DATABRICKS_TOKEN env vars.
        self._workspace_client = WorkspaceClient()

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

        markdown = table_summary.get_status_text(dialect="markdown").strip()
        self._comment(dbx_fqn, markdown)

        self._set_tags(dbx_fqn, table_summary.get_tags_to_apply())
        self._delete_tags(dbx_fqn, table_summary.get_tags_to_remove())

        return True

    def _comment(self, fqtable: str, markdown: str):
        # TODO get existing table comment and replace only the Anomalo content
        #  See, e.g. update_bigquery_description code in dataplex
        self._run_sql(
            f"COMMENT ON TABLE {fqtable} IS '" + markdown.replace("'", "''") + "'"
        )

    def _set_tags(self, fqtable: str, tags: list[str]):
        formatted_tags = ", ".join([f"'{t}' = 'y'" for t in tags])
        self._run_sql(f"ALTER TABLE {fqtable} SET TAGS ({formatted_tags})")

    def _delete_tags(self, fqtable: str, tags: list[str]):
        formatted_tags = ", ".join([f"'{t}'" for t in tags])
        self._run_sql(f"ALTER TABLE {fqtable} UNSET TAGS ({formatted_tags})")

    def _run_sql(self, sql: str):
        return self._workspace_client.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=self._dbx_warehouse_id,
            wait_timeout="30s",
        )
