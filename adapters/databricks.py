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
        print(f"  Updating asset: {dbx_fqn}")

        markdown = table_summary.get_status_text(dialect="markdown").strip()
        self._comment(dbx_fqn, markdown)

        tags_to_apply = table_summary.get_tags_to_apply()
        tags_to_remove = table_summary.get_tags_to_remove()
        print(f"    Tags to apply:  {tags_to_apply}")
        print(f"    Tags to remove: {tags_to_remove}")
        self._set_tags(dbx_fqn, tags_to_apply)
        self._delete_tags(dbx_fqn, tags_to_remove)

        return True

    def _comment(self, fqtable: str, markdown: str):
        ANOMALO_HEADER = "**Anomalo Data Quality Checks**"
        ANOMALO_SEPARATOR = "\n\n---\n\n"

        try:
            existing_comment = self._workspace_client.tables.get(fqtable).comment or ""
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
        print(f"    SQL: {sql}")
        result = self._run_sql(sql)
        print(f"    Comment result: {result.status}")

    def _set_tags(self, fqtable: str, tags: list[str]):
        if not tags:
            print(f"    No tags to set, skipping")
            return
        formatted_tags = ", ".join([f"'{t}' = 'y'" for t in tags])
        sql = f"ALTER TABLE {fqtable} SET TAGS ({formatted_tags})"
        print(f"    SQL: {sql}")
        result = self._run_sql(sql)
        print(f"    Set tags result: {result.status}")

    def _delete_tags(self, fqtable: str, tags: list[str]):
        if not tags:
            print(f"    No tags to delete, skipping")
            return
        formatted_tags = ", ".join([f"'{t}'" for t in tags])
        sql = f"ALTER TABLE {fqtable} UNSET TAGS ({formatted_tags})"
        print(f"    SQL: {sql}")
        result = self._run_sql(sql)
        print(f"    Delete tags result: {result.status}")

    def _run_sql(self, sql: str):
        return self._workspace_client.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=self._dbx_warehouse_id,
            wait_timeout="30s",
        )
