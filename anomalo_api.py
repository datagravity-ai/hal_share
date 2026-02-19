from datetime import date, timedelta

import anomalo


ANOMALO_ASSET_TAGS = [
    "ANOMALO_MONITORED",
    "ANOMALO_DQ_FAILED",
    "ANOMALO_DQ_PASSED",
    # "ANOMALO_DATA_FRESHNESS_PASSED", "ANOMALO_DATA_FRESHNESS_FAILED",
    # "ANOMALO_DATA_VOLUME_PASSED", "ANOMALO_DATA_VOLUME_FAILED",
    # "ANOMALO_METRIC_PASSED", "ANOMALO_METRIC_FAILED",
    # "ANOMALO_RULE_PASSED", "ANOMALO_RULE_FAILED",
    # "ANOMALO_MISSING_DATA_PASSED", "ANOMALO_MISSING_DATA_FAILED",
    # "ANOMALO_ANOMALY_PASSED", "ANOMALO_ANOMALY_FAILED",
]


class AnomaloClient:
    def __init__(self, organization_id=None):
        """Set global configuration for Anomalo API access."""
        self.api_client = anomalo.Client()
        if organization_id:
            self.api_client.set_active_organization_id(organization_id)
        check_res = self.api_client.ping()
        if not check_res.get("ping", False):
            raise Exception(
                "Anomalo API is not reachable. Please check your configuration."
            )
        self.organization_id = self.api_client.get_active_organization_id()

    def get_warehouses(self):
        """Get a list of the configured warehouses in the current Anomalo organization."""
        return self.api_client.list_warehouses()

    def get_configured_tables(self, warehouse_id=None):
        """Get a list of the configured tables in the current Anomalo organization, optionally filtered to a single warehouse."""
        config_tables = self.api_client.configured_tables(warehouse_id=warehouse_id)
        return config_tables

    def get_table_summary(self, table, warehouse_id=None):
        """Get an AnomaloTableSummary containing statistics and status for a table."""
        return AnomaloTableSummary(self.api_client, table, warehouse_id=warehouse_id)


class AnomaloCheckResult:
    def __init__(self, name, total, passed, failed, pending=False):
        self.name = name
        self.total = total
        self.passed = passed
        self.failed = failed
        self.pending = pending

    def __repr__(self) -> str:
        if self.pending:
            return f"{self.name}: 🕑"
        icon = "❌" if self.failed > 0 else "✅" if self.passed == self.total else "🕑"
        return f"{self.name}: {self.passed}/{self.total} passed {icon}"


class AnomaloTableSummary:
    def __init__(self, api_client, table, warehouse_id=None):
        """Finds the most recent (as of yesterday) check job run for the table and computes DQ summary statistics for that job run"""
        self.api_client = api_client

        self.table = table
        self.table_id = table["table"]["id"]
        self.table_full_name = table["table"]["full_name"]

        self.table_passed = False
        self.to_checks_failed = False
        self.dq_checks_failed = False

        self.data_freshness_total = 0
        self.data_freshness_pass = 0
        self.data_freshness_fail = 0
        self.data_volume_total = 0
        self.data_volume_pass = 0
        self.data_volume_fail = 0
        self.missing_data_total = 0
        self.missing_data_pass = 0
        self.missing_data_fail = 0
        self.anomaly_total = 0
        self.anomaly_pass = 0
        self.anomaly_fail = 0
        self.metric_total = 0
        self.metric_pass = 0
        self.metric_fail = 0
        self.rule_total = 0
        self.rule_pass = 0
        self.rule_fail = 0

        self.table_profile_img = None
        self.table_columns_img = None
        if warehouse_id:
            try:
                profile_resp = self.api_client.get_table_profile(
                    warehouse_id=warehouse_id, table_id=self.table_id
                )
                self.table_profile_img = profile_resp.get("profile", {}).get("img_url")
                self.table_columns_img = profile_resp.get("columns", {}).get("img_url")
            except anomalo.result.BadRequestException as e:
                full_name = self.table["table"]["full_name"]
                print(f"WARNING cannot fetch table profile for {full_name}: {e}")

        self.job_date = (date.today() - timedelta(1)).strftime("%Y-%m-%d")

        res = self.api_client.get_check_intervals(
            table_id=self.table_id, start=self.job_date, end=None
        )
        if res and len(res):
            self.job_id = res[0]["latest_run_checks_job_id"]
            results = self.api_client.get_run_result(job_id=self.job_id)
        else:
            results = {}

        # ANOMALO_FQN = table['table']['full_name']
        # ANOMALO_TABLE_ID = table['table']['id']
        # ANOMALO_WH_ID = table["table"]["warehouse_id"]

        # calculate DQ summary statistics
        for r in results.get("check_runs", []):
            # TO check types
            if r["run_config"]["_metadata"]["check_type"] == "data_freshness":
                self.data_freshness_total += 1
                if r["results"]["success"] == False:
                    self.data_freshness_fail += 1
                    self.to_checks_failed = True
                elif r["results"]["success"] == True:
                    self.data_freshness_pass += 1
            if r["run_config"]["_metadata"]["check_type"] == "data_volume":
                self.data_volume_total += 1
                if r["results"]["success"] == False:
                    self.data_volume_fail += 1
                    self.to_checks_failed = True
                elif r["results"]["success"] == True:
                    self.data_volume_pass += 1
            # DQ check types
            if r["run_config"]["_metadata"]["check_type"] == "missing_data":
                self.missing_data_total += 1
                if r["results"]["success"] == False:
                    self.missing_data_fail += 1
                    self.dq_checks_failed = True
                elif r["results"]["success"] == True:
                    self.missing_data_pass += 1
            if r["run_config"]["_metadata"]["check_type"] == "anomaly":
                self.anomaly_total += 1
                if r["results"]["success"] == False:
                    self.anomaly_fail += 1
                    self.dq_checks_failed = True
                elif r["results"]["success"] == True:
                    self.anomaly_pass += 1
            if r["run_config"]["_metadata"]["check_type"] == "metric":
                self.metric_total += 1
                if r["results"]["success"] == False:
                    self.metric_fail += 1
                    self.dq_checks_failed = True
                elif r["results"]["success"] == True:
                    self.metric_pass += 1
            if r["run_config"]["_metadata"]["check_type"] == "rule":
                self.rule_total += 1
                if r["results"]["success"] == False:
                    self.rule_fail += 1
                    self.dq_checks_failed = True
                elif r["results"]["success"] == True:
                    self.rule_pass += 1

        self.table_passed = (
            self.data_freshness_total == self.data_freshness_pass
            and self.data_volume_total == self.data_volume_pass
            and self.missing_data_total == self.missing_data_pass
            and self.anomaly_total == self.anomaly_pass
            and self.metric_total == self.metric_pass
            and self.rule_total == self.rule_pass
        )

        org_id = self.api_client.get_active_organization_id()
        self.anomalo_table_url = f"{self.api_client.proto}://{self.api_client.host}/dashboard/orgs/{org_id}/tables/{str(self.table_id)}"

        # pre-generate summary statistic descriptions
        self.results = [
            AnomaloCheckResult(
                "Data Freshness",
                self.data_freshness_total,
                self.data_freshness_pass,
                self.data_freshness_fail,
                False,
            ),
            AnomaloCheckResult(
                "Data Volume",
                self.data_volume_total,
                self.data_volume_pass,
                self.data_volume_fail,
                self.data_freshness_pass == 0,
            ),
            AnomaloCheckResult(
                "Missing Data",
                self.missing_data_total,
                self.missing_data_pass,
                self.missing_data_fail,
                self.to_checks_failed,
            ),
            AnomaloCheckResult(
                "Table Anomalies",
                self.anomaly_total,
                self.anomaly_pass,
                self.anomaly_fail,
                self.to_checks_failed,
            ),
            AnomaloCheckResult(
                "Key Metrics",
                self.metric_total,
                self.metric_pass,
                self.metric_fail,
                self.to_checks_failed,
            ),
            AnomaloCheckResult(
                "Validation Rules",
                self.rule_total,
                self.rule_pass,
                self.rule_fail,
                self.to_checks_failed,
            ),
        ]
        self.summaries = [str(r) for r in self.results]

    def update_anomalo_definition(self, definition):
        """Update the definition string for the table in Anomalo"""
        resp = self.api_client.update_table_configuration(
            table_id=self.table["table"]["id"], definition=definition
        )

    def get_tags_to_apply(self):
        """Return a list of tags to apply to the asset in the data catalog based on latest DQ results"""
        tags = ["ANOMALO_MONITORED"]
        if (
            self.data_freshness_fail
            or self.data_volume_fail
            or self.missing_data_fail
            or self.anomaly_fail
            or self.metric_fail
            or self.rule_fail
        ):
            tags.append("ANOMALO_DQ_CHECKS_FAILED")
        else:
            tags.append("ANOMALO_DQ_CHECKS_PASSED")

        # These highly granular tags are not necessary for most use cases
        # They are filtered out below where we only return tags that are defined in the ANOMALO_ASSET_TAGS list
        # To include some or all of these, uncomment the tags in the definition of ANOMALO_ASSET_tAGS at the top of the file
        if self.data_freshness_pass > 0 and self.data_freshness_fail == 0:
            tags.append("ANOMALO_DATA_FRESHNESS_CHECKS_PASSED")
            if self.data_volume_pass > 0 and self.data_volume_fail == 0:
                tags.append("ANOMALO_DATA_VOLUME_CHECKS_PASSED")
            elif self.data_volume_fail > 0:
                tags.append("ANOMALO_DATA_VOLUME_CHECKS_FAILED")
        elif self.data_freshness_fail > 0:
            tags.append("ANOMALO_DATA_FRESHNESS_CHECKS_FAILED")

        if not self.to_checks_failed:
            if self.missing_data_pass > 0 and self.missing_data_fail == 0:
                tags.append("ANOMALO_MISSING_DATA_CHECKS_PASSED")
            elif self.missing_data_fail > 0:
                tags.append("ANOMALO_MISSING_DATA_CHECKS_FAILED")
            if self.anomaly_pass > 0 and self.anomaly_fail == 0:
                tags.append("ANOMALO_ANOMALY_CHECKS_PASSED")
            elif self.anomaly_fail > 0:
                tags.append("ANOMALO_ANOMALY_CHECKS_FAILED")
            if self.metric_pass > 0 and self.metric_fail == 0:
                tags.append("ANOMALO_METRIC_CHECKS_PASSED")
            elif self.metric_fail > 0:
                tags.append("ANOMALO_METRIC_CHECKS_FAILED")
            if self.rule_pass > 0 and self.rule_fail == 0:
                tags.append("ANOMALO_RULE_CHECKS_PASSED")
            elif self.rule_fail > 0:
                tags.append("ANOMALO_RULE_CHECKS_FAILED")

        return [t for t in tags if t in ANOMALO_ASSET_TAGS]

    def get_tags_to_remove(self):
        """Return a list of tags that should be removed from the asset in the data catalog because they do not reflect the latest DQ results"""
        global ANOMALO_ASSET_TAGS
        applied_tags = self.get_tags_to_apply()
        return [t for t in ANOMALO_ASSET_TAGS if t not in applied_tags]

    def get_status_text(self, dialect="plaintext") -> str:
        """
        Return a summary of the DQ status of the table in one of the supported formatting 'dialects'.
        Supported dialects:
            - 'plaintext' (default): simple text summary
            - 'markdown': Markdown formatted summary
            - 'purview': HTML formatted summary using Purview-friendly markup
            - 'html': HTML formatted summary for generic HTML rendering

        Args:
            dialect: 'plaintext' (default), 'markdown', 'purview', 'html'
        """
        if dialect == "markdown":
            return self._get_status_markdown()
        if dialect == "purview":
            return self._get_status_purview()
        if dialect == "html":
            return self._get_status_html()

        summaries = "\n".join(["    * " + s for s in self.summaries])
        return f"""Anomalo Data Quality Checks
    {self.anomalo_table_url}
{summaries}
======
"""

    def _get_status_html(self) -> str:
        """Return an HTML string summarizing the DQ status of the table"""

        summaries = "\n".join(
            [
                f'    <li class="editor-listitem" dir="ltr" value="{idx + 1}"><span style="display: block;white-space: pre-wrap;">{s}</span></li>'
                for idx, s in enumerate(self.summaries)
            ]
        )

        return f"""<!-- begin anomalo table summary -->
<p class="editor-paragraph" dir="ltr">
    <a class="editor-link" href="{self.anomalo_table_url}" rel="noopener noreferrer" target="_blank">
        <span style="white-space: pre-wrap;">{self.anomalo_table_url}</span>
    </a>
</p>
<ul class="editor-list-ul">
{summaries}
</ul><!-- end anomalo table summary -->
    """

    def _get_status_markdown(self) -> str:
        """Return a Markdown string summarizing the DQ status of the table"""

        summaries = "\n".join([f"* {s}" for s in self.summaries])

        return f"""**Anomalo Data Quality Checks**
    [View table in Anomalo]({self.anomalo_table_url})

{summaries}
    """

    def _get_status_purview(self) -> str:
        """Return an HTML string summarizing the DQ status of the table using Purview-compatible markup"""

        summaries = "\n".join(
            [
                f"""
            <tr>
                <td style="width:100px; border-width:1px; border-style:solid; border-color:rgb(171, 171, 171); background-color:transparent;">{r.name}</td>
                <td style="width:100px; border-width:1px; border-style:solid; border-color:rgb(171, 171, 171); background-color:transparent;">{r.passed}{" ✅" if r.passed > 0 and r.failed == 0 else ""}</td>
                </td><td style="width:100px; border-width:1px; border-style:solid; border-color:rgb(171, 171, 171); background-color:transparent;">{r.failed}{" ❌" if r.failed > 0 else ""}</td>
            </tr>"""
                for r in self.results
            ]
        ).strip()

        return f"""<!-- begin anomalo table summary -->
            <div><span><a href="{self.anomalo_table_url}">{self.anomalo_table_url}</a><br></span></div>
            <div><br></div>
            <div><table style="border-collapse:collapse;"><tbody>
                <tr><td style="width:100px; border-width:1px; border-style:solid; border-color:rgb(171, 171, 171); background-color:transparent;">Check</td><td style="width:100px; border-width:1px; border-style:solid; border-color:rgb(171, 171, 171); background-color:transparent;">Pass</td><td style="width:100px; border-width:1px; border-style:solid; border-color:rgb(171, 171, 171); background-color:transparent;">Fail</td></tr>
                {summaries}
            </tbody></table></div>
            </ul><!-- end anomalo table summary -->
                """
