import json
import os
from urllib.parse import urlparse

import requests
from anomalo_api import AnomaloTableSummary

from adapters.base_adapter import AnomaloCatalogAdapter


class purview(AnomaloCatalogAdapter):
    def configure(self):
        print(f"Initializing {self.__class__.__name__} integration...")
        # For help creating an Entra Service Principal (aka Application) see: https://learn.microsoft.com/en-us/purview/tutorial-using-rest-apis
        self._ENTRA_TENANT_ID = os.environ.get("ENTRA_TENANT_ID", "")
        self._ENTRA_CLIENT_ID = os.environ.get("ENTRA_CLIENT_ID", "")
        self._ENTRA_CLIENT_SECRET = os.environ.get("ENTRA_CLIENT_SECRET", "")
        if (
            not self._ENTRA_TENANT_ID
            or not self._ENTRA_CLIENT_ID
            or not self._ENTRA_CLIENT_SECRET
        ):
            raise ValueError(
                "ENTRA_TENANT_ID, ENTRA_CLIENT_ID, and ENTRA_CLIENT_SECRET environment variables are required; define them and try again"
            )

        rooturl = os.environ.get("PURVIEW_ROOT_URL", "")
        if not rooturl:
            raise ValueError(
                "PURVIEW_ROOT_URL is required; define it in your environment and try again"
            )
        parsed_root = urlparse(rooturl)
        if not parsed_root.scheme or not parsed_root.netloc:
            self.purview_rooturl = "https://" + rooturl
        else:
            self.purview_rooturl = f"{parsed_root.scheme}://{parsed_root.netloc}"

        try:
            _login_url = f"https://login.microsoftonline.com/{self._ENTRA_TENANT_ID}/oauth2/token"
            _params = {
                "client_id": self._ENTRA_CLIENT_ID,
                "client_secret": self._ENTRA_CLIENT_SECRET,
                "grant_type": "client_credentials",
                "resource": "https://purview.azure.net",
            }
            _response = requests.post(_login_url, data=_params)
            _data = _response.json()
            _token = _data["access_token"]
            self.api_headers = {
                "Authorization": "Bearer " + _token,
                "Content-type": "application/json",
            }
        except Exception as e:
            raise ValueError(
                "Error getting Purview access token from Entra, please check your Entra config and credentials."
            ) from e

        self._register_purview_typedefs(self._args.force_update_typedefs)
        self.asset_list = self._get_purview_asset_list()

    def update_catalog_asset(
        self, warehouse: dict[str, str], table_summary: AnomaloTableSummary
    ) -> bool:
        """Update the Purview asset with Anomalo metadata."""
        p_uid = self._get_purview_uid(
            table_summary.table_full_name.split(".")[1], self.asset_list
        )
        if p_uid:
            print(
                f"FOUND table {table_summary.table_full_name} ({table_summary.table_id}) with Purview asset id {p_uid}; SYNCING..."
            )
            self._update_purview(p_uid, table_summary)
            return True
        else:
            print(
                f"WARNING cannot find a purview asset matching table {table_summary.table_full_name} ({table_summary.table_id})"
            )
            return False

    def _get_purview_asset_list(self):
        # TODO migrate this to GA api 2023-09-01
        _discovery_url = (
            f"{self.purview_rooturl}/catalog/api/browse?api-version=2023-02-01-preview"
        )
        _discovery_body = """{"entityType": "databricks_table"}"""

        response = requests.post(
            _discovery_url, data=_discovery_body, headers=self.api_headers
        )
        return response.json()

    def _get_purview_uid(self, anomalo_tablename, purview_list):
        for i in purview_list["value"]:
            if i["name"] == anomalo_tablename:
                return i["id"]
        return None

    def _update_purview(self, uid: str, summary: AnomaloTableSummary):
        """Publish DQ results to Purview for an asset"""
        # Tag table as being monitored by Anomalo
        if self._args.update_labels:
            labelpayload = json.dumps(
                summary.get_tags_to_apply() or ["ANOMALO_MONITORED"]
            )
            labelurl = (
                f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/guid/{uid}/labels"
            )
            response = requests.put(
                labelurl, data=labelpayload, headers=self.api_headers
            )

            # Remove labels that do not apply to this asset
            # https://learn.microsoft.com/en-us/rest/api/purview/datamapdataplane/entity/remove-labels
            del_labels = summary.get_tags_to_remove()
            if del_labels:
                dellabelpayload = json.dumps(del_labels)
                response = requests.delete(
                    labelurl, data=dellabelpayload, headers=self.api_headers
                )

        if self._args.update_endorsement:
            if summary.table_passed:
                # Certify asset passed all checks
                url = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/bulk/classification"
                body = json.dumps(
                    {
                        "classification": {
                            "typeName": "MICROSOFT.POWERBI.ENDORSEMENT",
                            "attributes": {
                                "endorsement": "Certified",
                                "certifiedBy": "Anomalo",
                            },
                        },
                        "entityGuids": [uid],
                    }
                )
                response = requests.post(url, data=body, headers=self.api_headers)
            else:
                # Remove certification if one or more checks failed
                url = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/guid/{uid}/classification/MICROSOFT.POWERBI.ENDORSEMENT"
                response = requests.delete(url, headers=self.api_headers)

        if self._args.update_aspect:
            # Write summary table to our metadata section
            url = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/guid/{uid}/businessmetadata"
            _profile_html = None
            if summary.table_profile_img:
                _profile_html = f"<img src='{summary.table_profile_img}' alt='Table column data visualization' width='auto' height='auto' />"
            _columns_html = None
            if summary.table_columns_img:
                _columns_html = f"<img src='{summary.table_columns_img}' alt='Table column data visualization' width='auto' height='auto' />"
            body = json.dumps(
                {
                    "AnomaloDQ": {
                        "AnomaloChecks": summary.get_status_text("purview"),
                        "AnomaloColumns": _columns_html,
                        "AnomaloProfile": _profile_html,
                    }
                }
            )
            response = requests.post(url, data=body, headers=self.api_headers)

    # API endpoints
    # listguid = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/bulk?guid=65646cd5-57fd-4238-82e1-d9f6f6f60000"
    # labelurl = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/guid/65646cd5-57fd-4238-82e1-d9f6f6f60000/labels"
    # certifyurl = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/bulk/classification"
    # delcertifyurl = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/guid/65646cd5-57fd-4238-82e1-d9f6f6f60000/classification/MICROSOFT.POWERBI.ENDORSEMENT"
    # attrgeturl = f"{self.purview_rooturl}/catalog/api/atlas/v2/types/businessmetadatadef/name/AnomaloGroup"
    # attrcreateurl = f"{self.purview_rooturl}/catalog/api/atlas/v2/types/typedefs"
    # setattrurl = f"{self.purview_rooturl}/catalog/api/atlas/v2/entity/guid/65646cd5-57fd-4238-82e1-d9f6f6f60000/businessmetadata"

    def _register_purview_typedefs(self, force_update: bool = False):
        """Creates Business Metadata `AnomaloDQ` containing a richtext section named `AnomaloChecks` in Purview"""
        ### https://purview.microsoft.com/datamap/governance/main/catalog/attributes

        url = f"{self.purview_rooturl}/catalog/api/atlas/v2/types/typedefs"
        body = json.dumps(
            {
                "businessMetadataDefs": [
                    {
                        "category": "BUSINESS_METADATA",
                        "createdBy": "Anomalo",
                        "version": 1,
                        "name": "AnomaloDQ",
                        "description": "Latest Anomalo data quality results",
                        "typeVersion": "1.1",
                        "attributeDefs": [
                            {
                                "name": "AnomaloChecks",
                                "typeName": "richtext",
                                "isOptional": True,
                                "cardinality": "SINGLE",
                                "valuesMinCount": 0,
                                "valuesMaxCount": 1,
                                "isUnique": False,
                                "isIndexable": True,
                                "includeInNotification": False,
                                "description": "Summary of data quality check results",
                                "options": {
                                    "applicableEntityTypes": '["databricks_catalog","databricks_schema","databricks_metastore","databricks_table","databricks_table_column","databricks_view","databricks_view_column"]',
                                    "isDisabled": "false",
                                },
                            },
                            {
                                "name": "AnomaloColumns",
                                "typeName": "richtext",
                                "isOptional": True,
                                "cardinality": "SINGLE",
                                "valuesMinCount": 0,
                                "valuesMaxCount": 1,
                                "isUnique": False,
                                "isIndexable": True,
                                "includeInNotification": False,
                                "description": "Visualization of column contents",
                                "options": {
                                    "applicableEntityTypes": '["databricks_catalog","databricks_schema","databricks_metastore","databricks_table","databricks_table_column","databricks_view","databricks_view_column"]',
                                    "isDisabled": "false",
                                },
                            },
                            {
                                "name": "AnomaloProfile",
                                "typeName": "richtext",
                                "isOptional": True,
                                "cardinality": "SINGLE",
                                "valuesMinCount": 0,
                                "valuesMaxCount": 1,
                                "isUnique": False,
                                "isIndexable": True,
                                "includeInNotification": False,
                                "description": "Data profile of columns",
                                "options": {
                                    "applicableEntityTypes": '["databricks_catalog","databricks_schema","databricks_metastore","databricks_table","databricks_table_column","databricks_view","databricks_view_column"]',
                                    "isDisabled": "false",
                                },
                            },
                        ],
                    }
                ]
            }
        )
        response = requests.post(url, data=body, headers=self.api_headers).json()
        if (
            response.get("errorMessage")
            and "already exists" not in response["errorMessage"]
        ):
            print(f"Error from {url}: {json.dumps(response)}")
            return False
        if (
            force_update
            and response.get("errorMessage")
            and "already exists" in response["errorMessage"]
        ):
            response = requests.put(url, data=body, headers=self.api_headers).json()
            print(f"Result from force-update of typedef: {response}")
        return True
