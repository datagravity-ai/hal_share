import json
import os

from anomalo_api import AnomaloTableSummary
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery, dataplex_v1
from google.protobuf.field_mask_pb2 import FieldMask
from google.protobuf.struct_pb2 import Struct

from adapters.base_adapter import AnomaloCatalogAdapter


DATAPLEX_ANOMALO_ASPECT_ID = "anomalo-dq-status"

GOOGLE_APPLICATION_CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    GOOGLE_APPLICATION_CREDENTIALS = "google-service-account-key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS


class dataplex(AnomaloCatalogAdapter):
    def configure(self):
        super().configure()
        self._gcp_user = None
        try:
            with open(GOOGLE_APPLICATION_CREDENTIALS) as fp:
                self._gcp_user = json.load(fp)["client_email"]
        except Exception as e:
            raise FileNotFoundError(
                f"ERROR loading Google Service Account key from `{GOOGLE_APPLICATION_CREDENTIALS}`:"
            ) from e

    def update_catalog_asset(
        self, warehouse: dict[str, str], table_summary: AnomaloTableSummary
    ) -> bool:
        client = bigquery.Client()

        project_id = warehouse.get("project_id")
        dataset_id = table_summary.table_full_name.split(".")[-2]
        table_id = table_summary.table_full_name.split(".")[-1]
        table_ref = (
            f"{project_id}.{dataset_id}.{table_id}"
            if project_id
            else f"{dataset_id}.{table_id}"
        )

        try:
            gcp_table = client.get_table(table_ref)
            if not gcp_table:
                raise Exception(f"Table `{table_ref}` not found")
        except:
            print(
                f"ERROR Cannot find table `{table_ref}` from data source `{warehouse['name']}` ({warehouse['id']})"
            )
            return False

        # Update BigQuery table description with plaintext DQ status
        if self._args.update_table_description:
            # Add a well-known footer to the status text block so we can non-destructively update the table description
            status_text = (
                table_summary.get_status_text(dialect="plaintext").strip() + "\n======"
            )
            if gcp_table.description:
                # Update description non-destructively
                desc = gcp_table.description
                anom_header = status_text.strip().split("\n")[0]
                anom_footer = status_text.strip().split("\n")[-1]
                # Replace existing Anomalo content if present
                if anom_header in desc:
                    prefix, suffix = desc.split(anom_header, 1)
                    old_desc, suffix = suffix.split(anom_footer, 1)
                    gcp_table.description = (
                        (prefix or "").rstrip()
                        + "\n"
                        + status_text
                        + "\n"
                        + (suffix or "").lstrip()
                    )
                else:
                    gcp_table.description = status_text
            else:
                gcp_table.description = status_text

        if self._args.update_labels:
            apply_labels = table_summary.get_tags_to_apply()
            remove_labels = table_summary.get_tags_to_remove()

            for t in apply_labels:
                gcp_table.labels[t.lower()] = (
                    "y"  # GCP only supports lower case letters
                )
            for t in remove_labels:
                if (
                    t.lower() in gcp_table.labels
                ):  # GCP only supports lower case letters
                    # See https://cloud.google.com/bigquery/docs/deleting-labels#python
                    gcp_table.labels[t.lower()] = None
        if self._args.update_labels or self._args.update_table_description:
            try:
                client.update_table(gcp_table, ["description", "labels"])
                print(
                    f"Updated `{gcp_table}` in data source `{warehouse['name']}` ({warehouse['id']})"
                )
            except BadRequest as e:
                print(
                    f"ERROR Update failed on `{table_ref}` in data source `{warehouse['name']}` ({warehouse['id']}): {e}"
                )
                return False
            except Exception as e:
                print("ERROR: Permission error")
                print(f"""{self._gcp_user or "The account you're using"} may be missing the `bigquery.tables.update` permission on some of your tables.

In Google Cloud IAM, grant `bigquery.tables.update` to this GCP user
using the BigQuery Data Editor role `roles/bigquery.dataEditor` or a custom role.""")
                return False

        if self._args.update_aspect:
            # Ensure aspect type exists in this table's project and location
            full_name = gcp_table.full_table_id
            project = gcp_table.project
            dataset_id = gcp_table.dataset_id

            cat_client = dataplex_v1.CatalogServiceClient()

            match_key = f"/{full_name.replace(':', '.').split('.')[-2]}/tables/{full_name.split('.')[-1]}".lower()

            print(
                f"Searching for {full_name.split('.', 1)[-1]} using matchkey '{match_key}'"
            )
            search_req = dataplex_v1.SearchEntriesRequest(
                name=f"projects/{project}/locations/global",
                query=full_name.split(".", 1)[-1],
            )
            search_res = cat_client.search_entries(request=search_req)

            found_entity = None
            for res in search_res:
                if res.linked_resource.lower().endswith(match_key):
                    found_entity = res.dataplex_entry
                    break

            print(
                f"Matched BigQuery asset {full_name} to DataPlex name {found_entity.name}"
            )
            if found_entity:
                aspect_parent_path = found_entity.name.split("/entryGroups")[0]
                aspect_type_path = (
                    f"{aspect_parent_path}/aspectTypes/{DATAPLEX_ANOMALO_ASPECT_ID}"
                )

                # Does the aspect type already exist?
                try:
                    aspect_type_res = cat_client.get_aspect_type(
                        request=dataplex_v1.GetAspectTypeRequest(name=aspect_type_path)
                    )
                except NotFound:
                    aspect_type_res = None

                if not aspect_type_res:
                    print(
                        f"Anomalo aspectType not found in Dataplex, attempting to create it..."
                    )

                    metadata_template = dataplex_v1.AspectType.MetadataTemplate()
                    metadata_template.type_ = "record"
                    metadata_template.name = "UserSchema"
                    metadata_template.record_fields.append(
                        dataplex_v1.types.AspectType.MetadataTemplate(
                            index=1,
                            name="anomalo-status",
                            type_="string",
                            annotations=dataplex_v1.types.AspectType.MetadataTemplate.Annotations(
                                string_type="richText",
                                display_name="DQ Status",
                                display_order=1,
                                description="Latest Data Quality status from Anomalo",
                            ),
                        )
                    )
                    aspect_type = dataplex_v1.AspectType()
                    aspect_type.display_name = "Anomalo"
                    aspect_type.description = "Anomalo Data Quality details"
                    aspect_type.metadata_template = metadata_template

                    aspect_request = dataplex_v1.CreateAspectTypeRequest(
                        parent=aspect_parent_path,
                        aspect_type_id=DATAPLEX_ANOMALO_ASPECT_ID,
                        aspect_type=aspect_type,
                    )

                    # create_aspect_type returns an Operation https://googleapis.dev/python/google-api-core/latest/operation.html
                    aspect_type_res = cat_client.create_aspect_type(
                        request=aspect_request
                    ).result()
                    print(f"Registered Anomalo aspectType: {aspect_type_res}")

                # FML :facepalm:
                # 400 error. Invalid map key projects/935953212207/locations/us/aspectTypes/anomalo-dq-status for the Aspects map. The proper format is "project.location.aspectType"
                dplx_path = aspect_parent_path.replace("projects/", "").replace(
                    "/locations/", "."
                )
                aspect_name = f"{dplx_path}.{DATAPLEX_ANOMALO_ASPECT_ID}"

                # Love me some manually-crafted protobuf
                aspect_data = Struct()
                aspect_data["anomalo-status"] = table_summary.get_status_text(
                    dialect="purview"
                )

                found_entity.aspects[aspect_name] = dataplex_v1.types.Aspect(
                    data=aspect_data,
                )

                update_request = dataplex_v1.UpdateEntryRequest(
                    entry=found_entity, update_mask=FieldMask(paths=["aspects"])
                )
                update_res = cat_client.update_entry(request=update_request)
                print(f"Update entry.aspects[{aspect_name}] on {found_entity.name}")
            else:
                print(
                    f"WARNING Cannot find Dataplex entry for {full_name}, will not update Dataplex status"
                )

        return True
