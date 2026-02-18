import os

from anomalo_api import AnomaloTableSummary


class AnomaloCatalogAdapter:
    @classmethod
    def adapters(clas):
        return clas.__subclasses__()

    def __init__(self, args):
        self._args = args

    def _get_or_throw(self, var_name: str) -> str:
        v = os.environ.get(var_name)
        if not v:
            raise ValueError(
                f"{var_name} is required; define it in your environment or a .env file and try again"
            )
        return v

    def configure(self):
        print(f"Initializing {self.__class__.__name__} integration...")

    def include_warehouse(self, warehouse) -> bool:
        return True

    def update_catalog_asset(
        self, warehouse: dict[str, str], table_summary: AnomaloTableSummary
    ) -> bool:
        raise NotImplementedError(
            f"{self.__class__.__name__} adapter is incomplete; it needs to override method `update_catalog_asset()`"
        )
