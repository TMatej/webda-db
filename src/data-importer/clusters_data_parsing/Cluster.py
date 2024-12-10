from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.Sanitizer import Sanitizer
from common.constants import SQL_FOLDER_NAME_COLUMN_NAME, SQL_NAME_COLUMN_NAME, SQL_IAU_CLUSTER_NUMBER_COLUMN_NAME


class Cluster(InsertLineBuilderBase):
    def __init__(self,
                 iau_cluster_number = None,
                 folder_name = None,            # cluster name (abbreviation)
                 name = None):
        self.iau_cluster_number = iau_cluster_number
        self.folder_name = folder_name
        self.name = name

    def is_empty(self):
        return (self.iau_cluster_number is None and
                self.folder_name is None and
                self.name is None)

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = Sanitizer.__sanitize_string_value__(self.folder_name)
        sanitized_iau_cluster_number = Sanitizer.__sanitize_string_value__(self.iau_cluster_number)
        sanitized_name = Sanitizer.__sanitize_string_value__(self.name)

        return f"({sanitized_folder_name}, {sanitized_iau_cluster_number}, {sanitized_name})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_FOLDER_NAME_COLUMN_NAME}, {SQL_IAU_CLUSTER_NUMBER_COLUMN_NAME}, {SQL_NAME_COLUMN_NAME}"