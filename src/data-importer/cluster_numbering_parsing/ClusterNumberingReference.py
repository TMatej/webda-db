from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.Sanitizer import Sanitizer
from common.constants import SQL_FOLDER_NAME_COLUMN_NAME, SQL_BIBCODE_COLUMN_NAME, SQL_REF_STRING_COLUMN_NAME, \
    SQL_CLUSTER_ID_COLUMN_NAME


class ClusterNumberingReference(InsertLineBuilderBase):
    def __init__(self,
                 folder_name: str = None, # cluster name (abbreviation)
                 bibcode: str = None,
                 ref_string: str = None):
        self.folder_name = folder_name
        self.bibcode = bibcode
        self.ref_string = ref_string

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = Sanitizer.__sanitize_string_value__(self.folder_name).lower()
        sanitized_bibcode = Sanitizer.__sanitize_string_value__(self.bibcode)
        sanitized_ref_string = Sanitizer.__sanitize_string_value__(self.ref_string)

        return f"(0, {sanitized_folder_name}, {sanitized_bibcode}, {sanitized_ref_string})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_CLUSTER_ID_COLUMN_NAME}, {SQL_FOLDER_NAME_COLUMN_NAME}, {SQL_BIBCODE_COLUMN_NAME}, {SQL_REF_STRING_COLUMN_NAME}"