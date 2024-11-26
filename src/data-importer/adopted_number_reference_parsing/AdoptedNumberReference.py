from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.constants import SQL_FOLDER_NAME_COLUMN_NAME, SQL_BIBCODE_COLUMN_NAME, SQL_REF_STRING_COLUMN_NAME, \
    SQL_REF_STRING_SQUEEZED_COLUMN_NAME
import re

class AdoptedNumberReference(InsertLineBuilderBase):
    def __init__(self,
                 folder_name: str = None, # cluster name (abbreviation)
                 bibcode: str = None,
                 ref_string: str = None):
        self.folder_name = folder_name
        self.bibcode = bibcode
        self.ref_string = ref_string

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = self.__sanitize_string_value__(self.folder_name).lower()
        sanitized_bibcode = self.__sanitize_string_value__(self.bibcode)
        sanitized_ref_string = self.__sanitize_string_value__(self.ref_string)
        sanitized_ref_string_squeezed = re.sub(r"\s+", "", sanitized_ref_string)

        return f"({sanitized_folder_name}, {sanitized_bibcode}, {sanitized_ref_string}, {sanitized_ref_string_squeezed})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_FOLDER_NAME_COLUMN_NAME}, {SQL_BIBCODE_COLUMN_NAME}, {SQL_REF_STRING_COLUMN_NAME}, {SQL_REF_STRING_SQUEEZED_COLUMN_NAME}"