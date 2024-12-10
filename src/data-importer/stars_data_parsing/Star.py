from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.Sanitizer import Sanitizer
from common.constants import SQL_FOLDER_NAME_COLUMN_NAME, SQL_ADOPTED_NUMBER_COLUMN_NAME


class Star(InsertLineBuilderBase):
    def __init__(
            self,
            folder_name: str = None,
            adopted_number: str = None):
        self.folder_name = folder_name  # cluster name (abbreviation)
        self.adopted_number = adopted_number

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = Sanitizer.__sanitize_string_value__(self.folder_name)
        sanitized_adopted_number = Sanitizer.__sanitize_adopted_number__(self.adopted_number)

        return  f"({sanitized_folder_name}, {sanitized_adopted_number})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_FOLDER_NAME_COLUMN_NAME}, {SQL_ADOPTED_NUMBER_COLUMN_NAME}"