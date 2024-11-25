from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.constants import SQL_FILE_NAME_COLUMN_NAME, SQL_ABBREVIATION_COLUMN_NAME, SQL_REF_FILE_NAME_COLUMN_NAME, \
    SQL_SHORT_DESCRIPTION_COLUMN_NAME, SQL_LONG_DESCRIPTION_COLUMN_NAME


class DataType(InsertLineBuilderBase):
    def __init__(self,
                 abbreviation: str = None,
                 file_name: str = None, # data type name
                 ref: str = None,
                 header: str = None,
                 format: str = None,
                 fttbl: str = None,
                 cols: str = None,
                 under: str = None,
                 long_description: str = None,
                 short_description: str = None):
        self.abbreviation = abbreviation
        self.file_name = file_name
        self.ref = ref
        self.header = header
        self.format = format
        self.fttbl = fttbl
        self.cols = cols
        self.under = under
        self.long_description = long_description
        self.short_description = short_description

    def build_insert_values_line(self) -> str:
        sanitized_file_name = self.__sanitize_string_value__(self.file_name).lower()
        sanitized_abbreviation = self.__sanitize_string_value__(self.abbreviation)
        sanitized_ref_file_name = self.__sanitize_string_value__(self.ref)
        sanitized_short_description = self.__sanitize_string_value__(self.short_description)
        sanitized_long_description = self.__sanitize_string_value__(self.long_description)

        return f"({sanitized_file_name}, {sanitized_abbreviation}, {sanitized_ref_file_name}, {sanitized_short_description}, {sanitized_long_description})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_FILE_NAME_COLUMN_NAME}, {SQL_ABBREVIATION_COLUMN_NAME}, {SQL_REF_FILE_NAME_COLUMN_NAME}, {SQL_SHORT_DESCRIPTION_COLUMN_NAME}, {SQL_LONG_DESCRIPTION_COLUMN_NAME}"