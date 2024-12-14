from common.InsertLineBuilderBase import InsertLineBuilderBase
import re

from common.Sanitizer import Sanitizer
from common.constants import SQL_FOLDER_NAME_COLUMN_NAME, SQL_ADOPTED_NUMBER_COLUMN_NAME, \
    SQL_ALTERNATIVE_NUMBER_COLUMN_NAME, SQL_REF_STRING_COLUMN_NAME, SQL_REF_STRING_SQUEEZED_COLUMN_NAME, \
    SQL_STAR_ID_COLUMN_NAME, SQL_STAR_ALIAS_TYPE_COLUMN_NAME


class StarAlias(InsertLineBuilderBase):
    def __init__(
            self,
            folder_name: str = None,
            adopted_number: str = None,
            alternative_number: str = None,
            ref_string: str = None,
            star_alias_type:str = None):
        self.folder_name = folder_name
        self.adopted_number = adopted_number
        self.alternative_number = alternative_number
        self.ref_string = ref_string
        self.star_alias_type = star_alias_type

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = Sanitizer.__sanitize_string_value__(self.folder_name)
        sanitized_adopted_number = Sanitizer.__sanitize_adopted_number__(self.adopted_number)
        sanitized_alternative_number = Sanitizer.__sanitize_adopted_number__(self.alternative_number)
        sanitized_ref_string = Sanitizer.__sanitize_string_value__(self.ref_string)
        x = re.search(r"(\d+, ?\d+).*", sanitized_ref_string)
        if x is not None:
            sanitized_ref_string_squeezed = Sanitizer.__sanitize_string_value__(x.group(1))
        else:
            sanitized_ref_string_squeezed = "NULL"
        sanitized_star_alias_type = Sanitizer.__sanitize_string_value__(self.star_alias_type)

        return  f"(0, {sanitized_folder_name}, {sanitized_adopted_number}, {sanitized_alternative_number}, {sanitized_ref_string}, {sanitized_ref_string_squeezed}, {sanitized_star_alias_type})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_STAR_ID_COLUMN_NAME}, {SQL_FOLDER_NAME_COLUMN_NAME}, {SQL_ADOPTED_NUMBER_COLUMN_NAME}, {SQL_ALTERNATIVE_NUMBER_COLUMN_NAME}, {SQL_REF_STRING_COLUMN_NAME}, {SQL_REF_STRING_SQUEEZED_COLUMN_NAME}, {SQL_STAR_ALIAS_TYPE_COLUMN_NAME}"
