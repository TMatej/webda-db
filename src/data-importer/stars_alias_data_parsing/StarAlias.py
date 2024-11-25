from common.InsertLineBuilderBase import InsertLineBuilderBase


class StarAlias(InsertLineBuilderBase):
    def __init__(
            self,
            folder_name: str = None,
            adopted_number: str = None,
            alternative_number: str = None,
            ref_string: str = None,
    ):
        self.folder_name = folder_name
        self.adopted_number = adopted_number
        self.alternative_number = alternative_number
        self.ref_string = ref_string

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = self.__sanitize_string_value__(self.folder_name)
        sanitized_adopted_number = self.__sanitize_string_value__(self.adopted_number)
        sanitized_alternative_number = self.__sanitize_string_value__(self.alternative_number)
        sanitized_ref_string = self.__sanitize_string_value__(self.ref_string)

        return  f"({sanitized_folder_name}, {sanitized_adopted_number}, {sanitized_alternative_number}, {sanitized_ref_string})"

    @staticmethod
    def get_table_parameters() -> str:
        return "Foldername, AdoptedNumber, AlternativeNumber, RefString"