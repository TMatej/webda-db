from common.InsertLineBuilderBase import InsertLineBuilderBase


class Star(InsertLineBuilderBase):
    def __init__(
            self,
            folder_name: str = None,
            adopted_number: str = None):
        self.folder_name = folder_name  # cluster name (abbreviation)
        self.adopted_number = adopted_number

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = self.__sanitize_string_value__(self.folder_name)
        sanitized_adopted_number = self.__sanitize_string_value__(self.adopted_number)

        return  f"({sanitized_folder_name}, {sanitized_adopted_number})"

    @staticmethod
    def get_table_parameters() -> str:
        return "Foldername, AdoptedNumber"