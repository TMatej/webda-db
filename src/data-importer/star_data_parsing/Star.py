from common.InsertLineBuilderBase import InsertLineBuilderBase


class Star(InsertLineBuilderBase):
    def __init__(self, folder_name: str = None, adopted_number: str = None, publication_bibcode: str = None):
        self.folder_name = folder_name
        self.adopted_number = adopted_number
        self.publication_bibcode = publication_bibcode

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = "'" + self.folder_name.lower().replace("'", "''") + "'"
        sanitized_adopted_number = "'" + self.adopted_number.replace("'", "''") + "'"
        sanitized_publication_bibcode = "'" + self.publication_bibcode.replace("'", "''") + "'"

        return  f"({sanitized_folder_name}, {sanitized_adopted_number}, {sanitized_publication_bibcode})"

    @staticmethod
    def get_table_parameters() -> str:
        return "folder_name, adopted_number, publication_bibcode"