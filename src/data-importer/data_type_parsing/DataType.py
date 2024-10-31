from common.InsertLineBuilderBase import InsertLineBuilderBase


class DataType(InsertLineBuilderBase):
    def __init__(self,
                 abbreviation: str = None,
                 file_name: str = None,
                 ref_file: str = None,
                 header: str = None,
                 format: str = None,
                 fttbl: str = None,
                 cols: str = None,
                 under: str = None,
                 long_description: str = None,
                 short_description: str = None):
        self.abbreviation = abbreviation
        self.file_name = file_name
        self.ref_file = ref_file
        self.header = header
        self.format = format
        self.fttbl = fttbl
        self.cols = cols
        self.under = under
        self.long_description = long_description
        self.short_description = short_description

    def build_insert_values_line(self) -> str:
        sanitized_abbreviation = self.abbreviation.strip().replace("'", "''")
        sanitized_file_name = self.file_name.strip().lower().replace("'", "''")
        sanitized_short_description = self.short_description.strip().replace("'", "''")
        sanitized_long_description = self.long_description.strip().replace("'", "''")

        return f"('{sanitized_abbreviation}', '{sanitized_file_name}', '{sanitized_short_description}', '{sanitized_long_description}')"

    @staticmethod
    def get_table_parameters() -> str:
        return "abbreviation, file_name, short_description, long_description"