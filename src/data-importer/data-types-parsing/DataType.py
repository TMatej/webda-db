from common.InsertLineBuilderBase import InsertLineBuilderBase


class DataType(InsertLineBuilderBase):
    def __init__(self, abbreviation = None, short_description = None, long_description = None):
        self.abbreviation = abbreviation
        self.short_description = short_description
        self.long_description = long_description

    def build_insert_values_line(self) -> str:
        sanitized_abbreviation = self.abbreviation.strip().replace("'", "''")
        sanitized_short_description = self.short_description.strip().replace("'", "''")
        sanitized_long_description = self.long_description.strip().replace("'", "''")

        return f"('{sanitized_abbreviation}', '{sanitized_short_description}', '{sanitized_long_description}')"

    @staticmethod
    def get_table_parameters() -> str:
        return "abbreviation, short_description, long_description"