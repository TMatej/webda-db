class InsertLineBuilderBase:
    @staticmethod
    def __sanitize_string_value__(value):
        return "NULL" if not value else "'" + value.replace("'", "''") + "'"

    @staticmethod
    def __sanitize_numeric_value__(value):
        return "NULL" if not value else value

    def build_insert_values_line(self) -> str:
        return f"()"

    @staticmethod
    def get_table_parameters() -> str:
        return ""