

class InsertLineBuilderBase:
    def build_insert_values_line(self) -> str:
        return f"()"

    @staticmethod
    def get_table_parameters() -> str:
        return ""