from decimal import Decimal, InvalidOperation


class InsertLineBuilderBase:
    @staticmethod
    def __sanitize_string_value__(value: str):
        return "NULL" if not value else "'" + value.replace("'", "''") + "'"

    @staticmethod
    def __sanitize_numeric_value__(value: str):
        if not value:
            return "NULL"

        while value.__contains__("--"):
            value = value.replace("--", "-")

        try:
            num = str(Decimal(value))
        except InvalidOperation:
            print(f"Invalid input: '{value}' cannot convert to integer.")

            # some values have random "/  " with their value
            value = value.strip("/").strip()
            try:
                num = str(Decimal(value))
            except InvalidOperation:
                print(f"Invalid input: '{value}' cannot convert to integer.")
                num = "NULL"

        return num

    def build_insert_values_line(self) -> str:
        return f"()"

    @staticmethod
    def get_table_parameters() -> str:
        return ""