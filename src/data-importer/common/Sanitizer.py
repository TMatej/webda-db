from decimal import Decimal, InvalidOperation

from pandas.core.construction import sanitize_array


class Sanitizer:
    @staticmethod
    def __sanitize_string_value__(value: str):
        return "NULL" if not value else "'" + value.replace("'", "''").strip() + "'"

    @staticmethod
    def __sanitize_numeric_value__(value: str):
        if not value:
            return "NULL"

        while value.__contains__("--"):
            value = value.replace("--", "-")

        try:
            num = str(Decimal(value.strip().lstrip("0")))
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

    @staticmethod
    def __sanitize_adopted_number__(value: str) -> str:
        # some files contain white characters inside their adopted number
        sanitized_value = Sanitizer.__sanitize_string_value__(value.strip().replace(' ', '0'))

        if sanitized_value.__eq__("NULL"):
            raise ValueError("Adopter number is missing")

        return sanitized_value
