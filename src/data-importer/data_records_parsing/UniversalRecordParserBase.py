import os

from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.constants import DATA_TYPES_FILE_NAME
from common.file_paths import DESTINATION_DATA_FOLDER_PATH
from data_types_parsing.DataType import DataType
from data_types_parsing.DataTypesParser import extract_data_type


def check_record_file_structure(data_type_origin_file, data_type: DataType):
    first_line = data_type_origin_file.readline().strip()

    stripped_first_line = first_line.lower()
    cols = data_type.cols.replace("\\t", "\t").lower()

    first_line_tuples = stripped_first_line.split("\t")
    cols_tuples = cols.split("\t")

    if len(first_line_tuples) > len(cols_tuples):
        sanitized_cols = data_type.cols.replace("\t", "\\t")
        sanitized_stripped_first_line = first_line.replace("\t", "\\t")
        print(f'Header columns does not match the number of predefined columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{len(cols_tuples)}" - "{len(first_line_tuples)}".')
        raise ValueError(f'Header columns does not match the number of predefined columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{len(cols_tuples)}" - "{len(first_line_tuples)}".')


    for i in range(len(first_line_tuples)):
            first_line_part = first_line_tuples[i].strip()
            cols_part = cols_tuples[i].strip()

            if first_line_part != cols_part:
                sanitized_cols = data_type.cols.replace("\t", "\\t")
                sanitized_stripped_first_line = first_line.replace("\t", "\\t")
                print(f'Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{cols_part}" - "{first_line_part}".')
                raise ValueError(f'Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{cols_part}" - "{first_line_part}".')

    # if stripped_first_line != cols:
    #     sanitized_cols = cols.replace("\t", "\\t")
    #     sanitized_stripped_first_line = stripped_first_line.replace("\t", "\\t")
    #     print(f'First line does not match the columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}".')
    #     raise ValueError(f'First line does not match the columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}".')

    # this can be optional
    # second_line = open_file.readline().lower()
    # stripped_second_line = second_line.rstrip().lower()
    # under = data_type.under.replace("\\t", "\t")
    # if stripped_second_line != under:
    #     sanitized_under = under.replace("\t", "\\t")
    #     sanitized_stripped_second_line = stripped_second_line.replace("\t", "\\t")
    #     print(f'Second line does not match the format. Expected: "{sanitized_under}" - received: "{sanitized_stripped_second_line}".')
    #     raise ValueError(f'Second line does not match the format. Expected: "{sanitized_under}" - received: "{sanitized_stripped_second_line}".')

    # reset pointer to beginning
    data_type_origin_file.seek(0)


def process_record(
        folder_name: str,
        file_name: str,
        line: str,
        columns: [str],
        column_formats: [str]) -> str | None:
    line_tuple = line.strip("\n").split("\t")

    if len(line_tuple) > len(column_formats):
        column_formats_sanitized_string = "\\t".join(column_formats)
        line_tuple_sanitized_string = "\\t".join(line_tuple)
        print(f'Number of values does not match the number of predefined columns. Expected: "{column_formats_sanitized_string}" - received: "{line_tuple_sanitized_string}". "{len(column_formats)}" - "{len(line_tuple)}".')
        raise ValueError(f'Number of values does not match the number of predefined columns. Expected: "{column_formats_sanitized_string}" - received: "{line_tuple_sanitized_string}". "{len(column_formats)}" - "{len(line_tuple)}".')

    line = f"'{folder_name}', '{file_name}'"

    for i in range(len(column_formats)):
        line = "".join([line, ", "])

        if i >= len(line_tuple):
            line = "".join([line, "NULL"])
            continue

        column_format: str = column_formats[i]
        column_value: str = line_tuple[i].strip().replace("'", "''")

        # empty value -> NULL
        if len(column_value) == 0 :
            line = "".join([line, "NULL"])
            continue

        if len(columns) > i:
            column_title: str = columns[i]
            if column_title.lower().__eq__('no'):  # adopter number consider as string and remove zeros in the beginning
                sanitized_value = InsertLineBuilderBase.__sanitize_adopted_number__(column_value)
                line = "".join([line, sanitized_value])
                continue

        if "d" in column_format or "f" in column_format:
            sanitized_value = InsertLineBuilderBase.__sanitize_numeric_value__(column_value)
            line = "".join([line, sanitized_value])
        else:
            sanitized_value = InsertLineBuilderBase.__sanitize_string_value__(column_value)
            line = "".join([line, sanitized_value])

    return line

def process_data_types() -> [DataType]:
    source_file = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_TYPES_FILE_NAME)

    data_types: [DataType] = []

    with open(source_file, "rt") as source:
        while True:
            data_type = extract_data_type(source)

            if data_type is None:
                break

            data_types.append(data_type)

    return data_types