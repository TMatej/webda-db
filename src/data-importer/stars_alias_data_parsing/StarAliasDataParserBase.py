import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import STAR_ALIASES_TABLE_NAME, ERROR_OUTPUT_FILE_NAME, NO_DATA_WERE_FOUND_SQL_COMMENT, BUFFER_SIZE, \
    CLUSTERS_ORIGIN_FOLDER_NAME, DATA_DESTINATION_FOLDER_NAME, STAR_ALIASES_DATA_FOLDER_NAME
from common.folder_paths import ORIGIN_FOLDER_PATH, DESTINATION_FOLDER_PATH
from StarAlias import StarAlias
from data_records_parsing.UniversalRecordParserBase import process_data_types
from data_types_parsing.DataType import DataType


def process_header_line(header_line: str) -> dict:
    reference_position_dictionary: dict = {}

    line_tuple: {str} = header_line.strip().split("\t")

    for i in range(len(line_tuple)):
        column_name = line_tuple[i].lower().strip()
        reference_position_dictionary[column_name] = i

    return reference_position_dictionary

def process_record(
        line: str,
        reference_position_dictionary: dict,
        cluster_name: str,
        skip_columns: [str],
        star_alias_type: str) -> list[StarAlias]:
    line_tuple: [str] = line.strip("\n").split("\t")

    if len(line_tuple) != len(reference_position_dictionary.keys()):
        raise ValueError(f"Number of line arguments: '{len(line_tuple)}' does not match the number of columns: '{len(reference_position_dictionary.keys())}")

    star_aliases = []

    adopted_number_position: int = reference_position_dictionary["no"]
    adopted_number = line_tuple[adopted_number_position]

    for column_name in reference_position_dictionary.keys():
        if column_name == "no":
            continue

        # skip undesired columns
        if column_name in skip_columns:
            continue

        alternative_number_position: int = reference_position_dictionary[column_name.lower()]

        # check if nuber of tuples is less due to missing values and multiple \t\t\t in the end of the line
        if alternative_number_position > len(line_tuple)-1:
            continue

        alternative_number = line_tuple[alternative_number_position]

        star_alias = StarAlias(cluster_name, adopted_number, alternative_number, column_name, star_alias_type)
        star_aliases.append(star_alias)

    return star_aliases


def process_file(
        origin_file_path: str,
        star_aliases_destination_sql_file,
        error_output_destination_file_path: str,
        cluster_folder_name: str,
        data_type_abbreviation: str,
        skip_columns: [str],
        comma_shall_be_writen: [bool]):
    # return if file does not exist
    if not os.path.exists(origin_file_path):
        print(f"FILE NOT FOUND: '{origin_file_path}'.")
        return

    with open(origin_file_path, "rt") as trans_tab_origin_file:
        #  process header line
        header_line = trans_tab_origin_file.readline()  # header
        reference_position_dictionary: dict = process_header_line(header_line)

        _ = trans_tab_origin_file.readline()  # header splitter
        line_number = 2  # first two lines were already read

        # process each record in the file
        while True:
            line = trans_tab_origin_file.readline()
            line_number += 1

            if not line:
                break

            # skip empty line
            if line == "\n":
                continue

            star_aliases = []

            # process record
            try:
                star_aliases = process_record(
                    line,
                    reference_position_dictionary,
                    cluster_folder_name,
                    skip_columns,
                    data_type_abbreviation)
            except ValueError as e:
                # store info about not processed lines due to format inconsistency
                with open(error_output_destination_file_path, "at") as not_processed_files_file:
                    message = "\t".join(
                        ["Line not processed", line_number.__str__(), origin_file_path, e.__str__(), "\n"])
                    not_processed_files_file.write(message)

            for star_alias in star_aliases:
                if not star_alias.adopted_number:
                    continue

                if not star_alias.alternative_number:
                    continue

                if comma_shall_be_writen[0]:
                    # write insert command to file
                    star_aliases_destination_sql_file.write(",\n")

                comma_shall_be_writen[0] = True

                # create and write VALUES part of SQL insert command
                write_sql_values_data_statement(star_aliases_destination_sql_file, star_alias)

def process(data_type_abbreviation:str, data_type_file_name: str, destination_sql_file_name: str, skip_columns: [str]):
    # check folder path existence and create folder on path if it does not exist yet
    star_aliases_destination_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, STAR_ALIASES_DATA_FOLDER_NAME)
    if not os.path.exists(star_aliases_destination_folder_path):
        os.makedirs(star_aliases_destination_folder_path, exist_ok=True)

    # error destination file
    error_output_file_name = "-".join([data_type_abbreviation, ERROR_OUTPUT_FILE_NAME])
    error_output_destination_file_path: str = os.path.join(star_aliases_destination_folder_path,
                                                           error_output_file_name)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    clusters_origin_folder_path: str = os.path.join(ORIGIN_FOLDER_PATH, CLUSTERS_ORIGIN_FOLDER_NAME)
    clusters_origin_folder_content = os.listdir(clusters_origin_folder_path)
    cluster_folder_names = [f for f in clusters_origin_folder_content if os.path.isdir(os.path.join(clusters_origin_folder_path, f))]

    # destination sql file path
    star_aliases_destination_sql_file_path: str = os.path.join(star_aliases_destination_folder_path,
                                                               destination_sql_file_name)

    if (len(cluster_folder_names) == 0):
        with open(star_aliases_destination_sql_file_path, "wt") as star_aliases_destination_sql_file:
            star_aliases_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        return

    cluster_folder_names.sort()

    comma_shall_be_writen: [bool] = [False]

    # get srv data type
    data_types = process_data_types()
    data_type: DataType = l[0] if (l := list(filter(lambda x: x.file_name == data_type_file_name, data_types))) else None

    if data_type is None:
        message = f"Data type {data_type_abbreviation} was not found."
        with open(error_output_destination_file_path, "at") as error_output_destination_file:
            error_output_destination_file.write(message)
        print(message)
        return

    columns: [str] = data_type.cols.strip().split("\\t")
    columns_formats: [str] = data_type.format.strip().split()

    # check that number of columns matches the number of column formats
    if len(columns) != len(columns_formats):
        message = "\t".join([f"Data type: '{data_type.file_name}' not processed",
                             f"Number of columns: '{len(columns)}' differs from number of columns formats: '{len(columns_formats)}'.",
                             "\n"])
        with open(error_output_destination_file_path, "at") as error_output_destination_file:
            error_output_destination_file.write(message)
        print(message)
        return

    with open(star_aliases_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as star_aliases_destination_sql_file:
        # create and write INSERT part of SQL insert command
        table_parameters = StarAlias.get_table_parameters()
        write_sql_insert_statement(star_aliases_destination_sql_file, STAR_ALIASES_TABLE_NAME, table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(star_aliases_destination_sql_file)

        # per each cluster
        for cluster_folder_name in cluster_folder_names:
            origin_file_path: str = os.path.join(clusters_origin_folder_path, cluster_folder_name, data_type_file_name)

            process_file(
                origin_file_path,
                star_aliases_destination_sql_file,
                error_output_destination_file_path,
                cluster_folder_name,
                data_type.abbreviation,
                skip_columns,
                comma_shall_be_writen)

        # write finish of the insert command to file
        star_aliases_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(star_aliases_destination_sql_file_path, "wt") as star_aliases_destination_sql_file:
            star_aliases_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
