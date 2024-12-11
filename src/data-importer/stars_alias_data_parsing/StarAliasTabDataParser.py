import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import SQL_STAR_ALIASES_TAB_DATA_FILE_NAME, STAR_ALIASES_DATA_FOLDER_NAME, TRANS_TAB_FILE, TRANS_REF_FILE, \
    STAR_ALIASES_TABLE_NAME, ERROR_OUTPUT_FILE_NAME, NO_DATA_WERE_FOUND_SQL_COMMENT, BUFFER_SIZE, \
    CLUSTERS_ORIGIN_FOLDER_NAME, DATA_DESTINATION_FOLDER_NAME
from common.folder_paths import ORIGIN_FOLDER_PATH, DESTINATION_FOLDER_PATH
from StarAlias import StarAlias

def check_trans_ref_standard(open_file):
    first_data_line = open_file.readline().strip()

    if "Col\tReference" not in first_data_line:
        raise ValueError("First data line does not contain 'Col\tReference'.")

    second_data_line = open_file.readline().strip()

    if "---\t---------" not in second_data_line:
        raise ValueError("Second data line does not contain '---\t---------'.")

    open_file.seek(0)

    return True

def check_trans_tab_standard(open_file, column_reference_dictionary: {}):
    first_data_line = open_file.readline().strip()

    parts = first_data_line.split("\t")

    # values check - if trans.ref contains more references than trans.tab
    # for key in column_reference_dictionary.keys():
    #     if key.lower() not in map(str.lower, parts):
    #         raise ValueError(f"Trans.tab file does not contain column named: '{key.lower()}' - '{", ".join(parts)}'.")

    # format check
    if not parts[0].lower().__eq__("no"):
        raise ValueError(f"Column 'No' is missing.")

    # for part in parts:
    #     if part.lower().__eq__("no"):
    #         continue
    #
    #     if part.lower() not in column_value_dictionary.keys():
    #         raise ValueError(f"Key {part.lower()} is not in the dictionary keys: '{", ".join(column_value_dictionary.keys())}'.")

    # second line check is skipped

    open_file.seek(0)

    return True

def process_ref_record(line: str, column_reference_dictionary: {}):
    line_tuple: (str, str) = line.split("\t")

    if len(line_tuple) != 2:
        column_formats_sanitized_string = "Col\tReference"
        line_tuple_sanitized_string = "\t".join(line_tuple)
        print(f'Number of values does not match the number of predefined columns. Expected: "{column_formats_sanitized_string}" - received: "{line_tuple_sanitized_string}". "2" - "{len(line_tuple)}".')
        raise ValueError(f'Number of values does not match the number of predefined columns. Expected: "{column_formats_sanitized_string}" - received: "{line_tuple_sanitized_string}". "2" - "{len(line_tuple)}".')

    key = line_tuple[0].lower().strip()
    value = line_tuple[1].strip()

    if key != "" and key is not None and value != "" and value is not None:
        column_reference_dictionary[key] = value
    else:
        print(f"Invalid value pair: Key: '{key}', Value: '{value}'.")

def process_header_line(header_line: str) -> dict:
    reference_position_dictionary: dict = {}

    line_tuple: {str} = header_line.strip().split("\t")

    for i in range(len(line_tuple)):
        reference_position_dictionary[line_tuple[i].lower().strip()] = i

    return reference_position_dictionary

def process_tab_record(
        line: str,
        column_reference_dictionary: dict,
        reference_position_dictionary: dict,
        cluster_name: str) -> [StarAlias]:
    line_tuple: [str] = line.strip().split("\t")

    star_aliases = []

    adopted_number_position: int = reference_position_dictionary["no"]
    adopted_number = line_tuple[adopted_number_position]

    for key in column_reference_dictionary.keys():
        ref_string = column_reference_dictionary[key.lower()]

        # check if trans.tab does not contain all columns defined in trans.ref
        if not reference_position_dictionary.keys().__contains__(key):
            continue

        alternative_number_position: int = reference_position_dictionary[key.lower()]

        # check if nuber of tuples is less due to missing values and multiple \t\t\t in the end of the line
        if alternative_number_position > len(line_tuple)-1:
            continue

        alternative_number = line_tuple[alternative_number_position]

        star_alias = StarAlias(cluster_name, adopted_number, alternative_number, ref_string, "tab")
        star_aliases.append(star_alias)

    return star_aliases

def process_trans_ref_file(
        trans_ref_origin_file_path: str,
        error_output_destination_file_path: str) -> dict | None:
    column_reference_dictionary = dict()

    # if data type file does not exist for the cluster skip to next
    if not os.path.exists(trans_ref_origin_file_path):
        print(f"FILE NOT FOUND: '{trans_ref_origin_file_path}'.")
        return None

    with open(trans_ref_origin_file_path, "rt") as trans_ref_file:
        # check the format
        try:
            check_trans_ref_standard(trans_ref_file)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(error_output_destination_file_path, "at") as not_processed_files_file:
                message = "\t".join(["File not processed", trans_ref_origin_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return None

        #  move to first record
        _ = trans_ref_file.readline()  # header
        _ = trans_ref_file.readline()  # header splitter
        line_number = 2  # first two lines were already read

        # process each record in the file
        while True:
            line = trans_ref_file.readline()
            line_number += 1

            if not line:
                break

            # skip empty line
            if line == "\n":
                continue

            # process record
            try:
                process_ref_record(line, column_reference_dictionary)
            except ValueError as e:
                # store info about not processed lines due to format inconsistency
                with open(error_output_destination_file_path, "at") as not_processed_files_file:
                    message = "\t".join(
                        ["Line not processed", line_number.__str__(), trans_ref_origin_file_path, e.__str__(), "\n"])
                    not_processed_files_file.write(message)

    return column_reference_dictionary

def process_trans_tab_file(
        trans_tab_origin_file_path: str,
        star_aliases_destination_sql_file,
        error_output_destination_file_path: str,
        cluster_folder_name: str,
        column_reference_dictionary: dict,
        comma_shall_be_writen: [bool]):
    # return if file does not exist
    if not os.path.exists(trans_tab_origin_file_path):
        print(f"FILE NOT FOUND: '{trans_tab_origin_file_path}'.")
        return

    with open(trans_tab_origin_file_path, "rt") as trans_tab_origin_file:
        # check the format
        try:
            check_trans_tab_standard(trans_tab_origin_file, column_reference_dictionary)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(error_output_destination_file_path, "at") as not_processed_files_file:
                message = "\t".join(["File not processed", trans_tab_origin_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return

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
                star_aliases = process_tab_record(line, column_reference_dictionary, reference_position_dictionary,
                                                  cluster_folder_name)
            except ValueError as e:
                # store info about not processed lines due to format inconsistency
                with open(error_output_destination_file_path, "at") as not_processed_files_file:
                    message = "\t".join(
                        ["Line not processed", line_number.__str__(), trans_tab_origin_file_path, e.__str__(), "\n"])
                    not_processed_files_file.write(message)

            for star_alias in star_aliases:
                if not star_alias.alternative_number:
                    continue

                if comma_shall_be_writen[0]:
                    # write insert command to file
                    star_aliases_destination_sql_file.write(",\n")

                comma_shall_be_writen[0] = True

                # create and write VALUES part of SQL insert command
                write_sql_values_data_statement(star_aliases_destination_sql_file, star_alias)

def main():
    # check folder path existence and create folder on path if it does not exist yet
    star_aliases_destination_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, STAR_ALIASES_DATA_FOLDER_NAME)
    if not os.path.exists(star_aliases_destination_folder_path):
        os.makedirs(star_aliases_destination_folder_path, exist_ok=True)

    # error destination file
    error_output_file_name = "-".join(["tab", ERROR_OUTPUT_FILE_NAME])
    error_output_destination_file_path: str = os.path.join(star_aliases_destination_folder_path,
                                                           error_output_file_name)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    clusters_origin_folder_path: str = os.path.join(ORIGIN_FOLDER_PATH, CLUSTERS_ORIGIN_FOLDER_NAME)
    clusters_origin_folder_content = os.listdir(clusters_origin_folder_path)
    cluster_folder_names = [f for f in clusters_origin_folder_content if os.path.isdir(os.path.join(clusters_origin_folder_path, f))]

    # destination sql file path
    star_aliases_destination_sql_file_path: str = os.path.join(star_aliases_destination_folder_path,
                                                               SQL_STAR_ALIASES_TAB_DATA_FILE_NAME)

    if (len(cluster_folder_names) == 0):
        with open(star_aliases_destination_sql_file_path, "wt") as star_aliases_destination_sql_file:
            star_aliases_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        return

    cluster_folder_names.sort()

    comma_shall_be_writen: [bool] = [False]

    with open(star_aliases_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as star_aliases_destination_sql_file:
        # create and write INSERT part of SQL insert command
        table_parameters = StarAlias.get_table_parameters()
        write_sql_insert_statement(star_aliases_destination_sql_file, STAR_ALIASES_TABLE_NAME, table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(star_aliases_destination_sql_file)

        # per each cluster
        for cluster_folder_name in cluster_folder_names:
            trans_ref_origin_file_path: str = os.path.join(clusters_origin_folder_path, cluster_folder_name, TRANS_REF_FILE)

            column_reference_dictionary = process_trans_ref_file(
                trans_ref_origin_file_path,
                error_output_destination_file_path)

            # process further only if there exists any column reference data
            if column_reference_dictionary is None:
                continue

            trans_tab_origin_file_path: str = os.path.join(clusters_origin_folder_path, cluster_folder_name, TRANS_TAB_FILE)

            # existence of trans ref file indicates existence of trans tab file
            if not os.path.exists(trans_tab_origin_file_path):
                raise ValueError(f"File {trans_tab_origin_file_path} does not exist.")

            process_trans_tab_file(
                    trans_tab_origin_file_path,
                    star_aliases_destination_sql_file,
                    error_output_destination_file_path,
                    cluster_folder_name,
                    column_reference_dictionary,
                    comma_shall_be_writen)

        # write finish of the insert command to file
        star_aliases_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(star_aliases_destination_sql_file_path, "wt") as star_aliases_destination_sql_file:
            star_aliases_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)


if __name__ == '__main__':
    main()