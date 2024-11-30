import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import STARS_TABLE_NAME, DATA_FILE_REFERENCE_FILE_NAME, SQL_STARS_FILE_NAME, \
    STARS_DATA_FOLDER_NAME, NO_DATA_WERE_FOUND_SQL_COMMENT, \
    ERROR_OUTPUT_FILE_NAME, BUFFER_SIZE, CLUSTERS_ORIGIN_FOLDER_NAME, DATA_DESTINATION_FOLDER_NAME
from common.file_paths import ORIGIN_FOLDER_PATH, DESTINATION_FOLDER_PATH
from Star import Star

def check_standard(data_file_origin_file):
    first_data_line = data_file_origin_file.readline().strip()

    if first_data_line.lower().startswith("col"):               #
        return False

    if first_data_line.lower().startswith("description"):       #
        return False

    if first_data_line.lower().startswith("year"):              # bdp.cat
        return False

    if not first_data_line.lower().startswith("no"):
        raise ValueError("First data line does not start with 'No'")

    second_data_line = data_file_origin_file.readline().strip()
    if not second_data_line.startswith("--"):
        raise ValueError("Second data line does not start with '--'")

    data_file_origin_file.seek(0)

    return True

def get_data_types_names(data_file_reference_file_source_path: str) -> set:
    data_files_names_set: set = set()

    with open(data_file_reference_file_source_path, "rt") as data_file_reference_file_source:
        while True:
            line = data_file_reference_file_source.readline()

            if not line:
                break

            data_file, _ = line.split("\t", 1)
            data_files_names_set.add(data_file)

    # remove types without star data -> based on "types_excluded_from_processing.txt" file
    data_files_names_set.remove("lyn.dat")
    data_files_names_set.remove("trans.tab")
    data_files_names_set.remove("trans.ref")
    data_files_names_set.remove("bdp.cat")
    data_files_names_set.remove("br.cmd")
    data_files_names_set.remove("vie.mes")
    data_files_names_set.remove("vik.mes")
    data_files_names_set.remove("vrin.ccd")
    data_files_names_set.remove("xmm.src")
    data_files_names_set.remove("xmm.xray")

    return data_files_names_set

def get_adopted_numbers_from_file(data_file_origin_file_path):
    adopted_numbers_set: set = set()

    # if file does not exist
    if not os.path.exists(data_file_origin_file_path):
        # print(f"FILE NOT FOUND: '{data_file_origin_file_path}'.")
        return adopted_numbers_set

    # get all ids of stars in data files -> set<string>
    with open(data_file_origin_file_path, "rt") as data_file_origin_file:
        print(f"Processing file: {data_file_origin_file_path}")

        # check file structure
        if not check_standard(data_file_origin_file):
            return adopted_numbers_set

        # skip first two lines
        next(data_file_origin_file)
        next(data_file_origin_file)

        # take ids from first column NO
        while True:
            line = data_file_origin_file.readline()

            if not line:
                break

            if line.startswith("\n"):
                continue

            split_line = line.split("\t", 1)

            if len(split_line) < 2:
                adopted_number = split_line[0]
            else:
                adopted_number, _ = split_line

            adopted_numbers_set.add(adopted_number)

    return adopted_numbers_set

def get_adopted_numbers(
        cluster_folder_name: str,
        data_files_names_set: set) -> set:
    adopted_numbers_set: set = set()

    # get all files in folder that correspond to data types names
    for data_file_name in data_files_names_set:
        data_file_origin_file_path = os.path.join(ORIGIN_FOLDER_PATH, CLUSTERS_ORIGIN_FOLDER_NAME, cluster_folder_name, data_file_name)

        # if file does not exist
        if not os.path.exists(data_file_origin_file_path):
            continue

        # get all ids of stars in data files -> set<string>
        with open(data_file_origin_file_path, "rt") as data_file_origin_file:
            print(f"Processing folder content: {data_file_name}")
            # check file structure
            if not check_standard(data_file_origin_file):
                continue

            # skip first two lines
            next(data_file_origin_file)
            next(data_file_origin_file)

            # take ids from first column NO
            while True:
                line = data_file_origin_file.readline()

                if not line:
                    break

                if line.startswith("\n"):
                    continue

                split_line = line.split("\t", 1)

                if len(split_line) < 2:
                    adopted_number = split_line[0]
                else:
                    adopted_number, _ = split_line

                # remove starting zeros and spaces in the middle of the number
                sanitized_adopted_number = adopted_number.strip().lstrip("0").strip()

                adopted_numbers_set.add(sanitized_adopted_number)

    return adopted_numbers_set

def process_adopted_numbers(
        stars_destination_sql_file,
        adopted_star_numbers: set,
        cluster_folder_name: str,
        comma_shall_be_writen: [bool]):
    sorted_adopted_star_numbers: list = sorted(adopted_star_numbers)

    for adopted_star_number in sorted_adopted_star_numbers:
        star = Star(cluster_folder_name, adopted_star_number)

        if star.adopted_number == "" or star.adopted_number is None:
            continue

        if comma_shall_be_writen[0]:
            # write insert command to file
            stars_destination_sql_file.write(",\n")

        comma_shall_be_writen[0] = True

        # create and write VALUES part of SQL insert command
        write_sql_values_data_statement(stars_destination_sql_file, star)

def main():
    # check folder path existence and create folder on path if it does not exist yet
    stars_destination_sql_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, STARS_DATA_FOLDER_NAME)
    if not os.path.exists(stars_destination_sql_folder_path):
        os.makedirs(stars_destination_sql_folder_path, exist_ok=True)

    # error destination file
    error_output_destination_file_path: str = os.path.join(stars_destination_sql_folder_path,
                                                           ERROR_OUTPUT_FILE_NAME)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    clusters_origin_folder_path: str = os.path.join(ORIGIN_FOLDER_PATH, CLUSTERS_ORIGIN_FOLDER_NAME)
    clusters_origin_folder_content = os.listdir(clusters_origin_folder_path)
    cluster_folder_names = [f for f in clusters_origin_folder_content if os.path.isdir(os.path.join(clusters_origin_folder_path, f))]

    # destination sql file path
    stars_destination_sql_file_path: str = os.path.join(stars_destination_sql_folder_path, SQL_STARS_FILE_NAME)

    # CHECK
    if len(cluster_folder_names) == 0:
        with open(stars_destination_sql_file_path, "wt") as stars_destination_sql_file:
            stars_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        return

    cluster_folder_names.sort()

    # get all data types names
    data_file_reference_file_source_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, DATA_FILE_REFERENCE_FILE_NAME)
    data_files_names_set: [str] = get_data_types_names(data_file_reference_file_source_path)

    # CHECK
    if len(data_files_names_set) == 0:
        with open(stars_destination_sql_file_path, "wt") as stars_destination_sql_file:
            stars_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        return

    comma_shall_be_writen: [bool] = [False]

    with open(stars_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as stars_destination_sql_file:
        # create and write INSERT part of SQL insert command
        table_parameters = Star.get_table_parameters()
        write_sql_insert_statement(stars_destination_sql_file, STARS_TABLE_NAME, table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(stars_destination_sql_file)

        # per each cluster
        for cluster_folder_name in cluster_folder_names:
            adopted_star_numbers: [] = set()

            for data_file_name in data_files_names_set:
                data_origin_file_path = os.path.join(clusters_origin_folder_path, cluster_folder_name, data_file_name)
                adopted_star_numbers_from_file = get_adopted_numbers_from_file(data_origin_file_path)
                adopted_star_numbers = adopted_star_numbers.union(adopted_star_numbers_from_file)

            if len(adopted_star_numbers) == 0:
                continue

            process_adopted_numbers(
                stars_destination_sql_file,
                adopted_star_numbers,
                cluster_folder_name,
                comma_shall_be_writen)

        # write finish of the insert command to file
        stars_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(stars_destination_sql_file_path, "wt") as stars_destination_sql_file:
            stars_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)


if __name__ == '__main__':
    main()