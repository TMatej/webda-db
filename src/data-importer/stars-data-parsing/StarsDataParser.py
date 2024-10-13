import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import STARS_TABLE_NAME, CLUSTERS_DATA_FOLDER_NAME, NUMBERING_SYSTEM_FILE_NAME, \
    DATA_FILE_REFERENCE_FILE_NAME, SQL_STARS_FILE_NAME
from common.file_paths import ORIGIN_CLUSTERS_FOLDER_PATH, ORIGIN_PUBLICATIONS_FOLDER_PATH, DESTINATION_DATA_FOLDER_PATH
from Star import Star

def check_standard(open_file):
    first_data_line = open_file.readline().strip()

    if first_data_line.lower().startswith("col"):               #
        return False

    if first_data_line.lower().startswith("description"):       #
        return False

    if first_data_line.lower().startswith("year"):              # bdp.cat
        return False

    if not first_data_line.lower().startswith("no"):
        raise ValueError("First data line does not start with 'No'")

    second_data_line = open_file.readline().strip()
    if not second_data_line.startswith("--"):
        raise ValueError("Second data line does not start with '--'")

    open_file.seek(0)

    return True

def main():
    folder_content = os.listdir(ORIGIN_CLUSTERS_FOLDER_PATH)

    cluster_folder_names = [f for f in folder_content if os.path.isdir(os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, f))]
    cluster_folder_names.sort()

    # per each cluster
    for cluster_folder in cluster_folder_names:
        print(cluster_folder)

        # get all data types names
        data_files_names_set: set = set()
        with open (os.path.join(ORIGIN_PUBLICATIONS_FOLDER_PATH, DATA_FILE_REFERENCE_FILE_NAME), "rt") as data_file_reference_file_source:
            while True:
                line = data_file_reference_file_source.readline()

                if not line:
                    break

                data_file, _ = line.split("\t", 1)
                data_files_names_set.add(data_file)

        adopted_numbers_set: set = set()

        # get all files in folder that correspond to data types names
        for data_file_name in data_files_names_set:
            file_in_origin = os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, cluster_folder, data_file_name)

            # if file does not exist
            if not os.path.exists(file_in_origin):
                continue

            # get all ids of stars in data files -> set<int>
            with open(file_in_origin, "rt") as data_file_source:
                # check file structure
                if not check_standard(data_file_source):
                    continue

                # skip first two lines
                next(data_file_source)
                next(data_file_source)

                # take ids from first column NO
                while True:
                    line = data_file_source.readline()

                    if not line:
                        break

                    if line.startswith("\n"):
                        continue

                    split_line = line.split("\t", 1)

                    if len(split_line) < 2:
                        adopted_number = split_line[0]
                    else:
                        adopted_number, _ = split_line

                    adopted_numbers_set.add(adopted_number.strip())

        # get bibcode of original numbering system publication if exists
        bibcode: str = ""
        with open(os.path.join(ORIGIN_PUBLICATIONS_FOLDER_PATH, NUMBERING_SYSTEM_FILE_NAME), "rt") as numbering_system_file:
            _ = numbering_system_file.readline()    # ignore first line
            _ = numbering_system_file.readline()    # ignore second line

            while True:
                line = numbering_system_file.readline()

                if not line:
                    break

                cluster_name, _ = line.split("\t", 1)

                if cluster_folder.lower() != cluster_name.lower():
                    continue

                line = numbering_system_file.readline()

                second_cluster_name, the_bibcode = line.split("\t", 1)

                if cluster_folder.lower() != second_cluster_name.lower():
                    # cluster does not have adopted numbers from any publication
                    break

                bibcode = the_bibcode.strip()
                break

        # create the stars insert command
        destination_folder = os.path.join(DESTINATION_DATA_FOLDER_PATH, CLUSTERS_DATA_FOLDER_NAME, cluster_folder)

        with open(os.path.join(destination_folder, SQL_STARS_FILE_NAME), "wt") as sql_stripped_data_file:
            if len(adopted_numbers_set) > 0:
                write_sql_insert_statement(sql_stripped_data_file, STARS_TABLE_NAME, Star.get_table_parameters())
                write_sql_values_keyword_statement(sql_stripped_data_file)

                first_line_was_written = False

                sorted_adopted_numbers = sorted(adopted_numbers_set)
                for adopted_star_number in sorted_adopted_numbers:
                    star = Star(cluster_folder, adopted_star_number, bibcode)

                    if first_line_was_written:
                        # write insert command to file
                        sql_stripped_data_file.write(",\n")

                    first_line_was_written = True

                    # create and write VALUES part of SQL insert command
                    write_sql_values_data_statement(sql_stripped_data_file, star)

                # write finish of the insert command to file
                sql_stripped_data_file.write(";\n")


if __name__ == '__main__':
    main()