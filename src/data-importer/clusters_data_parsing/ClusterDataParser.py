import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, \
    write_sql_values_data_statement
from common.constants import CLUSTERS_TABLE_NAME, CLUSTERS_DATA_FOLDER_NAME, SQL_CLUSTERS_FILE_NAME, \
    CLUSTER_PARAMETERS_FILE_NAME, CLUSTER_PARAMETERS_TABLE_NAME, SQL_CLUSTER_PARAMETERS_FILE_NAME, \
    ERROR_OUTPUT_FILE_NAME, \
    NO_DATA_WERE_FOUND_SQL_COMMENT, BUFFER_SIZE
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, CLUSTERS_ORIGIN_FOLDER_PATH
from Cluster import Cluster
from ClusterParameters import ClusterParameters


def check_standard(open_file):
    first_data_line = open_file.readline().strip()

    if "Description\tParam" not in first_data_line:
        raise ValueError("First data line does not start with 'Description'.")

    second_data_line = open_file.readline().strip()

    if "-----------\t-----" not in second_data_line:
        raise ValueError("Second data line does not start with 'Description'.")

    open_file.seek(0)

    return open_file


def process_cluster(lines: [str], folder_name: str) -> (Cluster, ClusterParameters):
    iau_cluster_number = None
    name = None
    right_ascension = None
    declination = None
    longitude = None
    latitude = None
    angular_diameter = None
    distance = None
    ebv = None
    log_age = None
    feh = None
    radial_velocity = None
    proper_motion_ra = None
    proper_motion_dec = None

    for line in lines:
        pair: (str, str) = line.split("\t", 1)

        if len(pair) == 2:
            key, value = pair
        else:
            continue

        stripped_value = value.strip()

        match key.lower():
            case "iau cluster number":
                iau_cluster_number = stripped_value
            case "cluster name":
                name = stripped_value
            case "right ascension j2000":
                right_ascension = stripped_value
            case "declination j2000":
                declination = stripped_value
            case "galactic longitude":
                longitude = stripped_value
            case "galactic latitude":
                latitude = stripped_value
            case "angular diameter":
                angular_diameter = stripped_value
            case "distance":
                distance = stripped_value
            case "e(b-v)":
                ebv = stripped_value
            case "log(age)":
                log_age = stripped_value
            case "fe/h":
                feh = stripped_value
            case "radial velocity":
                radial_velocity = stripped_value
            case "proper motion ra":
                proper_motion_ra = stripped_value
            case "proper motion  dec":
                proper_motion_dec = stripped_value

    stripped_folder_name = folder_name.strip()

    return (Cluster(iau_cluster_number, stripped_folder_name, name),
            ClusterParameters(stripped_folder_name, right_ascension, declination, longitude, latitude, angular_diameter,
                              distance, ebv, log_age, feh, radial_velocity, proper_motion_ra, proper_motion_dec))


def process_cluster_data_file(
        original_cluster_details_file_path,
        clusters_destination_sql_file,
        cluster_parameters_destination_sql_file,
        error_output_destination_file_path,
        cluster_folder_name: str,
        comma_shall_be_writen: [bool]):
    # if dias.dat does not exist for the cluster create only cluster record
    if not os.path.exists(original_cluster_details_file_path):
        print(f"FILE NOT FOUND: '{original_cluster_details_file_path}'. Creating empty cluster record.", )

        # use folder name also as the cluster name
        cluster, cluster_parameters = process_cluster(["\t".join(["cluster name", cluster_folder_name])],
                                                      cluster_folder_name)

        if comma_shall_be_writen[0]:
            # write insert command to file
            clusters_destination_sql_file.write(",\n")

        comma_shall_be_writen[0] = True

        # create and write VALUES part of SQL insert command
        write_sql_values_data_statement(clusters_destination_sql_file, cluster)
        return

    with open(original_cluster_details_file_path, 'rt') as original_cluster_details_file:
        print(f"Processing file content: {original_cluster_details_file_path}")

        # check the format
        try:
            check_standard(original_cluster_details_file)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(error_output_destination_file_path, "at") as not_processed_files_file:
                message = "\t".join(["File not processed", original_cluster_details_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return

        # process record in the file
        _ = original_cluster_details_file.readline()  # first line
        _ = original_cluster_details_file.readline()  # second line

        lines = []

        # collect record properties
        line = original_cluster_details_file.readline()
        while line and line != "\n":
            lines.append(line)
            line = original_cluster_details_file.readline()

        # process record
        cluster, cluster_parameters = process_cluster(lines, cluster_folder_name)

        if comma_shall_be_writen[0]:
            # write insert command to file
            clusters_destination_sql_file.write(",\n")

        comma_shall_be_writen[0] = True

        # create and write VALUES part of SQL insert command for CLUSTERS file
        write_sql_values_data_statement(clusters_destination_sql_file, cluster)

        if comma_shall_be_writen[1]:
            # write insert command to file
            cluster_parameters_destination_sql_file.write(",\n")

        comma_shall_be_writen[1] = True

        # create and write VALUES part of SQL insert command for CLUSTER PARAMETERS file
        write_sql_values_data_statement(cluster_parameters_destination_sql_file, cluster_parameters)


def main():
    # check folder path existence and create folder on path if it does not exist yet
    clusters_destination_folder_path: str = os.path.join(DESTINATION_DATA_FOLDER_PATH, CLUSTERS_DATA_FOLDER_NAME)
    if not os.path.exists(clusters_destination_folder_path):
        os.makedirs(clusters_destination_folder_path, exist_ok=True)

    # error destination file
    error_output_destination_file_path: str = os.path.join(clusters_destination_folder_path, ERROR_OUTPUT_FILE_NAME)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    clusters_origin_folder_content = os.listdir(CLUSTERS_ORIGIN_FOLDER_PATH)
    cluster_folder_names = [f for f in clusters_origin_folder_content if
                            os.path.isdir(os.path.join(CLUSTERS_ORIGIN_FOLDER_PATH, f))]

    # destination sql files paths
    clusters_destination_sql_file_path: str = os.path.join(clusters_destination_folder_path, SQL_CLUSTERS_FILE_NAME)
    cluster_parameters_destination_sql_file_path: str = os.path.join(clusters_destination_folder_path,
                                                                SQL_CLUSTER_PARAMETERS_FILE_NAME)

    # CHECK
    if len(cluster_folder_names) == 0:
        with open(clusters_destination_sql_file_path,
                  "wt") as clusters_destination_sql_file:
            clusters_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        with open(cluster_parameters_destination_sql_file_path,
                  "wt") as cluster_parameters_destination_sql_file:
            cluster_parameters_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        return

    cluster_folder_names.sort()

    # [cluster file , cluster parameters file]
    comma_shall_be_writen: [bool] = [False, False]

    with open(clusters_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as clusters_destination_sql_file:
        # create and write INSERT part of SQL insert command
        cluster_table_parameters = Cluster.get_table_parameters()
        write_sql_insert_statement(clusters_destination_sql_file, CLUSTERS_TABLE_NAME, cluster_table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(clusters_destination_sql_file)

        with open(cluster_parameters_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as cluster_parameters_destination_sql_file:
            # create and write INSERT part of SQL insert command
            cluster_parameters_table_parameters = ClusterParameters.get_table_parameters()
            write_sql_insert_statement(cluster_parameters_destination_sql_file, CLUSTER_PARAMETERS_TABLE_NAME,
                                       cluster_parameters_table_parameters)

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(cluster_parameters_destination_sql_file)

            for cluster_folder_name in cluster_folder_names:
                original_cluster_details_file_path = os.path.join(CLUSTERS_ORIGIN_FOLDER_PATH, cluster_folder_name,
                                                                  CLUSTER_PARAMETERS_FILE_NAME)

                process_cluster_data_file(
                    original_cluster_details_file_path,
                    clusters_destination_sql_file,
                    cluster_parameters_destination_sql_file,
                    error_output_destination_file_path,
                    cluster_folder_name,
                    comma_shall_be_writen)

            # write finish of the insert command to file clusters sql file
            if comma_shall_be_writen[0]:
                # write end of INSERT command to file
                clusters_destination_sql_file.write(";\n")

            # write finish of the insert command to file cluster parameters sql file
            if comma_shall_be_writen[1]:
                # write end of INSERT command to file
                cluster_parameters_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(clusters_destination_sql_file_path, "wt") as clusters_destination_sql_file:
            clusters_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)

    if not comma_shall_be_writen[1]:
        # remove file contents as no data were found
        with open(cluster_parameters_destination_sql_file_path, "wt") as cluster_parameters_destination_sql_file:
            cluster_parameters_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)


if __name__ == '__main__':
    main()
