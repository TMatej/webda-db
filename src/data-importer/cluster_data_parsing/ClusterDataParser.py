import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, \
    write_sql_values_data_statement
from common.constants import CLUSTERS_TABLE_NAME, CLUSTERS_DATA_FOLDER_NAME, SQL_CLUSTERS_FILE_NAME, \
    CLUSTER_PARAMETERS_FILE, CLUSTER_PARAMETERS_TABLE_NAME, SQL_CLUSTER_PARAMETERS_FILE_NAME, ERROR_OUTPUT_FILE_NAME
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_CLUSTERS_FOLDER_PATH
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


def process_single_file(
        original_cluster_details_file_path,
        sql_destination_clusters_file,
        sql_destination_cluster_parameters_file,
        error_output_file_path,
        cluster_folder_name: str,
        first_lines_were_written: [bool]):
    # if dias.dat does not exist for the cluster create only cluster record
    if not os.path.exists(original_cluster_details_file_path):
        print("FILE NOT FOUND: ", original_cluster_details_file_path)

        # use folder name also as the cluster name
        cluster, cluster_parameters = process_cluster(["\t".join(["cluster name", cluster_folder_name])],
                                                      cluster_folder_name)

        if first_lines_were_written[0]:
            # write insert command to file
            sql_destination_clusters_file.write(",\n")

        first_lines_were_written[0] = True

        # create and write VALUES part of SQL insert command
        write_sql_values_data_statement(sql_destination_clusters_file, cluster)
        return

    with open(original_cluster_details_file_path, 'rt') as references_file:
        # check the format
        try:
            check_standard(references_file)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(error_output_file_path, "at") as not_processed_files_file:
                message = "\t".join(["File not processed", original_cluster_details_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return

        # process record in the file
        _ = references_file.readline()  # first line
        _ = references_file.readline()  # second line

        lines = []

        # collect record properties
        line = references_file.readline()
        while line and line != "\n":
            lines.append(line)
            line = references_file.readline()

        # process record
        cluster, cluster_parameters = process_cluster(lines, cluster_folder_name)

        if first_lines_were_written[0]:
            # write insert command to file
            sql_destination_clusters_file.write(",\n")

        first_lines_were_written[0] = True

        # create and write VALUES part of SQL insert command for CLUSTERS file
        write_sql_values_data_statement(sql_destination_clusters_file, cluster)

        if first_lines_were_written[1]:
            # write insert command to file
            sql_destination_cluster_parameters_file.write(",\n")

        first_lines_were_written[1] = True

        # create and write VALUES part of SQL insert command for CLUSTER PARAMETERS file
        write_sql_values_data_statement(sql_destination_cluster_parameters_file, cluster_parameters)


def main():
    destination_clusters_folder_path = os.path.join(DESTINATION_DATA_FOLDER_PATH, CLUSTERS_DATA_FOLDER_NAME)
    original_clusters_folder_content = os.listdir(ORIGIN_CLUSTERS_FOLDER_PATH)

    # create folder on path
    if not os.path.exists(destination_clusters_folder_path):
        os.mkdir(destination_clusters_folder_path)

    cluster_folder_names = [f for f in original_clusters_folder_content if
                    os.path.isdir(os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, f))]

    if len(cluster_folder_names) == 0:
        return

    cluster_folder_names.sort()

    # destination sql files paths
    sql_destination_clusters_file_path = os.path.join(destination_clusters_folder_path, SQL_CLUSTERS_FILE_NAME)
    sql_destination_cluster_parameters_file_path = os.path.join(destination_clusters_folder_path,
                                                                SQL_CLUSTER_PARAMETERS_FILE_NAME)

    # error destination file
    error_output_file_path = os.path.join(destination_clusters_folder_path, ERROR_OUTPUT_FILE_NAME)

    with open(sql_destination_clusters_file_path, "wt") as sql_destination_clusters_file:
        write_sql_insert_statement(sql_destination_clusters_file, CLUSTERS_TABLE_NAME, Cluster.get_table_parameters())
        write_sql_values_keyword_statement(sql_destination_clusters_file)

        with open(sql_destination_cluster_parameters_file_path, "wt") as sql_destination_cluster_parameters_file:
            write_sql_insert_statement(sql_destination_cluster_parameters_file, CLUSTER_PARAMETERS_TABLE_NAME,
                                       ClusterParameters.get_table_parameters())
            write_sql_values_keyword_statement(sql_destination_cluster_parameters_file)

            # [cluster file , cluster parameters file]
            first_lines_were_written: [bool] = [False, False]

            for cluster_folder_name in cluster_folder_names:
                original_cluster_details_file_path = os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, cluster_folder_name,
                                                            CLUSTER_PARAMETERS_FILE)

                process_single_file(
                    original_cluster_details_file_path,
                    sql_destination_clusters_file,
                    sql_destination_cluster_parameters_file,
                    error_output_file_path,
                    cluster_folder_name,
                    first_lines_were_written)

            # end clusters sql file
            if first_lines_were_written[0]:
                # write end of INSERT command to file
                sql_destination_clusters_file.write(";\n")

            # end cluster parameters sql file
            if first_lines_were_written[1]:
                # write end of INSERT command to file
                sql_destination_cluster_parameters_file.write(";\n")

    if not first_lines_were_written[0]:
        # remove file contents as no data were found
        with open(sql_destination_clusters_file_path, "wt") as sql_destination_clusters_file:
            sql_destination_clusters_file.write("-- NO DATA WERE FOUND")

    if not first_lines_were_written[1]:
        # remove file contents as no data were found
        with open(sql_destination_cluster_parameters_file_path, "wt") as sql_destination_cluster_parameters_file:
            sql_destination_cluster_parameters_file.write("-- NO DATA WERE FOUND")


if __name__ == '__main__':
    main()
