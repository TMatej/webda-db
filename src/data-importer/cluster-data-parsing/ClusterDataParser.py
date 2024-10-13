import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import CLUSTERS_TABLE_NAME, CLUSTERS_DATA_FOLDER_NAME, SQL_CLUSTERS_FILE_NAME, \
    CLUSTER_PARAMETERS_FILE
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_CLUSTERS_FOLDER_PATH
from Cluster import Cluster

def check_standard(open_file):
    first_data_line = open_file.readline().strip()

    if "Description\tParam" not in first_data_line:
        raise ValueError("First data line does not start with 'Description'")

    second_data_line = open_file.readline().strip()

    if "-----------\t-----" not in second_data_line:
        raise ValueError("Second data line does not start with 'Description'")

    open_file.seek(0)

    return open_file

def process_cluster(lines: [str], folder_name: str) -> Cluster:
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
        pair = line.split("\t", 1)

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
    return Cluster(iau_cluster_number, stripped_folder_name, name, right_ascension, declination, longitude, latitude, angular_diameter, distance, ebv, log_age, feh, radial_velocity, proper_motion_ra, proper_motion_dec)

def process_single_file(original_file_path, sql_stripped_data_file_path, folder_name):
    with open(sql_stripped_data_file_path, "wt") as sql_stripped_data_file:
        with open(original_file_path, 'rt') as references_file:
            # standardize file as they occur in two formats
            check_standard(references_file)

            # create and write INSERT part of SQL insert command
            write_sql_insert_statement(sql_stripped_data_file, CLUSTERS_TABLE_NAME, Cluster.get_table_parameters())

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(sql_stripped_data_file)

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
            cluster = process_cluster(lines, folder_name)

            # create and write VALUES part of SQL insert command
            write_sql_values_data_statement(sql_stripped_data_file, cluster)

            # write finish of the insert command to file
            sql_stripped_data_file.write(";\n")

def main():
    destination_folder = os.path.join(DESTINATION_DATA_FOLDER_PATH, CLUSTERS_DATA_FOLDER_NAME)
    folder_content = os.listdir(ORIGIN_CLUSTERS_FOLDER_PATH)

    folder_names = [f for f in folder_content if os.path.isdir(os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, f))]
    folder_names.sort()

    for folder_name in folder_names:
        cluster_parameters_file_path = os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, folder_name, CLUSTER_PARAMETERS_FILE)

        # create folders on path
        cluster_sql_folder_path = os.path.join(destination_folder, folder_name)
        if not os.path.exists(cluster_sql_folder_path):
            os.mkdir(cluster_sql_folder_path)

        # destination sql file
        sql_stripped_data_file_path = os.path.join(cluster_sql_folder_path, SQL_CLUSTERS_FILE_NAME)

        # if dias.dat does not exist for the cluster create empty record and skip to next
        if not os.path.exists(cluster_parameters_file_path):
            print(cluster_parameters_file_path)

            with open(sql_stripped_data_file_path, "wt") as sql_stripped_data_file:
                cluster = process_cluster(["\t".join(["cluster name", folder_name])], folder_name)

                write_sql_insert_statement(sql_stripped_data_file, CLUSTERS_TABLE_NAME, Cluster.get_table_parameters())
                write_sql_values_keyword_statement(sql_stripped_data_file)

                # create and write VALUES part of SQL insert command
                values_data = cluster.build_insert_values_line()
                sql_stripped_data_file.write(values_data)

                # write finish of the insert command to file
                sql_stripped_data_file.write(";\n")
            continue

        try:
            process_single_file(cluster_parameters_file_path, sql_stripped_data_file_path, folder_name)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    main()