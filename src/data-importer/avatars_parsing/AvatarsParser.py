import os

from avatars_parsing.Avatar import Avatar
from avatars_parsing.Dimension import Dimension
from common.constants import ERROR_OUTPUT_FILE_NAME, BUFFER_SIZE, \
    DATA_DESTINATION_FOLDER_NAME, CLUSTERS_DATA_FOLDER_NAME, \
    SQL_AVATARS_FILE_NAME, CLUSTERS_DSS_FOLDER_NAME
from common.folder_paths import DESTINATION_FOLDER_PATH, ORIGIN_FOLDER_PATH

def process_dimension_data(dimensions_file_path: str) -> dict[str, Dimension] | None:
    # return if file does not exist
    if not os.path.exists(dimensions_file_path):
        print(f"FILE NOT FOUND: '{dimensions_file_path}'.")
        return

    # check the format
    # result = check_standard(isochrones_origin_file_path, error_output_destination_file_path)
    # if not result:
    #     return

    with open(dimensions_file_path, "rt") as dimensions_file:
        print(f"Processing file content: {dimensions_file_path}")

        dimensions_dict: dict = dict()
        while True:
            # process record
            record_line = dimensions_file.readline()

            if not record_line:
                break

            folder_name, dimension = record_line.split("\t")
            dimension = Dimension(folder_name, dimension)
            dimensions_dict[folder_name] = dimension

    return dimensions_dict

def main():
    # check folder path existence and create folder on path if it does not exist yet
    clusters_destination_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, CLUSTERS_DATA_FOLDER_NAME)
    if not os.path.exists(clusters_destination_folder_path):
        os.makedirs(clusters_destination_folder_path, exist_ok=True)

    error_output_file_name = "avatars-" + ERROR_OUTPUT_FILE_NAME

    # error destination file
    error_output_destination_file_path: str = os.path.join(clusters_destination_folder_path, error_output_file_name)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    # folder contents
    dimensions_file_name:str = "ocl_diam.asc"
    spicy_star:str = "net01.png"
    clusters_dss_folder_path: str = os.path.join(ORIGIN_FOLDER_PATH, CLUSTERS_DSS_FOLDER_NAME)
    clusters_dss_folder_content = os.listdir(clusters_dss_folder_path)
    avatar_file_names = [f for f in clusters_dss_folder_content if
                            os.path.isfile(os.path.join(clusters_dss_folder_path, f))
                         and not f.__eq__(dimensions_file_name)
                         and not f.__eq__(spicy_star)]

    # destination sql file path
    avatars_sql_file_path: str = os.path.join(clusters_destination_folder_path, SQL_AVATARS_FILE_NAME)

    # process file with dimension data
    dimensions_file_path = os.path.join(clusters_dss_folder_path, dimensions_file_name)
    dimensions_records: dict[str, Dimension] = process_dimension_data(dimensions_file_path)

    if not dimensions_records:
        return

    with open(avatars_sql_file_path, "wt", buffering=BUFFER_SIZE) as avatars_sql_file:
        for avatar_file_name in avatar_file_names:
            folder_name = os.path.splitext(avatar_file_name)[0] # cluster name
            dimension = dimensions_records[folder_name]
            avatar_file_path = os.path.join(clusters_dss_folder_path, avatar_file_name)
            avatar = Avatar(folder_name, avatar_file_path, dimension.picture_dimension)

            # insert new conditional update command
            update_command = avatar.build_update_line()

            avatars_sql_file.write(update_command)

if __name__ == '__main__':
    main()