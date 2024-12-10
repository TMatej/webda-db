from common.Sanitizer import Sanitizer
from common.constants import SQL_AVATAR_COLUMN_NAME, SQL_AVATAR_DIMENSIONS_COLUMN_NAME, SQL_FOLDER_NAME_COLUMN_NAME, \
    CLUSTERS_TABLE_NAME


class Avatar:
    def __init__(
            self,
            folder_name: str = None,
            path_to_picture: str = None,
            dimension: str = None):
        self.folder_name = folder_name
        self.path_to_picture = path_to_picture
        self.picture_dimension = dimension

    def build_update_line(self) -> str:
        sanitized_folder_name = Sanitizer.__sanitize_string_value__(self.folder_name)
        sanitized_path_to_picture = Sanitizer.__sanitize_string_value__(self.path_to_picture)
        sanitized_picture_dimension = Sanitizer.__sanitize_numeric_value__(self.picture_dimension)

        return f"UPDATE {CLUSTERS_TABLE_NAME} SET {SQL_AVATAR_DIMENSIONS_COLUMN_NAME} = {sanitized_picture_dimension}, {SQL_AVATAR_COLUMN_NAME} = LOAD_FILE({sanitized_path_to_picture}) WHERE {SQL_FOLDER_NAME_COLUMN_NAME} = {sanitized_folder_name};\n"
