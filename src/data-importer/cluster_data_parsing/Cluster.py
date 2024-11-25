from common.InsertLineBuilderBase import InsertLineBuilderBase


class Cluster(InsertLineBuilderBase):
    def __init__(self,
                 iau_cluster_number = None,
                 folder_name = None,            # cluster name (abbreviation)
                 name = None):
        self.iau_cluster_number = iau_cluster_number
        self.folder_name = folder_name
        self.name = name

    def is_empty(self):
        return (self.iau_cluster_number is None and
                self.folder_name is None and
                self.name is None)

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = self.__sanitize_string_value__(self.folder_name)
        sanitized_iau_cluster_number = self.__sanitize_string_value__(self.iau_cluster_number)
        sanitized_name = self.__sanitize_string_value__(self.name)

        return f"({sanitized_folder_name}, {sanitized_iau_cluster_number}, {sanitized_name})"

    @staticmethod
    def get_table_parameters() -> str:
        return "Foldername, IauClusterNumber, Name"