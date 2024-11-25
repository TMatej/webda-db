# common file suffixes
PUBLICATION_FILE_SUFFIX: str = ".ref"
SQL_FILE_SUFFIX: str = ".sql"

# destination data folder names
CLUSTERS_DATA_FOLDER_NAME: str = "clusters"
STARS_DATA_FOLDER_NAME: str = "stars"
STAR_ALIASES_DATA_FOLDER_NAME: str = "star-aliases"
PUBLICATION_REFERENCES_DATA_FOLDER_NAME: str = "publication-references"
DATA_TYPES_DATA_FOLDER_NAME: str = "data-types"
DATA_RECORDS_DATA_FOLDER_NAME: str = "data-records"

# original data names
NUMBERING_SYSTEM_FILE_NAME: str = "sysno.ref"
DATA_TYPES_FILE_NAME: str = "types_to_files_to_refs.me"
DATA_FILE_REFERENCE_FILE_NAME: str = "data_types_file_names.list"
CLUSTER_PARAMETERS_FILE_NAME: str = "dias.dat"

# original data file names
TRANS_TAB_FILE = "trans.tab"
TRANS_REF_FILE = "trans.ref"

# generic name for files containing error outputs
ERROR_OUTPUT_FILE_NAME: str = "errors.log"

# destination data names
DATA_TYPES_PROCESSED_NAMES_FILE_NAME: str = "types-processed.txt"

# sql file names
SQL_CLUSTERS_FILE_NAME: str = "clusters.sql"
SQL_CLUSTER_PARAMETERS_FILE_NAME: str = "cluster-parameters.sql"
SQL_STARS_FILE_NAME: str = "stars.sql"
SQL_STAR_ALIASES_FILE_NAME: str = "star-aliases.sql"
SQL_DATA_TYPES_FILE_NAME: str = "data-types.sql"
SQL_PUBLICATION_REFERENCES_FILE_NAME: str = "publication-references.sql"

# sql
NO_DATA_WERE_FOUND_SQL_COMMENT: str = "-- NO DATA WERE FOUND"

# table names
CLUSTERS_TABLE_NAME: str = "Clusters"
CLUSTER_PARAMETERS_TABLE_NAME: str = "ClusterParameters"
STARS_TABLE_NAME: str = "Stars"
STAR_ALIASES_TABLE_NAME: str = "StarAliases"
PUBLICATION_REFERENCES_TABLE_NAME: str = "PublicationReferences"
DATA_TYPES_TABLE_NAME: str = "DataTypes"

# data records table names
AD2K_TYPE_TABLE_NAME: str = "CoordinatesJ2000"

BUFFER_SIZE = 104857600 # 100MB