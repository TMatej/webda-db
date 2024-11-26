# common file suffixes
PUBLICATION_FILE_SUFFIX: str = ".ref"
SQL_FILE_SUFFIX: str = ".sql"

# destination data folder names
ADOPTED_NUMBER_REFERENCES_FOLDER_NAME: str = "adopted-number-references"
CLUSTERS_DATA_FOLDER_NAME: str = "clusters"
DATA_TYPES_DATA_FOLDER_NAME: str = "data-types"
DATA_RECORDS_DATA_FOLDER_NAME: str = "data-records"
PUBLICATION_REFERENCES_DATA_FOLDER_NAME: str = "publication-references"
STARS_DATA_FOLDER_NAME: str = "stars"
STAR_ALIASES_DATA_FOLDER_NAME: str = "star-aliases"

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

# sql
NO_DATA_WERE_FOUND_SQL_COMMENT: str = "-- NO DATA WERE FOUND"

# sql file names
SQL_ADOPTED_NUMBER_REFERENCES_FILE_NAME: str = "adopted-number-references.sql"
SQL_CLUSTERS_FILE_NAME: str = "clusters.sql"
SQL_CLUSTER_PARAMETERS_FILE_NAME: str = "cluster-parameters.sql"
SQL_DATA_TYPES_FILE_NAME: str = "data-types.sql"
SQL_PUBLICATION_REFERENCES_FILE_NAME: str = "publication-references.sql"
SQL_STARS_FILE_NAME: str = "stars.sql"
SQL_STAR_ALIASES_FILE_NAME: str = "star-aliases.sql"

#sql column names
SQL_ABBREVIATION_COLUMN_NAME: str = "Abbreviation"
SQL_ADOPTED_NUMBER_COLUMN_NAME: str = "AdoptedNumber"
SQL_ALTERNATIVE_NUMBER_COLUMN_NAME: str = "AlternativeNumber"
SQL_AUTHOR_COLUMN_NAME: str = "Author"
SQL_BIBCODE_COLUMN_NAME: str = "Bibcode"
SQL_FILE_NAME_COLUMN_NAME: str = "FileName"
SQL_FOLDER_NAME_COLUMN_NAME: str = "FolderName"
SQL_IAU_CLUSTER_NUMBER_COLUMN_NAME: str = "IauClusterNumber"
SQL_JOURNAL_COLUMN_NAME: str = "Journal"
SQL_LONG_DESCRIPTION_COLUMN_NAME: str = "LongDescription"
SQL_NAME_COLUMN_NAME: str = "Name"
SQL_REF_FILE_NAME_COLUMN_NAME: str = "RefFileName"
SQL_REF_NUMBER_COLUMN_NAME: str = "RefNumber"
SQL_REF_STRING_COLUMN_NAME: str = "RefString"
SQL_REF_STRING_SQUEEZED_COLUMN_NAME: str = "RefStringSqueezed"
SQL_SHORT_DESCRIPTION_COLUMN_NAME: str = "ShortDescription"
SQL_TITLE_COLUMN_NAME: str = "Title"
SQL_YEAR_COLUMN_NAME: str = "Year"

# table names
ADOPTED_NUMBER_REFERENCES_TABLE_NAME: str = "AdoptedNumberReferences"
CLUSTERS_TABLE_NAME: str = "Clusters"
CLUSTER_PARAMETERS_TABLE_NAME: str = "ClusterParameters"
STARS_TABLE_NAME: str = "Stars"
STAR_ALIASES_TABLE_NAME: str = "StarAliases"
PUBLICATION_REFERENCES_TABLE_NAME: str = "PublicationReferences"
DATA_TYPES_TABLE_NAME: str = "DataTypes"

# data records table names
AD2K_TYPE_TABLE_NAME: str = "CoordinatesJ2000"

BUFFER_SIZE = 104857600 # 100MB