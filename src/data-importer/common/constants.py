# common file suffixes
PUBLICATION_FILE_SUFFIX: str = ".ref"
SQL_FILE_SUFFIX: str = ".sql"

# destination data folder names
DATA_DESTINATION_FOLDER_NAME: str = "data"
SCHEMA_DESTINATION_FOLDER_NAME: str = "schema"

ADOPTED_NUMBER_REFERENCES_FOLDER_NAME: str = "adopted-number-references"
CLUSTERS_DATA_FOLDER_NAME: str = "clusters"
DATA_TYPES_DATA_FOLDER_NAME: str = "data-types"
DATA_RECORDS_DATA_FOLDER_NAME: str = "data-records"
DATA_ISOCHRONES_FOLDER_NAME: str = "isochrones"
PUBLICATION_REFERENCES_DATA_FOLDER_NAME: str = "publication-references"
STARS_DATA_FOLDER_NAME: str = "stars"
STAR_ALIASES_DATA_FOLDER_NAME: str = "star-aliases"

# origin folder names
REFERENCES_ORIGIN_FOLDER_NAME: str = "references"
CLUSTERS_ORIGIN_FOLDER_NAME: str = "ocl"
CLUSTERS_DSS_FOLDER_NAME: str = "dss"
ISOCHRONES_ORIGIN_FOLDER_NAME: str = "isochrones"
GENEVA_FOLDER_NAME: str = "geneva"
PADOVA_FOLDER_NAME: str = "padova"


# original data names
NUMBERING_SYSTEM_FILE_NAME: str = "sysno.ref"
DATA_TYPES_FILE_NAME: str = "types_to_files_to_refs.txt"
DATA_FILE_REFERENCE_FILE_NAME: str = "data_types_file_names.txt"
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
SQL_AVATARS_FILE_NAME: str = "avatars.sql"
SQL_ISOCHRONES_FILE_NAME: str = "isochrones.sql"
SQL_PUBLICATION_REFERENCES_FILE_NAME: str = "publication-references.sql"
SQL_STARS_FILE_NAME: str = "stars.sql"
SQL_STAR_ALIASES_FILE_NAME: str = "star-aliases.sql"

#sql column names

# entities
SQL_ABBREVIATION_COLUMN_NAME: str = "abbreviation"
SQL_ADOPTED_NUMBER_COLUMN_NAME: str = "adopted_number"
SQL_ALTERNATIVE_NUMBER_COLUMN_NAME: str = "alternative_number"
SQL_ANGULAR_DIAMETER_COLUMN_NAME: str = "angular_diameter"
SQL_AUTHOR_COLUMN_NAME: str = "author"
SQL_BIBCODE_COLUMN_NAME: str = "bibcode"
SQL_DECLINATION_COLUMN_NAME: str = "declination"
SQL_DESCRIPTION_COLUMN_NAME: str = "description"
SQL_DISTANCE_COLUMN_NAME: str = "distance"
SQL_EBV_COLUMN_NAME: str = "ebv"
SQL_FEH_COLUMN_NAME: str = "feh"
SQL_FILE_NAME_COLUMN_NAME: str = "file_name"
SQL_FOLDER_NAME_COLUMN_NAME: str = "folder_name"
SQL_IAU_CLUSTER_NUMBER_COLUMN_NAME: str = "iau_cluster_number"
SQL_JOURNAL_COLUMN_NAME: str = "journal"
SQL_LATITUDE_COLUMN_NAME: str = "latitude"
SQL_LOGAGE_COLUMN_NAME: str = "log_age"
SQL_LONGITUDE_COLUMN_NAME: str = "longitude"
SQL_NAME_COLUMN_NAME: str = "name"
SQL_AVATAR_COLUMN_NAME: str = "avatar"
SQL_AVATAR_DIMENSIONS_COLUMN_NAME: str = "dimensions"
SQL_PROPER_MOTION_DEC_COLUMN_NAME: str = "proper_motion_dec"
SQL_PROPER_MOTION_RA_COLUMN_NAME: str = "proper_motion_ra"
SQL_RADIAL_VELOCITY_COLUMN_NAME: str = "radial_velocity"
SQL_REF_FILE_NAME_COLUMN_NAME: str = "ref_file_name"
SQL_REF_NUMBER_COLUMN_NAME: str = "ref_number"
SQL_REF_STRING_COLUMN_NAME: str = "ref_string"
SQL_REF_STRING_SQUEEZED_COLUMN_NAME: str = "ref_string_squeezed"
SQL_RIGHT_ASCENSION_COLUMN_NAME: str = "right_ascension"
SQL_TITLE_COLUMN_NAME: str = "title"
SQL_YEAR_COLUMN_NAME: str = "year"

## isochrones
SQL_ISOCHRONE_TYPE_COLUMN_NAME: str = "type"
SQL_LOGT_COLUMN_NAME: str = "log_t"
SQL_MV_COLUMN_NAME: str = "mv"
SQL_MBV_COLUMN_NAME: str = "mbv"
SQL_MUB_COLUMN_NAME: str = "mub"
SQL_MVR_COLUMN_NAME: str = "mvr"
SQL_MVI_COLUMN_NAME: str = "mvi"
SQL_MRI_COLUMN_NAME: str = "mri"

# table names
ADOPTED_NUMBER_REFERENCES_TABLE_NAME: str = "adopted_number_references"
CLUSTERS_TABLE_NAME: str = "clusters"
CLUSTER_PARAMETERS_TABLE_NAME: str = "cluster_parameters"
ISOCHRONES_TABLE_NAME: str = "isochrones"
STARS_TABLE_NAME: str = "stars"
STAR_ALIASES_TABLE_NAME: str = "star_aliases"
PUBLICATION_REFERENCES_TABLE_NAME: str = "publication_references"
DATA_TYPES_TABLE_NAME: str = "data_types"

# data records table names
AD2K_TYPE_TABLE_NAME: str = "CoordinatesJ2000"

BUFFER_SIZE = 104857600 # 100MB