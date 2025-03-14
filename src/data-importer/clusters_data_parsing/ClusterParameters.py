from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.Sanitizer import Sanitizer
from common.constants import SQL_FOLDER_NAME_COLUMN_NAME, SQL_RIGHT_ASCENSION_COLUMN_NAME, SQL_DECLINATION_COLUMN_NAME, \
    SQL_PROPER_MOTION_DEC_COLUMN_NAME, SQL_PROPER_MOTION_RA_COLUMN_NAME, SQL_RADIAL_VELOCITY_COLUMN_NAME, \
    SQL_FEH_COLUMN_NAME, SQL_LOGAGE_COLUMN_NAME, SQL_EBV_COLUMN_NAME, SQL_DISTANCE_COLUMN_NAME, \
    SQL_ANGULAR_DIAMETER_COLUMN_NAME, SQL_LATITUDE_COLUMN_NAME, SQL_LONGITUDE_COLUMN_NAME, SQL_CLUSTER_ID_COLUMN_NAME


class ClusterParameters(InsertLineBuilderBase):
    def __init__(self,
                 folder_name = None,    # cluster name (abbreviation)
                 right_ascension = None,
                 declination = None,
                 longitude = None,
                 latitude = None,
                 angular_diameter = None,
                 distance = None,
                 ebv = None,
                 log_age = None,
                 feh = None,
                 radial_velocity = None,
                 proper_motion_ra = None,
                 proper_motion_dec = None):
        self.folder_name = folder_name
        self.right_ascension = right_ascension
        self.declination = declination
        self.longitude = longitude
        self.latitude = latitude
        self.angular_diameter = angular_diameter
        self.distance = distance
        self.ebv = ebv
        self.log_age = log_age
        self.feh = feh
        self.radial_velocity = radial_velocity
        self.proper_motion_ra = proper_motion_ra
        self.proper_motion_dec = proper_motion_dec

    def is_empty(self):
        return (self.folder_name is None and
                self.right_ascension is None and
                self.declination is None and
                self.longitude is None and
                self.latitude is None and
                self.angular_diameter is None and
                self.distance is None and
                self.ebv is None and
                self.log_age is None and
                self.feh is None and
                self.radial_velocity is None and
                self.proper_motion_ra is None and
                self.proper_motion_dec is None)

    def build_insert_values_line(self) -> str:
        sanitized_folder_name = Sanitizer.__sanitize_string_value__(self.folder_name)
        sanitized_right_ascension = Sanitizer.__sanitize_string_value__(self.right_ascension)
        sanitized_declination = Sanitizer.__sanitize_string_value__(self.declination)
        sanitized_longitude = Sanitizer.__sanitize_numeric_value__(self.longitude)
        sanitized_latitude = Sanitizer.__sanitize_numeric_value__(self.latitude)
        sanitized_angular_diameter = Sanitizer.__sanitize_numeric_value__(self.angular_diameter)
        sanitized_distance = Sanitizer.__sanitize_numeric_value__(self.distance)
        sanitized_ebv = Sanitizer.__sanitize_numeric_value__(self.ebv)
        sanitized_log_age = Sanitizer.__sanitize_numeric_value__(self.log_age)
        sanitized_feh = Sanitizer.__sanitize_numeric_value__(self.feh)
        sanitized_radial_velocity = Sanitizer.__sanitize_numeric_value__(self.radial_velocity)
        sanitized_proper_motion_ra = Sanitizer.__sanitize_numeric_value__(self.proper_motion_ra)
        sanitized_proper_motion_dec = Sanitizer.__sanitize_numeric_value__(self.proper_motion_dec)

        return f"(0, {sanitized_folder_name}, {sanitized_right_ascension}, {sanitized_declination}, {sanitized_longitude}, {sanitized_latitude}, {sanitized_angular_diameter}, {sanitized_distance}, {sanitized_ebv}, {sanitized_log_age}, {sanitized_feh}, {sanitized_radial_velocity}, {sanitized_proper_motion_ra}, {sanitized_proper_motion_dec})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_CLUSTER_ID_COLUMN_NAME}, {SQL_FOLDER_NAME_COLUMN_NAME}, {SQL_RIGHT_ASCENSION_COLUMN_NAME}, {SQL_DECLINATION_COLUMN_NAME}, {SQL_LONGITUDE_COLUMN_NAME}, {SQL_LATITUDE_COLUMN_NAME}, {SQL_ANGULAR_DIAMETER_COLUMN_NAME}, {SQL_DISTANCE_COLUMN_NAME}, {SQL_EBV_COLUMN_NAME}, {SQL_LOGAGE_COLUMN_NAME}, {SQL_FEH_COLUMN_NAME}, {SQL_RADIAL_VELOCITY_COLUMN_NAME}, {SQL_PROPER_MOTION_RA_COLUMN_NAME}, {SQL_PROPER_MOTION_DEC_COLUMN_NAME}"