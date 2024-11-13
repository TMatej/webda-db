from common.InsertLineBuilderBase import InsertLineBuilderBase


class ClusterParameters(InsertLineBuilderBase):
    def __init__(self,
                 folder_name = None,
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
        sanitized_folder_name = self.__sanitize_string_value__(self.folder_name)
        sanitized_right_ascension = self.__sanitize_string_value__(self.right_ascension)
        sanitized_declination = self.__sanitize_string_value__(self.declination)
        sanitized_longitude = self.__sanitize_numeric_value__(self.longitude)
        sanitized_latitude = self.__sanitize_numeric_value__(self.latitude)
        sanitized_angular_diameter = self.__sanitize_numeric_value__(self.angular_diameter)
        sanitized_distance = self.__sanitize_numeric_value__(self.distance)
        sanitized_ebv = self.__sanitize_numeric_value__(self.ebv)
        sanitized_log_age = self.__sanitize_numeric_value__(self.log_age)
        sanitized_feh = self.__sanitize_string_value__(self.feh)
        sanitized_radial_velocity = self.__sanitize_string_value__(self.radial_velocity)
        sanitized_proper_motion_ra = self.__sanitize_numeric_value__(self.proper_motion_ra)
        sanitized_proper_motion_dec = self.__sanitize_numeric_value__(self.proper_motion_dec)

        return f"({sanitized_folder_name}, {sanitized_right_ascension}, {sanitized_declination}, {sanitized_longitude}, {sanitized_latitude}, {sanitized_angular_diameter}, {sanitized_distance}, {sanitized_ebv}, {sanitized_log_age}, {sanitized_feh}, {sanitized_radial_velocity}, {sanitized_proper_motion_ra}, {sanitized_proper_motion_dec})"

    @staticmethod
    def get_table_parameters() -> str:
        return "Foldername, RightAscension, Declination, Longitude, Latitude, AngularDiameter, Distance, Ebv, LogAge, Feh, RadialVelocity, ProperMotionRa, ProperMotionDec"