from common.InsertLineBuilderBase import InsertLineBuilderBase


class Cluster(InsertLineBuilderBase):
    def __init__(self,
                 iau_cluster_number = None,
                 folder_name = None,
                 name = None,
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
        self.iau_cluster_number = iau_cluster_number
        self.name = name
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
        return (self.iau_cluster_number is None and
                self.name is None and
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
        sanitized_iau_cluster_number = self.iau_cluster_number.replace("'", "''")
        sanitized_folder_name = self.folder_name.lower().replace("'", "''")
        sanitized_name = self.name.replace("'", "''")
        sanitized_right_ascension = self.right_ascension.replace("'", "''")
        sanitized_declination = self.declination.replace("'", "''")
        sanitized_longitude = self.longitude.replace("'", "''")
        sanitized_latitude = self.latitude.replace("'", "''")
        sanitized_angular_diameter = self.angular_diameter.replace("'", "''")
        sanitized_distance = self.distance.replace("'", "''")
        sanitized_ebv = self.ebv.replace("'", "''")
        sanitized_log_age = self.log_age.replace("'", "''")
        sanitized_feh = self.feh.replace("'", "''")
        sanitized_radial_velocity = self.radial_velocity.replace("'", "''")
        sanitized_proper_motion_ra = self.proper_motion_ra.replace("'", "''")
        sanitized_proper_motion_dec = self.proper_motion_dec.replace("'", "''")

        return f"('{sanitized_iau_cluster_number}', '{sanitized_folder_name}', '{sanitized_name}', '{sanitized_right_ascension}', '{sanitized_declination}', {sanitized_longitude}, {sanitized_latitude}, {sanitized_angular_diameter}, {sanitized_distance}, {sanitized_ebv}, {sanitized_log_age}, '{sanitized_feh}', '{sanitized_radial_velocity}', {sanitized_proper_motion_ra}, {sanitized_proper_motion_dec})"

    @staticmethod
    def get_table_parameters() -> str:
        return "iau_cluster_number, folder_name,  name, right_ascension, declination, longitude, latitude, angular_diameter, distance, ebv, log_age, feh, radial_velocity, proper_motion_ra, proper_motion_dec"