from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.constants import SQL_FILE_NAME_COLUMN_NAME, SQL_LOGT_COLUMN_NAME, SQL_MV_COLUMN_NAME, SQL_MBV_COLUMN_NAME, \
    SQL_MUB_COLUMN_NAME, SQL_MVR_COLUMN_NAME, SQL_MVI_COLUMN_NAME, SQL_MRI_COLUMN_NAME, SQL_ISOCHRONE_TYPE_COLUMN_NAME


class Isochrone(InsertLineBuilderBase):
    def __init__(
            self,
            file_name: str = None,
            isochrone_type: str = None,
            logt: str = None,
            mv: str = None,
            mbv: str = None,
            mub: str = None,
            mvr: str = None,
            mvi: str = None,
            mri: str = None):
        self.file_name = file_name
        self.isochrone_type = isochrone_type
        self.logt = logt
        self.mv = mv
        self.mbv = mbv
        self.mub = mub
        self.mvr = mvr
        self.mvi = mvi
        self.mri = mri

    def build_insert_values_line(self) -> str:
        sanitized_file_name = self.__sanitize_string_value__(self.file_name)
        sanitized_isochrone_type = self.__sanitize_string_value__(self.isochrone_type)
        sanitized_logt = self.__sanitize_numeric_value__(self.logt)
        sanitized_mv = self.__sanitize_numeric_value__(self.mv)
        sanitized_mbv = self.__sanitize_numeric_value__(self.mbv)
        sanitized_mub = self.__sanitize_numeric_value__(self.mub)
        sanitized_mvr = self.__sanitize_numeric_value__(self.mvr)
        sanitized_mvi = self.__sanitize_numeric_value__(self.mvi)
        sanitized_mri = self.__sanitize_numeric_value__(self.mri)

        return f"({sanitized_file_name}, {sanitized_isochrone_type}, {sanitized_logt}, {sanitized_mv}, {sanitized_mbv}, {sanitized_mub}, {sanitized_mvr}, {sanitized_mvi}, {sanitized_mri})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_FILE_NAME_COLUMN_NAME}, {SQL_ISOCHRONE_TYPE_COLUMN_NAME}, {SQL_LOGT_COLUMN_NAME}, {SQL_MV_COLUMN_NAME}, {SQL_MBV_COLUMN_NAME}, {SQL_MUB_COLUMN_NAME}, {SQL_MVR_COLUMN_NAME}, {SQL_MVI_COLUMN_NAME}, {SQL_MRI_COLUMN_NAME}"
