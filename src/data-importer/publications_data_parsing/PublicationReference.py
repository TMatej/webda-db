from common.InsertLineBuilderBase import InsertLineBuilderBase
from common.constants import SQL_REF_FILE_NAME_COLUMN_NAME, SQL_REF_STRING_COLUMN_NAME, SQL_REF_NUMBER_COLUMN_NAME, \
    SQL_BIBCODE_COLUMN_NAME, SQL_TITLE_COLUMN_NAME, SQL_AUTHOR_COLUMN_NAME, SQL_JOURNAL_COLUMN_NAME, \
    SQL_YEAR_COLUMN_NAME


class PublicationReference(InsertLineBuilderBase):
    def __init__(
            self,
            ref_file_name = None,
            ref_number = None,
            author = None,
            journal = None,
            title = None,
            bibcode = None,
            year = None,
            data = None):
        self.ref_file_name = ref_file_name          # data type name ([data_type].ref)
        self.ref_number = ref_number
        self.author = author
        self.journal = journal
        self.title = title
        self.bibcode = bibcode
        self.year = year
        self.data = data

    def is_empty(self):
        return (
                self.ref_file_name is None and
                self.ref_number is None and
                self.author is None and
                self.journal is None and
                self.title is None and
                self.bibcode is None and
                self.year is None and
                self.data is None)

    def build_insert_values_line(self) -> str:
        sanitized_ref_file_name = self.__sanitize_string_value__(self.ref_file_name)
        sanitized_ref_number = self.__sanitize_numeric_value__(self.ref_number)
        sanitized_author = self.__sanitize_string_value__(self.author)
        sanitized_journal = self.__sanitize_string_value__(self.journal)
        sanitized_title = self.__sanitize_string_value__(self.title)
        sanitized_bibcode = self.__sanitize_string_value__(self.bibcode)
        sanitized_year = self.__sanitize_numeric_value__(self.year)
        # parameter data are not to be stored in db

        return f"({sanitized_ref_file_name}, {sanitized_ref_number}, {sanitized_title}, {sanitized_author}, {sanitized_journal}, {sanitized_year}, {sanitized_bibcode})"

    @staticmethod
    def get_table_parameters() -> str:
        return f"{SQL_REF_FILE_NAME_COLUMN_NAME}, {SQL_REF_NUMBER_COLUMN_NAME}, {SQL_TITLE_COLUMN_NAME}, {SQL_AUTHOR_COLUMN_NAME}, {SQL_JOURNAL_COLUMN_NAME}, {SQL_YEAR_COLUMN_NAME}, {SQL_BIBCODE_COLUMN_NAME}"