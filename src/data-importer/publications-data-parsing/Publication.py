from common.InsertLineBuilderBase import InsertLineBuilderBase


class Publication(InsertLineBuilderBase):
    def __init__(self, file_name = None, ref = None, author = None, journal = None, title = None, bibcode = None, year = None, data = None):
        self.file_name = file_name
        self.ref = ref
        self.author = author
        self.journal = journal
        self.title = title
        self.bibcode = bibcode
        self.year = year
        self.data = data

    def is_empty(self):
        return (self.file_name is None and
            self.ref is None and
            self.author is None and
            self.journal is None and
            self.title is None and
            self.bibcode is None and
            self.year is None and
            self.data is None)

    def build_insert_values_line(self) -> str:
        sanitized_file_name = "'" + self.file_name.lower().replace("'", "''") + "'"
        sanitized_ref = "'" + self.file_name.replace("'", "''") + "'"
        sanitized_title = "'" + self.file_name.replace("'", "''") + "'"
        sanitized_author = "'" + self.file_name.replace("'", "''") + "'"
        sanitized_journal = "'" + self.file_name.replace("'", "''") + "'"
        sanitized_year = self.file_name.replace("'", "''")
        sanitized_bibcode = "'" + self.file_name.replace("'", "''") + "'"

        return f"({sanitized_file_name}, {sanitized_ref}, {sanitized_title}, {sanitized_author}, {sanitized_journal}, {sanitized_year}, {sanitized_bibcode})"

    @staticmethod
    def get_table_parameters() -> str:
        return "filename, ref, title, author, journal, year, bibcode"