import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import PUBLICATIONS_TABLE_NAME, SQL_FILE_SUFFIX, PUBLICATION_FILE_SUFFIX, PUBLICATIONS_DATA_FOLDER_NAME
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_PUBLICATIONS_FOLDER_PATH
from Publication import Publication

def standardize(open_file):
    data_line = open_file.readline().strip()

    # without header
    if data_line == "":
        return open_file

    # with header
    if data_line.startswith("Dummy"):
        _ = open_file.readline()
        return open_file

    # unknown format -> raise error
    raise ValueError("Unknown file format.")

def process_publication_record(file_name, lines: [str]) -> Publication:
    ref = None
    author = None
    journal = None
    title = None
    bibcode = None
    year = None
    data = None

    for line in lines:
        pair = line.split("\t", 1)

        if len(pair) == 2:
            key, value = pair
        else:
            continue

        stripped_value = value.strip()

        match key.lower():
            case "ref":
                ref = stripped_value
            case "author":
                author = stripped_value
            case "journal":
                journal = stripped_value
            case "title":
                title = stripped_value
            case "bibcode":
                bibcode = stripped_value
            case "year":
                year = stripped_value
            case "data":
                data = stripped_value

    return Publication(file_name, ref, author, journal, title, bibcode, year, data)

def process_single_file(filename, references_file_path, sql_stripped_data_file_path):
    with open(sql_stripped_data_file_path, "wt") as sql_stripped_data_file:
        with open(references_file_path, 'rt') as references_file:
            print(f"Printing file content: {references_file_path}")

            # standardize file as they occur in two formats
            standardize(references_file)

            # create and write INSERT part of SQL insert command
            write_sql_insert_statement(sql_stripped_data_file, PUBLICATIONS_TABLE_NAME, Publication.get_table_parameters())

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(sql_stripped_data_file)

            first_line_was_written = False

            # process each record in the file
            while True:
                line = references_file.readline()
                if not line:
                    break

                # collect record properties
                lines = []
                while line and line != "\n":
                    lines.append(line)
                    line = references_file.readline()

                # process record
                publication = process_publication_record(filename, lines)

                if publication.is_empty():
                    continue

                if first_line_was_written:
                    # write insert command to file
                    sql_stripped_data_file.write(",\n")

                first_line_was_written = True

                # create and write VALUES part of SQL insert command
                write_sql_values_data_statement(sql_stripped_data_file, publication)

            # write insert command to file
            sql_stripped_data_file.write(";\n")

def main():
    destination_folder = os.path.join(DESTINATION_DATA_FOLDER_PATH, PUBLICATIONS_DATA_FOLDER_NAME)
    folder_content = os.listdir(ORIGIN_PUBLICATIONS_FOLDER_PATH)

    file_names = [f for f in folder_content if os.path.isfile(os.path.join(ORIGIN_PUBLICATIONS_FOLDER_PATH, f)) and f.endswith(PUBLICATION_FILE_SUFFIX)]
    file_names.sort()

    for file in file_names:
        references_file_path = os.path.join(ORIGIN_PUBLICATIONS_FOLDER_PATH, file)
        sql_file_name = file.replace(PUBLICATION_FILE_SUFFIX, SQL_FILE_SUFFIX)
        sql_stripped_data_file_path = os.path.join(destination_folder, sql_file_name)

        process_single_file(file, references_file_path, sql_stripped_data_file_path)

if __name__ == '__main__':
    main()