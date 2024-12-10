import os

from common.constants import DATA_TYPES_FILE_NAME, DATA_FILE_REFERENCE_FILE_NAME
from common.folder_paths import DESTINATION_DATA_FOLDER_PATH


def parse_value(line: str) -> str:
    return line.split("=>", 1)[1].strip(" \",\n")

def main():
    source_file = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_TYPES_FILE_NAME)
    destination_file = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_FILE_REFERENCE_FILE_NAME)

    with open(source_file, "rt") as source:
        with open(destination_file, "wt") as destination:
            while True:
                line = source.readline()

                if not line:
                    break

                line = source.readline()
                data_file_name = parse_value(line)

                line = source.readline()
                ref_file_name = parse_value(line)

                result = "".join([data_file_name, "\t", ref_file_name, "\n"])

                destination.write(result)

                for _ in range(6):
                    next(source)

                line = source.readline()

                if line.strip() != "},":
                    line = source.readline()

                if line.strip() != "},":
                    raise ValueError("Incorrect format")

if __name__ == '__main__':
    main()