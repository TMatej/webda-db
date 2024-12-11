from common.constants import IDAM_IDS_FILE, SQL_STAR_ALIASES_IDS_DATA_FILE_NAME
from stars_alias_data_parsing.StarAliasDataParserBase import process


def main():
    process("ids", IDAM_IDS_FILE, SQL_STAR_ALIASES_IDS_DATA_FILE_NAME, ["mult", "ang", "sep", "ma", "mb"])

if __name__ == '__main__':
    main()