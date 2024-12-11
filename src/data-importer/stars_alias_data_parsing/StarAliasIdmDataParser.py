from common.constants import IDAM_CAT_FILE, SQL_STAR_ALIASES_IDM_DATA_FILE_NAME
from stars_alias_data_parsing.StarAliasDataParserBase import process


def main():
    process("idm", IDAM_CAT_FILE, SQL_STAR_ALIASES_IDM_DATA_FILE_NAME, ["mult"])

if __name__ == '__main__':
    main()