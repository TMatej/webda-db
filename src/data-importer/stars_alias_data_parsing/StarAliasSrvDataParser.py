from common.constants import IDAM_SRV_FILE, SQL_STAR_ALIASES_SRV_DATA_FILE_NAME
from stars_alias_data_parsing.StarAliasDataParserBase import process


def main():
    process("srv", IDAM_SRV_FILE, SQL_STAR_ALIASES_SRV_DATA_FILE_NAME, [])


if __name__ == '__main__':
    main()