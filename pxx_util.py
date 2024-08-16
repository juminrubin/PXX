
import os

class AppUtil:
    def __init__(self, year: int):
        self.year = year
        self.connection = None

    def get_fillings_table_name(self) -> str:
        return f'filings_{self.year}'

    def get_fillings_folder_path(self) -> str:
        return os.path.join('filings', f'{self.year}')

    def get_blocks_folder_path(self) -> str:
        return os.path.join('blocks', f'{self.year}')