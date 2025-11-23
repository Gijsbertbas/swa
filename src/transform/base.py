import json
from uuid import UUID

from tqdm import tqdm

from util import ETLConfig


class Transformer:

    def __init__(self, config: ETLConfig):
        self.config = config
        self.config.transformation_folder.mkdir(parents=True, exist_ok=True)

    def applies(self, filename: str) -> bool:
        return filename.startswith(self.config.filename_prefix)

    def transform(self, filename: str) -> list[dict]:
        records = json.load(open(filename, "r"))
        return [self._transform(record) for record in records]
    
    def transform_all(self):
        print(f'Transforming files in {self.config.extraction_folder}')
        for file in tqdm(self.config.extraction_folder.iterdir(), desc="Transforming"):
            if file.is_file():
                if not self.applies(file.name):
                    print(f'[ERROR] Transformer is not fit for {file}')
                    continue
                transformed = self.transform(file)
                self.store(transformed, file.name)
    
    def store(self, records: list[dict], filename: str) -> None:
        output_path = self.config.transformation_folder / filename
        with open(output_path, 'w') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')

    def _transform(self, record: dict) -> dict:
        raise NotImplementedError

    @staticmethod
    def is_valid_uuid(uuid_to_test):
        try:
            uuid_obj = UUID(uuid_to_test)
        except ValueError:
            return False
        return str(uuid_obj) == uuid_to_test