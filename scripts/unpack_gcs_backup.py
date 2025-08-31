import json
from pathlib import Path
import sys
from tqdm import tqdm
from src.transform import (
    P4HourData2025Transformer,
    Transformer,
    DailyUsageDataTransformer,
    P4QuarterData2024Transformer,
    P4QuarterData2025Transformer,
)


TRANSFORMERS: list[Transformer] = [
    DailyUsageDataTransformer(),
    P4QuarterData2024Transformer(),
    P4HourData2025Transformer(),
    P4QuarterData2025Transformer(),
]


def store(records: list[dict], out_path: str):
    with open(out_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")


def parse_file(file_name: str) -> None:
    for transformer in TRANSFORMERS:
        if transformer.applies(file_name.name):
            transformed = transformer.transform(file_name)
            store(transformed, transformer.output_path / file_name.name)
            return
    print(f"No transformer found for {file_name}")


def parse_all(root_path: str):
    root = Path(root_path)

    for folder_path in tqdm(root.iterdir(), desc="Unpacking GCS backup files"):
        if folder_path.is_file():
            parse_file(folder_path)


if __name__ == "__main__":
    print("Unpacking GCS backup files from /gcs")
    root = "./gcs"
    if len(sys.argv) > 1:
        root = sys.argv[1]
    parse_all(root)
