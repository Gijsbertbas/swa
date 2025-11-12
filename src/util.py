from dataclasses import dataclass
from enum import Enum
import os
from pathlib import Path, PosixPath
from typing import Any


class DataType(Enum):
    DAILY_USAGE = "daily_usage"
    P4_HOUR_2025 = "p4_hour_2025"
    P4_QUARTER_2025 = "p4_quarter_2025"
    P4_QUARTER_2024 = "p4_quarter_2024"


@dataclass
class ETLConfig:
    type: DataType
    filename_prefix: str
    transformer: Any
    root_extraction_folder: PosixPath = Path("extracted")
    root_transformation_folder: PosixPath = Path("transformed")

    @property
    def s3_folder(self) -> str:
        return os.path.join('gcs', self.type.value, '')

    @property
    def s3_prefix(self) -> str:
        return os.path.join(self.s3_folder, self.filename_prefix)
    
    @property
    def extraction_folder(self) -> PosixPath:
        return self.root_extraction_folder / self.type.value
  
    @property
    def transformation_folder(self) -> PosixPath:
        return self.root_transformation_folder / self.type.value
  