import typer
from load.s3 import S3Loader
from src.extract.google import GoogleExtractor
from src.extract.s3 import S3Extractor

from transform.google import DailyUsageDataTransformer, P4HourData2025Transformer, P4QuarterData2024Transformer, P4QuarterData2025Transformer
from util import DataType, ETLConfig

from typing_extensions import Annotated

  
daily_usage_config = ETLConfig(DataType.DAILY_USAGE, 'daily_usage_data', transformer=DailyUsageDataTransformer)
p4_quarter_2024 = ETLConfig(DataType.P4_QUARTER_2024, 'p4_hour_data_2024', P4QuarterData2024Transformer)
p4_hour_2025 = ETLConfig(DataType.P4_HOUR_2025, 'p4_hour_data_2025', P4HourData2025Transformer)
p4_quarter_2025 = ETLConfig(DataType.P4_QUARTER_2025, 'p4_hour_data__migration_2025', P4QuarterData2025Transformer)
household_exceptions = ETLConfig(DataType.HOUSEHOLD_EXCEPTIONS, 'household_exceptions', eligible_steps="L")

BUCKET = 'slimwonen-analysis-data'
PROFILE = 'SA'


config = household_exceptions


def main(extract: bool = False, transform: bool = False, load: bool = False):

    if extract:
        if "E" not in config.eligible_steps:
            print(f"Extraction step is not eligible for data type {config.type}")
            return

        s3 = S3Extractor(
            aws_profile=PROFILE,
            bucket_name=BUCKET,
            config=config,
        )
        s3.collect_filenames()
        rclone_filter_file = s3.generate_rclone_filter_list()

        google = GoogleExtractor()
        google.rclone_sync('google:inactive-usage-data/', config.extraction_folder, rclone_filter_file)

    if transform:
        if "T" not in config.eligible_steps:
            print(f"Transformation step is not eligible for data type {config.type}")
            return

        transformer = config.transformer(config)
        transformer.transform_all()

    if load:
        if "L" not in config.eligible_steps:
            print(f"Loading step is not eligible for data type {config.type}")
            return

        loader = S3Loader(
            aws_profile=PROFILE,
            bucket_name=BUCKET,
            config=config,
        )
        loader.load_all()


if __name__ == '__main__':
    typer.run(main)