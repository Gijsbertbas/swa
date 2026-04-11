import typer
from load.s3 import S3Loader
from src.extract.google import GoogleExtractor
from src.extract.s3 import S3Extractor

from transform.google import DailyUsageDataTransformer, P4HourData2025Transformer, P4QuarterData2024Transformer, P4QuarterData2025Transformer
from util import DataType, ETLConfig


CONFIGS: dict[str, ETLConfig] = {
    "daily_usage": ETLConfig(DataType.DAILY_USAGE, 'daily_usage_data', transformer=DailyUsageDataTransformer),
    "p4_quarter_2024": ETLConfig(DataType.P4_QUARTER_2024, 'p4_hour_data_2024', P4QuarterData2024Transformer),
    "p4_hour_2025": ETLConfig(DataType.P4_HOUR_2025, 'p4_hour_data_2025', P4HourData2025Transformer),
    "p4_quarter_2025": ETLConfig(DataType.P4_QUARTER_2025, 'p4_hour_data__migration_2025', P4QuarterData2025Transformer),
    "household_exceptions": ETLConfig(DataType.HOUSEHOLD_EXCEPTIONS, 'household_exceptions', eligible_steps="L"),
}

BUCKET = 'slimwonen-analysis-data'
PROFILE = 'SA'


def main(extract: bool = False, transform: bool = False, load: bool = False, config_name: str = typer.Option(None, "--config", "-c", help="ETL config to use")):

    if config_name is None:
        options = list(CONFIGS.keys())
        for i, name in enumerate(options):
            print(f"  [{i}] {name}")
        choice = typer.prompt("Select config", default="0")
        try:
            config_name = options[int(choice)]
        except (ValueError, IndexError):
            typer.echo(f"Invalid choice: {choice}", err=True)
            raise typer.Exit(1)

    if config_name not in CONFIGS:
        typer.echo(f"Unknown config '{config_name}'. Choose from: {', '.join(CONFIGS)}", err=True)
        raise typer.Exit(1)

    config = CONFIGS[config_name]

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