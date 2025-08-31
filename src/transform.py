import json
from pathlib import Path
from datetime import datetime
from uuid import UUID


class Transformer:
    filename_prefix: str = ""

    def __init__(self, root_path: str = "extracted"):
        self.output_path = Path(root_path) / self.filename_prefix
        self.output_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def applies(cls, filename: str) -> bool:
        return filename.startswith(cls.filename_prefix)

    def transform(self, filename: str) -> list[dict]:
        records = json.load(open(filename, "r"))
        return [self._transform(record) for record in records]

    @staticmethod
    def _transform(record: dict) -> dict:
        raise NotImplementedError

    @staticmethod
    def is_valid_uuid(uuid_to_test):
        try:
            uuid_obj = UUID(uuid_to_test)
        except ValueError:
            return False
        return str(uuid_obj) == uuid_to_test


class P4HourData2025Transformer(Transformer):
    filename_prefix: str = "p4_hour_data_2025"

    @staticmethod
    def _transform(record: dict) -> dict:
        return {
            "date": record.get("query_date"),
            "type": record.get("type"),
            "meter_ean": record.get("meter_ean"),
        } | {f"measurement_h_{i}": record.get(f"measurement_h_{i}") for i in range(25)}


class P4QuarterData2024Transformer(Transformer):
    filename_prefix: str = "p4_hour_data_2024"

    @staticmethod
    def _transform(record: dict) -> dict:
        if "datetime" not in record:
            print(f"Incorrect data format, missing datetime field")
            return None

        local_time = datetime.fromisoformat(record["datetime"])
        houseid = record["houseID"]
        if not Transformer.is_valid_uuid(houseid):
            record["house_id"] = houseid
        else:
            record["household_id"] = houseid

        return {
            "household_id": record.get("household_id"),
            "house_id": record.get("house_id"),
            "datetime": record["datetime"],
            "date": local_time.date().isoformat(),
            "time": local_time.time().isoformat(),
            "backfeed": record["backfeedMeasurement"]["meter"]
            if "backfeedMeasurement" in record
            else None,
            "electricity": record["electricityMeasurement"]["meter"]
            if "electricityMeasurement" in record
            else None,
            "gas": record["gasMeasurement"]["meter"]
            if "gasMeasurement" in record
            else None,
        }


class P4QuarterData2025Transformer(Transformer):
    filename_prefix: str = "p4_hour_data__migration_2025"

    @staticmethod
    def _transform(record: dict) -> dict:
        if "datetime" not in record:
            print(f"Incorrect data format, missing datetime field")
            return None

        local_time = datetime.fromisoformat(record["datetime"])
        houseid = record.get("houseID")
        assert Transformer.is_valid_uuid(houseid)

        return {
            "household_id": houseid,
            "datetime": record["datetime"],
            "date": local_time.date().isoformat(),
            "time": local_time.time().isoformat(),
            "backfeed": record["backfeedMeasurement"]["meter"]
            if "backfeedMeasurement" in record
            else None,
            "electricity": record["electricityMeasurement"]["meter"]
            if "electricityMeasurement" in record
            else None,
            "gas": record["gasMeasurement"]["meter"]
            if "gasMeasurement" in record
            else None,
        }


class DailyUsageDataTransformer(Transformer):
    filename_prefix: str = "daily_usage_data"

    @staticmethod
    def _transform(record: dict) -> dict:
        return {
            "household_id": record["household_id"],
            "activation_code": record["household_activation_code"],
            "date": record["date"],
            "type": record["type"],
            "usage": record["usage"],
        }
