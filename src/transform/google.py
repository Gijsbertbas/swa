from datetime import datetime

from transform.base import Transformer


class P4HourData2025Transformer(Transformer):

    def _transform(self, record: dict) -> dict:
        return {
            "date": record.get("query_date"),
            "type": record.get("type"),
            "meter_ean": record.get("meter_ean"),
        } | {f"measurement_h_{i}": record.get(f"measurement_h_{i}") for i in range(25)}


class P4QuarterData2024Transformer(Transformer):

    def _transform(self, record: dict) -> dict:
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

    def _transform(self, record: dict) -> dict:
        if "datetime" not in record:
            print(f"Incorrect data format, missing datetime field")
            return None

        local_time = datetime.fromisoformat(record["datetime"])
        houseid = record.get("houseID")
        assert self.is_valid_uuid(houseid)

        backfeed, electricity, gas = self._parse_measurements(record)

        return {
            "household_id": houseid,
            "datetime": record["datetime"],
            "date": local_time.date().isoformat(),
            "time": local_time.time().isoformat(),
            "backfeed": backfeed,
            "electricity": electricity,
            "gas": gas,
        }

    @staticmethod
    def _parse_measurements(record: dict) -> tuple:
        backfeed = record["backfeedMeasurement"]["meter"] if "backfeedMeasurement" in record else None
        electricity = record.get("electricityMeasurement")

        if electricity is None:
            return backfeed, None, None

        match electricity['unit']:
            case 'WH':
                return backfeed, electricity['meter'], None
            case 'MTQ':
                return None, None, electricity['meter']
            case _:
                raise RuntimeError(f"Unknown unit for electricityMeasurement: {electricity['unit']}")


class DailyUsageDataTransformer(Transformer):

    def _transform(self, record: dict) -> dict:
        return {
            "household_id": record["household_id"],
            "activation_code": record["household_activation_code"],
            "date": record["date"],
            "type": record["type"],
            "usage": record["usage"],
        }
