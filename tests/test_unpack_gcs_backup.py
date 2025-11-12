from tests.fixture import p4_hour_example_june_25
from transform.google import P4HourData2025Transformer


def test_transform_p4_new_format():
    transformed = P4HourData2025Transformer().transform(p4_hour_example_june_25)

    assert transformed['date'] == "2025-06-01"
    assert transformed['type'] == "gas"
    assert transformed['meter_ean'] == "871688540006514357"
    assert transformed['measurement_h_0'] == 3881.251
    assert transformed['measurement_h_24'] == 3881.623
