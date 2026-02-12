import argparse
import json
import apache_beam as beam #pip install apache beam for stream processing pipeline job
from apache_beam.options.pipeline_options 
import PipelineOptions, StandardOptions

# serialize and store in dictionary
def bytes_to_dict(b: bytes) -> dict:
    # Pub/Sub delivers bytes
    s = b.decode("utf-8")
    return json.loads(s)

# erase invalid records NOT containing temperature nor pressure
def is_valid(rec: dict) -> bool:
    """
    Filter out records with missing measurements (None).
    Keys in message format:
      - pressure (kPa)
      - temperature (C)
    """
    if rec is None:
        return False
    # Required fields for conversion:
    if rec.get("pressure") is None:
        return False
    if rec.get("temperature") is None:
        return False
    return True

# convert from kilopascals to PSI for pressure entered in JSON message
# convert from celsius to farenheit for temperature entered in JSON message
def convert_units(rec: dict) -> dict:
    """
    P(psi) = P(kPa) / 6.895
    T(F)   = T(C) * 1.8 + 32
    """
    p_kpa = float(rec["pressure"])
    t_c = float(rec["temperature"])

    rec["pressure_psi"] = p_kpa / 6.895
    rec["temperature_f"] = (t_c * 1.8) + 32

    return rec

# for python dictionary
def dict_to_bytes(rec: dict) -> bytes:
    return json.dumps(rec).encode("utf-8")

# receive arguments in command run for dataflow job
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--staging_location", required=True)

    # Pub/Sub topics in the "projects/<PROJECT>/topics/<TOPIC>" format
    parser.add_argument("--input_topic", required=True)
    parser.add_argument("--output_topic", required=True)

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(
        pipeline_args,
        project=known_args.project,
        region=known_args.region,
        temp_location=known_args.temp_location,
        staging_location=known_args.staging_location,
        save_main_session=True,
    )

    # Streaming job (keeps running)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
            | "ToDict" >> beam.Map(bytes_to_dict)
            | "FilterMissing" >> beam.Filter(is_valid)
            | "ConvertUnits" >> beam.Map(convert_units)
            | "ToBytes" >> beam.Map(dict_to_bytes)
            | "WriteToPubSub" >> beam.io.WriteToPubSub(topic=known_args.output_topic)
        )


if __name__ == "__main__":
    run()
