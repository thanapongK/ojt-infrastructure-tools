from opentelemetry.sdk.resources import SERVICE_NAME

from pyeqx.common import ReturnResult
from pyeqx.opentelemetry.config import (
    TelemetryConfiguration,
    TelemetryType,
)


def parse_telemetry_config(config: dict):
    type = config.get("type", TelemetryType.OTLP)
    endpoint = config.get("endpoint", "localhost:4317")
    protocol = config.get("protocol", "grpc")
    service_name = config.get(SERVICE_NAME, "")

    if type is None:
        raise ValueError("telemetry type is not specified in the configuration.")

    if type == TelemetryType.AZURE_MONITOR:
        return TelemetryConfiguration(
            service_name=service_name,
            type=TelemetryType.AZURE_MONITOR,
            endpoint=endpoint,
        )
    elif type == TelemetryType.OTLP:
        return TelemetryConfiguration(
            service_name=service_name,
            type=TelemetryType.OTLP,
            endpoint=endpoint,
            protocol=protocol,
        )
    else:
        raise ValueError(f"telemetry type {type} is not supported.")
