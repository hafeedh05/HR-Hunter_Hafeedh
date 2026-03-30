from __future__ import annotations

import math
from typing import Optional, Tuple

from hr_hunter.models import GeoSpec


def parse_lat_lon(value: Optional[str]) -> Optional[Tuple[float, float]]:
    if not value:
        return None
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 2:
        return None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None


def haversine_miles(
    latitude_one: float,
    longitude_one: float,
    latitude_two: float,
    longitude_two: float,
) -> float:
    radius_miles = 3958.8

    phi_one = math.radians(latitude_one)
    phi_two = math.radians(latitude_two)
    delta_phi = math.radians(latitude_two - latitude_one)
    delta_lambda = math.radians(longitude_two - longitude_one)

    component = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi_one) * math.cos(phi_two) * math.sin(delta_lambda / 2) ** 2
    )
    return 2 * radius_miles * math.atan2(math.sqrt(component), math.sqrt(1 - component))


def distance_from_center(geo: GeoSpec, location_geo: Optional[str]) -> Optional[float]:
    if geo.center_latitude is None or geo.center_longitude is None:
        return None

    parsed = parse_lat_lon(location_geo)
    if not parsed:
        return None

    latitude, longitude = parsed
    return haversine_miles(geo.center_latitude, geo.center_longitude, latitude, longitude)
