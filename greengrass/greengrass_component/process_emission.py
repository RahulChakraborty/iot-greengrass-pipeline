# process_emission.py

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class EmissionProcessor:
    """
    Pure-Python processor for vehicle emission events.
    No Greengrass or greengrasssdk dependency.

    You can tweak thresholds/logic here without touching main.py.
    """

    def __init__(
        self,
        co2_warn_threshold: float = 8000.0,
        co_warn_threshold: float = 200.0,
        nox_warn_threshold: float = 5.0,
    ):
        self.co2_warn_threshold = co2_warn_threshold
        self.co_warn_threshold = co_warn_threshold
        self.nox_warn_threshold = nox_warn_threshold

    def process(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a single vehicle emission event and return a summarized record.

        Expected keys (based on your CSV):
          - timestep_time
          - vehicle_CO
          - vehicle_CO2
          - vehicle_HC
          - vehicle_NOx
          - vehicle_PMx
          - vehicle_id
          - vehicle_speed
          - vehicle_type
          - vehicle_x
          - vehicle_y

        Any missing keys are handled gracefully.
        """
        # Extract with defaults
        timestep = event.get("timestep_time")
        vehicle_id = event.get("vehicle_id") or "unknown"

        co = self._safe_float(event.get("vehicle_CO"))
        co2 = self._safe_float(event.get("vehicle_CO2"))
        nox = self._safe_float(event.get("vehicle_NOx"))
        pmx = self._safe_float(event.get("vehicle_PMx"))
        speed = self._safe_float(event.get("vehicle_speed"))

        # Simple status flags
        co2_status = "OK"
        if co2 >= self.co2_warn_threshold:
            co2_status = "HIGH"

        co_status = "OK"
        if co >= self.co_warn_threshold:
            co_status = "HIGH"

        nox_status = "OK"
        if nox >= self.nox_warn_threshold:
            nox_status = "HIGH"

        # Very simple "emission score"
        # You can adjust the formula as you like.
        emission_score = co * 0.3 + co2 * 0.0005 + nox * 1.0 + pmx * 5.0

        result = {
            "vehicle_id": vehicle_id,
            "timestep_time": timestep,
            "vehicle_CO": co,
            "vehicle_CO2": co2,
            "vehicle_NOx": nox,
            "vehicle_PMx": pmx,
            "vehicle_speed": speed,
            "co2_status": co2_status,
            "co_status": co_status,
            "nox_status": nox_status,
            "emission_score": emission_score,
        }

        logger.info(
            "Processed emission event for %s at t=%s: co2=%s (%s), co=%s (%s), nox=%s (%s), score=%.3f",
            vehicle_id,
            timestep,
            co2,
            co2_status,
            co,
            co_status,
            nox,
            nox_status,
            emission_score,
        )

        return result

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0


# -----------------------------------------------------------------------------
# Lambda-style entry point used by main.py
# -----------------------------------------------------------------------------

_processor = EmissionProcessor()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda-style wrapper retained so main.py can keep calling:

        result = process_emission.lambda_handler(event, None)

    This function does NOT call AWS Lambda or use greengrasssdk.
    """
    return _processor.process(event)
