package com.example.mangroves.telemetry;

import java.util.List;

public interface AnomalyDetector {
    List<AnomalyResult> detect(TelemetryMessage message, long expectedIntervalSeconds);
}
