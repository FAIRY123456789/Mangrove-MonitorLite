package com.example.mangroves.telemetry;

import java.time.Instant;
import java.util.Map;

public record AnomalyResult(
        Long telemetryId,
        String deviceId,
        Instant observedAt,
        String anomalyType,
        String metricName,
        double anomalyScore,
        String detectorName,
        String detectorVersion,
        DetectionStatus status,
        Map<String, Object> evidence) {

    public AnomalyResult withTelemetryId(long id) {
        return new AnomalyResult(id, deviceId, observedAt, anomalyType, metricName, anomalyScore,
                detectorName, detectorVersion, status, evidence);
    }

    public enum DetectionStatus {
        DETECTED,
        SCORED,
        WARMING_UP,
        SKIPPED_INSUFFICIENT_DATA,
        SKIPPED_FEATURE_MISMATCH
    }
}
