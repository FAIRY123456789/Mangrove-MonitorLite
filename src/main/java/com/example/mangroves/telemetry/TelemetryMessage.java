package com.example.mangroves.telemetry;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Protocol-neutral telemetry envelope. Unknown source fields remain in rawPayload.
 */
public record TelemetryMessage(
        String messageId,
        String deviceId,
        String deviceType,
        Instant observedAt,
        Instant receivedAt,
        String schemaVersion,
        SourceProtocol sourceProtocol,
        Map<String, Double> metrics,
        String rawPayload) {

    public TelemetryMessage {
        messageId = requireText(messageId, "messageId");
        deviceId = requireText(deviceId, "deviceId");
        deviceType = requireText(deviceType, "deviceType");
        observedAt = Objects.requireNonNull(observedAt, "observedAt is required");
        receivedAt = Objects.requireNonNull(receivedAt, "receivedAt is required");
        schemaVersion = requireText(schemaVersion, "schemaVersion");
        sourceProtocol = Objects.requireNonNull(sourceProtocol, "sourceProtocol is required");
        rawPayload = Objects.requireNonNullElse(rawPayload, "");
        Map<String, Double> cleaned = new LinkedHashMap<>();
        Objects.requireNonNull(metrics, "metrics are required").forEach((name, value) -> {
            String metricName = requireText(name, "metric name");
            if (value == null || !Double.isFinite(value)) {
                throw new IllegalArgumentException("metric " + metricName + " must be finite");
            }
            cleaned.put(metricName, value);
        });
        if (cleaned.isEmpty()) {
            throw new IllegalArgumentException("at least one numeric metric is required");
        }
        metrics = Map.copyOf(cleaned);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value.trim();
    }

    public enum SourceProtocol {
        HTTP, MQTT, REPLAY
    }
}
