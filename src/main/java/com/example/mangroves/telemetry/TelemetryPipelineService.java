package com.example.mangroves.telemetry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class TelemetryPipelineService {
    private static final DateTimeFormatter CSV_TIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ObjectMapper objectMapper;
    private final TelemetryRepository repository;
    private final AnomalyDetector anomalyDetector;

    public TelemetryPipelineService(
            ObjectMapper objectMapper,
            TelemetryRepository repository,
            AnomalyDetector anomalyDetector) {
        this.objectMapper = objectMapper;
        this.repository = repository;
        this.anomalyDetector = anomalyDetector;
    }

    @Transactional
    public IngestOutcome ingest(TelemetryMessage message) {
        TelemetryRepository.SaveResult saved = repository.saveTelemetry(message);
        if (saved.duplicate()) {
            return new IngestOutcome(message, saved.telemetryId(), true, List.of());
        }

        List<AnomalyResult> results = anomalyDetector.detect(
                message, repository.expectedIntervalSeconds(message.deviceId())).stream()
                .map(result -> result.withTelemetryId(saved.telemetryId()))
                .toList();
        results.forEach(result -> repository.saveAnomaly(saved.telemetryId(), result));
        return new IngestOutcome(
                message, saved.telemetryId(), false, List.copyOf(results));
    }

    @Transactional
    public List<IngestOutcome> ingestJson(
            String rawPayload, TelemetryMessage.SourceProtocol sourceProtocol) {
        return adaptJson(rawPayload, sourceProtocol).stream().map(this::ingest).toList();
    }

    @Transactional
    public List<IngestOutcome> ingestCsv(
            String csv, String deviceId, String deviceType) {
        return adaptCsv(csv, deviceId, deviceType).stream().map(this::ingest).toList();
    }

    public List<TelemetryMessage> adaptJson(
            String rawPayload, TelemetryMessage.SourceProtocol sourceProtocol) {
        JsonNode root = parseRoot(rawPayload);
        if (root.has("head") && root.has("data")) {
            return adaptIrgason(root, rawPayload, sourceProtocol);
        }
        return List.of(adaptFlatJson(root, rawPayload, sourceProtocol));
    }

    public List<TelemetryMessage> adaptCsv(
            String csv, String deviceId, String deviceType) {
        if (csv == null || csv.isBlank()) {
            throw new IllegalArgumentException("CSV payload is empty");
        }
        String[] lines = csv.strip().split("\\R");
        if (lines.length < 2) {
            throw new IllegalArgumentException("CSV must contain a header and at least one record");
        }
        String[] headers = lines[0].split(",", -1);
        int timeIndex = indexOf(headers, "time");
        if (timeIndex < 0) {
            throw new IllegalArgumentException("CSV time column is required");
        }

        List<TelemetryMessage> messages = new ArrayList<>();
        for (int lineNumber = 1; lineNumber < lines.length; lineNumber++) {
            if (lines[lineNumber].isBlank()) {
                continue;
            }
            String[] values = lines[lineNumber].split(",", -1);
            if (values.length != headers.length) {
                throw new IllegalArgumentException(
                        "CSV line " + (lineNumber + 1) + " has unexpected column count");
            }
            Instant observedAt = parseInstant(values[timeIndex]);
            Map<String, Double> metrics = new LinkedHashMap<>();
            for (int column = 0; column < headers.length; column++) {
                String name = headers[column].trim();
                if (column == timeIndex || name.isBlank() || name.equalsIgnoreCase("id")) {
                    continue;
                }
                String rawValue = values[column].trim();
                if (rawValue.isEmpty()) {
                    continue;
                }
                double value;
                try {
                    value = Double.parseDouble(rawValue);
                } catch (NumberFormatException error) {
                    throw new IllegalArgumentException(
                            "CSV metric " + name + " is not numeric at line " + (lineNumber + 1),
                            error);
                }
                if (!Double.isFinite(value)) {
                    throw new IllegalArgumentException(
                            "CSV metric " + name + " is not finite at line " + (lineNumber + 1));
                }
                metrics.put(name, value);
            }
            String sourceId = headers.length > 0 && !values[0].isBlank()
                    ? values[0].trim()
                    : Integer.toString(lineNumber);
            messages.add(new TelemetryMessage(
                    deviceId + ":csv:" + sourceId + ":" + observedAt,
                    deviceId,
                    deviceType,
                    observedAt,
                    Instant.now(),
                    "1",
                    TelemetryMessage.SourceProtocol.REPLAY,
                    metrics,
                    lines[lineNumber]));
        }
        return List.copyOf(messages);
    }

    private List<TelemetryMessage> adaptIrgason(
            JsonNode root, String rawPayload, TelemetryMessage.SourceProtocol sourceProtocol) {
        JsonNode head = root.path("head");
        JsonNode environment = head.path("environment");
        JsonNode fields = head.path("fields");
        JsonNode data = root.path("data");
        if (!fields.isArray() || !data.isArray()) {
            throw new IllegalArgumentException("IRGASON fields and data must be arrays");
        }

        String station = textOrFallback(environment, "station_name", "unknown-station");
        String serial = textOrFallback(environment, "serial_no", station);
        String model = textOrFallback(environment, "model", "CR6");
        List<String> names = new ArrayList<>();
        fields.forEach(field -> names.add(field.path("name").asText("")));

        List<TelemetryMessage> messages = new ArrayList<>();
        for (JsonNode entry : data) {
            JsonNode vals = entry.path("vals");
            if (!vals.isArray()) {
                throw new IllegalArgumentException("IRGASON data vals must be an array");
            }
            Instant observedAt = parseInstant(textRequired(entry, "time"));
            Map<String, Double> metrics = new LinkedHashMap<>();
            int count = Math.min(names.size(), vals.size());
            for (int index = 0; index < count; index++) {
                String name = names.get(index);
                if (name.isBlank()) {
                    continue;
                }
                Double value = numericValue(vals.get(index), name, false);
                if (value != null) {
                    metrics.put(name, value);
                }
            }
            String recordNumber = entry.hasNonNull("no")
                    ? entry.get("no").asText()
                    : Integer.toHexString(entry.toString().hashCode());
            messages.add(new TelemetryMessage(
                    serial + ":" + recordNumber + ":" + observedAt,
                    serial,
                    model,
                    observedAt,
                    Instant.now(),
                    "cr6-fields-vals-v1",
                    sourceProtocol,
                    metrics,
                    rawPayload));
        }
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("IRGASON data array is empty");
        }
        return List.copyOf(messages);
    }

    private TelemetryMessage adaptFlatJson(
            JsonNode root, String rawPayload, TelemetryMessage.SourceProtocol sourceProtocol) {
        JsonNode metricsNode = root.path("metrics");
        if (!metricsNode.isObject()) {
            throw new IllegalArgumentException("metrics object is required");
        }
        Map<String, Double> metrics = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = metricsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            Double value = numericValue(field.getValue(), field.getKey(), true);
            if (value != null) {
                metrics.put(field.getKey(), value);
            }
        }
        return new TelemetryMessage(
                textRequired(root, "messageId"),
                textRequired(root, "deviceId"),
                textRequired(root, "deviceType"),
                parseInstant(textRequired(root, "observedAt")),
                root.hasNonNull("receivedAt")
                        ? parseInstant(root.get("receivedAt").asText())
                        : Instant.now(),
                root.hasNonNull("schemaVersion")
                        ? root.get("schemaVersion").asText()
                        : "1",
                sourceProtocol,
                metrics,
                rawPayload);
    }

    private JsonNode parseRoot(String rawPayload) {
        if (rawPayload == null || rawPayload.isBlank()) {
            throw new IllegalArgumentException("JSON payload is empty");
        }
        String normalized = rawPayload.strip();
        if (normalized.startsWith("\uFEFF")) {
            normalized = normalized.substring(1);
        }
        if (!normalized.startsWith("{")) {
            normalized = "{" + normalized;
        }
        if (!normalized.endsWith("}")) {
            normalized = normalized + "}";
        }
        try {
            return objectMapper.readTree(normalized);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("invalid JSON payload: " + error.getOriginalMessage(), error);
        }
    }

    private static Double numericValue(JsonNode node, String name, boolean strict) {
        if (node == null || node.isNull() || (node.isTextual() && node.asText().isBlank())) {
            return null;
        }
        double value;
        if (node.isNumber()) {
            value = node.asDouble();
        } else if (node.isTextual()) {
            try {
                value = Double.parseDouble(node.asText().trim());
            } catch (NumberFormatException error) {
                if (!strict) {
                    return null;
                }
                throw new IllegalArgumentException("metric " + name + " is not numeric", error);
            }
        } else {
            if (!strict) {
                return null;
            }
            throw new IllegalArgumentException("metric " + name + " is not numeric");
        }
        if (!Double.isFinite(value)) {
            throw new IllegalArgumentException("metric " + name + " must be finite");
        }
        return value;
    }

    static Instant parseInstant(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("timestamp is required");
        }
        String normalized = value.trim();
        try {
            return Instant.parse(normalized);
        } catch (DateTimeParseException ignored) {
            try {
                return OffsetDateTime.parse(normalized).toInstant();
            } catch (DateTimeParseException ignoredAgain) {
                try {
                    return LocalDateTime.parse(normalized).toInstant(ZoneOffset.UTC);
                } catch (DateTimeParseException ignoredThird) {
                    try {
                        return LocalDateTime.parse(normalized, CSV_TIME).toInstant(ZoneOffset.UTC);
                    } catch (DateTimeParseException error) {
                        throw new IllegalArgumentException("unsupported timestamp: " + value, error);
                    }
                }
            }
        }
    }

    private static String textRequired(JsonNode node, String field) {
        if (!node.hasNonNull(field) || node.get(field).asText().isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return node.get(field).asText().trim();
    }

    private static String textOrFallback(JsonNode node, String field, String fallback) {
        return node.hasNonNull(field) && !node.get(field).asText().isBlank()
                ? node.get(field).asText().trim()
                : fallback;
    }

    private static int indexOf(String[] values, String target) {
        for (int i = 0; i < values.length; i++) {
            if (values[i].trim().equalsIgnoreCase(target)) {
                return i;
            }
        }
        return -1;
    }

    public record IngestOutcome(
            TelemetryMessage message,
            long telemetryId,
            boolean duplicate,
            List<AnomalyResult> anomalyResults) {}
}
