package com.example.mangroves.telemetry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class TelemetryRepository {
    private static final TypeReference<Map<String, Double>> METRICS_TYPE = new TypeReference<>() {};
    private static final TypeReference<Map<String, Object>> EVIDENCE_TYPE = new TypeReference<>() {};

    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final long defaultExpectedIntervalSeconds;

    public TelemetryRepository(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this(jdbc, objectMapper, 5);
    }

    public TelemetryRepository(JdbcTemplate jdbc, ObjectMapper objectMapper, long defaultExpectedIntervalSeconds) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.defaultExpectedIntervalSeconds = defaultExpectedIntervalSeconds;
    }

    public SaveResult saveTelemetry(TelemetryMessage message) {
        ensureDevice(message);
        Optional<Long> existing = findIdByMessageId(message.messageId());
        if (existing.isPresent()) {
            return new SaveResult(existing.get(), true);
        }

        KeyHolder keys = new GeneratedKeyHolder();
        try {
            jdbc.update(connection -> {
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO telemetry_record " +
                                "(message_id, device_id, device_type, observed_at, received_at, schema_version, " +
                                "source_protocol, metrics_json, raw_payload, cleaning_status) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        Statement.RETURN_GENERATED_KEYS);
                statement.setString(1, message.messageId());
                statement.setString(2, message.deviceId());
                statement.setString(3, message.deviceType());
                statement.setTimestamp(4, Timestamp.from(message.observedAt()));
                statement.setTimestamp(5, Timestamp.from(message.receivedAt()));
                statement.setString(6, message.schemaVersion());
                statement.setString(7, message.sourceProtocol().name());
                statement.setString(8, writeJson(message.metrics()));
                statement.setString(9, message.rawPayload());
                statement.setString(10, "CLEAN");
                return statement;
            }, keys);
            Number id = null;
            if (!keys.getKeyList().isEmpty()) {
                for (Map.Entry<String, Object> key : keys.getKeyList().get(0).entrySet()) {
                    if (key.getKey().equalsIgnoreCase("id") && key.getValue() instanceof Number number) {
                        id = number;
                        break;
                    }
                }
            }
            if (id == null) {
                throw new IllegalStateException("database did not return telemetry id");
            }
            return new SaveResult(id.longValue(), false);
        } catch (DuplicateKeyException duplicate) {
            return new SaveResult(findIdByMessageId(message.messageId())
                    .orElseThrow(() -> duplicate), true);
        }
    }

    private void ensureDevice(TelemetryMessage message) {
        Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM device_info WHERE device_id = ?",
                Integer.class, message.deviceId());
        if (count != null && count > 0) {
            return;
        }
        try {
            jdbc.update("INSERT INTO device_info " +
                            "(device_id, device_type, device_name, expected_interval_seconds, enabled) " +
                            "VALUES (?, ?, ?, ?, ?)",
                    message.deviceId(), message.deviceType(), message.deviceId(),
                    defaultExpectedIntervalSeconds, true);
        } catch (DuplicateKeyException ignored) {
            // A concurrent message inserted the same device. The unique key is the final guard.
        }
    }

    public long expectedIntervalSeconds(String deviceId) {
        List<Long> values = jdbc.query(
                "SELECT expected_interval_seconds FROM device_info WHERE device_id = ?",
                (rs, rowNum) -> rs.getLong(1), deviceId);
        return values.isEmpty() ? defaultExpectedIntervalSeconds : values.get(0);
    }

    public void saveAnomaly(long telemetryId, AnomalyResult result) {
        if (result.status() != AnomalyResult.DetectionStatus.DETECTED) {
            return;
        }
        jdbc.update("INSERT INTO anomaly_event " +
                        "(telemetry_id, device_id, observed_at, anomaly_type, metric_name, anomaly_score, " +
                        "detector_name, detector_version, evidence_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                telemetryId,
                result.deviceId(),
                Timestamp.from(result.observedAt()),
                result.anomalyType(),
                result.metricName(),
                result.anomalyScore(),
                result.detectorName(),
                result.detectorVersion(),
                writeJson(result.evidence()));
    }

    public Optional<Long> findIdByMessageId(String messageId) {
        List<Long> ids = jdbc.query("SELECT id FROM telemetry_record WHERE message_id = ?",
                (rs, rowNum) -> rs.getLong(1), messageId);
        return ids.stream().findFirst();
    }

    public long countTelemetry() {
        Long count = jdbc.queryForObject("SELECT COUNT(*) FROM telemetry_record", Long.class);
        return count == null ? 0 : count;
    }

    public List<TelemetryMessage> findTelemetry(
            String deviceId, Instant from, Instant to) {
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM telemetry_record WHERE device_id = ?");
        List<Object> args = new ArrayList<>();
        args.add(deviceId);
        if (from != null) {
            sql.append(" AND observed_at >= ?");
            args.add(Timestamp.from(from));
        }
        if (to != null) {
            sql.append(" AND observed_at <= ?");
            args.add(Timestamp.from(to));
        }
        sql.append(" ORDER BY observed_at");
        return jdbc.query(sql.toString(), telemetryMapper(), args.toArray());
    }

    public Optional<TelemetryMessage> findTelemetryById(long id) {
        return jdbc.query("SELECT * FROM telemetry_record WHERE id = ?",
                telemetryMapper(), id).stream().findFirst();
    }

    public List<AnomalyResult> findAnomalies(
            String deviceId, Instant from, Instant to, String anomalyType) {
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM anomaly_event WHERE device_id = ?");
        List<Object> args = new ArrayList<>();
        args.add(deviceId);
        if (from != null) {
            sql.append(" AND observed_at >= ?");
            args.add(Timestamp.from(from));
        }
        if (to != null) {
            sql.append(" AND observed_at <= ?");
            args.add(Timestamp.from(to));
        }
        if (anomalyType != null && !anomalyType.isBlank()) {
            sql.append(" AND anomaly_type = ?");
            args.add(anomalyType);
        }
        sql.append(" ORDER BY observed_at");
        return jdbc.query(sql.toString(), anomalyMapper(), args.toArray());
    }

    public List<AnomalyResult> findAnomaliesByTelemetryId(long telemetryId) {
        return jdbc.query("SELECT * FROM anomaly_event WHERE telemetry_id = ? ORDER BY id",
                anomalyMapper(), telemetryId);
    }

    private RowMapper<TelemetryMessage> telemetryMapper() {
        return (rs, rowNum) -> new TelemetryMessage(
                rs.getString("message_id"),
                rs.getString("device_id"),
                rs.getString("device_type"),
                rs.getTimestamp("observed_at").toInstant(),
                rs.getTimestamp("received_at").toInstant(),
                rs.getString("schema_version"),
                TelemetryMessage.SourceProtocol.valueOf(rs.getString("source_protocol")),
                readJson(rs.getString("metrics_json"), METRICS_TYPE),
                rs.getString("raw_payload"));
    }

    private RowMapper<AnomalyResult> anomalyMapper() {
        return (rs, rowNum) -> new AnomalyResult(
                rs.getObject("telemetry_id") == null ? null : rs.getLong("telemetry_id"),
                rs.getString("device_id"),
                rs.getTimestamp("observed_at").toInstant(),
                rs.getString("anomaly_type"),
                rs.getString("metric_name"),
                rs.getDouble("anomaly_score"),
                rs.getString("detector_name"),
                rs.getString("detector_version"),
                AnomalyResult.DetectionStatus.DETECTED,
                readJson(rs.getString("evidence_json"), EVIDENCE_TYPE));
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("cannot serialize database JSON", error);
        }
    }

    private <T> T readJson(String value, TypeReference<T> type) {
        try {
            return objectMapper.readValue(value, type);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("cannot read database JSON", error);
        }
    }

    public record SaveResult(long telemetryId, boolean duplicate) {}
}
