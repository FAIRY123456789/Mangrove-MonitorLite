CREATE TABLE IF NOT EXISTS device_info (
    id BIGINT NOT NULL AUTO_INCREMENT,
    device_id VARCHAR(128) NOT NULL,
    device_type VARCHAR(64) NOT NULL,
    device_name VARCHAR(255),
    expected_interval_seconds BIGINT NOT NULL DEFAULT 5,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    CONSTRAINT uk_device_info_device_id UNIQUE (device_id)
);

CREATE TABLE IF NOT EXISTS telemetry_record (
    id BIGINT NOT NULL AUTO_INCREMENT,
    message_id VARCHAR(191) NOT NULL,
    device_id VARCHAR(128) NOT NULL,
    device_type VARCHAR(64) NOT NULL,
    observed_at TIMESTAMP(6) NOT NULL,
    received_at TIMESTAMP(6) NOT NULL,
    schema_version VARCHAR(32) NOT NULL,
    source_protocol VARCHAR(16) NOT NULL,
    metrics_json TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    cleaning_status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    CONSTRAINT uk_telemetry_message_id UNIQUE (message_id),
    INDEX idx_telemetry_device_observed (device_id, observed_at)
);

CREATE TABLE IF NOT EXISTS anomaly_event (
    id BIGINT NOT NULL AUTO_INCREMENT,
    telemetry_id BIGINT,
    device_id VARCHAR(128) NOT NULL,
    observed_at TIMESTAMP(6) NOT NULL,
    anomaly_type VARCHAR(32) NOT NULL,
    metric_name VARCHAR(128),
    anomaly_score DOUBLE NOT NULL,
    detector_name VARCHAR(64) NOT NULL,
    detector_version VARCHAR(64) NOT NULL,
    evidence_json TEXT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_anomaly_telemetry FOREIGN KEY (telemetry_id) REFERENCES telemetry_record(id),
    INDEX idx_anomaly_device_observed (device_id, observed_at),
    INDEX idx_anomaly_type (anomaly_type)
);
