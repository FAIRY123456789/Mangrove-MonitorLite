package com.example.mangroves.telemetry;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

@Component
@ConditionalOnProperty(name = "mangrove.mqtt.enabled", havingValue = "true")
public class MqttTelemetryGateway implements MqttCallback, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(MqttTelemetryGateway.class);

    private final TelemetryPipelineService pipeline;
    private final String brokerUri;
    private final String topic;
    private final String clientId;
    private final boolean enabled;
    private final AtomicLong acceptedMessages = new AtomicLong();
    private volatile String lastError;
    private volatile MqttClient client;

    @Autowired
    public MqttTelemetryGateway(TelemetryPipelineService pipeline, Environment environment) {
        this(pipeline,
                environment.getProperty("mangrove.mqtt.broker-uri", "tcp://127.0.0.1:1883"),
                environment.getProperty("mangrove.mqtt.topic", "mangrove/+/telemetry"),
                environment.getProperty("mangrove.mqtt.client-id", "mangrove-ingest"),
                environment.getProperty("mangrove.mqtt.enabled", Boolean.class, false));
    }

    public MqttTelemetryGateway(
            TelemetryPipelineService pipeline,
            String brokerUri,
            String topic,
            String clientId,
            boolean enabled) {
        this.pipeline = pipeline;
        this.brokerUri = brokerUri;
        this.topic = topic;
        this.clientId = clientId;
        this.enabled = enabled;
    }

    @PostConstruct
    public synchronized void start() {
        if (!enabled || (client != null && client.isConnected())) {
            return;
        }
        try {
            MqttClient mqttClient = new MqttClient(brokerUri, clientId, new MemoryPersistence());
            mqttClient.setCallback(this);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(false);
            options.setConnectionTimeout(5);
            mqttClient.connect(options);
            mqttClient.subscribe(topic, 1);
            client = mqttClient;
            log.info("MQTT telemetry subscribed to {} with QoS 1", topic);
        } catch (Exception error) {
            lastError = error.getMessage();
            throw new IllegalStateException("cannot start MQTT telemetry gateway", error);
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        lastError = cause == null ? "connection lost" : cause.getMessage();
        log.warn("MQTT connection lost: {}", lastError);
    }

    @Override
    public void messageArrived(String arrivedTopic, MqttMessage message) {
        String payload = new String(message.getPayload(), StandardCharsets.UTF_8);
        try {
            pipeline.ingestJson(payload, TelemetryMessage.SourceProtocol.MQTT);
            acceptedMessages.incrementAndGet();
        } catch (RuntimeException error) {
            lastError = error.getMessage();
            log.warn("Rejected MQTT payload on {}: {}", arrivedTopic, lastError);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Subscriber-only gateway; no delivery action is required.
    }

    public long acceptedMessages() {
        return acceptedMessages.get();
    }

    public String lastError() {
        return lastError;
    }

    public boolean isConnected() {
        return client != null && client.isConnected();
    }

    @PreDestroy
    @Override
    public synchronized void close() {
        if (client == null) {
            return;
        }
        try {
            if (client.isConnected()) {
                client.unsubscribe(topic);
                client.disconnect();
            }
            client.close();
        } catch (Exception error) {
            log.warn("Error while closing MQTT gateway: {}", error.getMessage());
        } finally {
            client = null;
        }
    }
}
