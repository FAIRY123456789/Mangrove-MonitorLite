package com.example.mangroves.telemetry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

@Component
public class TelemetryReplayTool {
    private final TelemetryPipelineService pipeline;
    private final ObjectMapper objectMapper;

    public TelemetryReplayTool(TelemetryPipelineService pipeline, ObjectMapper objectMapper) {
        this.pipeline = pipeline;
        this.objectMapper = objectMapper;
    }

    public List<TelemetryMessage> loadJson(Path path) {
        try {
            return pipeline.adaptJson(Files.readString(path), TelemetryMessage.SourceProtocol.REPLAY);
        } catch (IOException error) {
            throw new IllegalArgumentException("cannot read replay JSON: " + path, error);
        }
    }

    public List<TelemetryMessage> loadCsv(Path path, String deviceId, String deviceType) {
        try {
            return pipeline.adaptCsv(Files.readString(path), deviceId, deviceType);
        } catch (IOException error) {
            throw new IllegalArgumentException("cannot read replay CSV: " + path, error);
        }
    }

    public ReplaySummary replay(
            List<TelemetryMessage> source,
            ReplayOptions options,
            Consumer<TelemetryMessage> target) {
        Random random = new Random(options.randomSeed());
        int sent = 0;
        int skipped = 0;
        int spikeInjected = 0;
        int multivariateInjected = 0;

        for (int index = 0; index < source.size(); index++) {
            TelemetryMessage original = source.get(index);
            boolean skip = options.missingIndexes().contains(index)
                    || (options.missingRatio() > 0.0
                    && random.nextDouble() < options.missingRatio());
            if (skip) {
                skipped++;
                continue;
            }

            Map<String, Double> metrics = new LinkedHashMap<>(original.metrics());
            Set<String> faults = new LinkedHashSet<>();
            if (options.spikeIndexes().contains(index)
                    && options.spikeMetric() != null
                    && metrics.containsKey(options.spikeMetric())) {
                metrics.computeIfPresent(
                        options.spikeMetric(),
                        (key, value) -> value * options.spikeFactor());
                faults.add("SUDDEN_CHANGE:" + options.spikeMetric());
                spikeInjected++;
            }
            if (options.multivariateIndexes().contains(index)) {
                options.multivariateFactors().forEach((metric, factor) ->
                        metrics.computeIfPresent(metric, (key, value) -> value * factor));
                if (!options.multivariateFactors().isEmpty()) {
                    faults.add("MULTIVARIATE");
                    multivariateInjected++;
                }
            }

            String rawPayload = original.rawPayload();
            if (!faults.isEmpty()) {
                rawPayload = faultPayload(original.rawPayload(), faults, options.randomSeed());
            }
            String faultSuffix = faults.isEmpty()
                    ? ""
                    : ":fault-" + Integer.toHexString(faults.hashCode());
            TelemetryMessage replayed = new TelemetryMessage(
                    original.messageId() + ":replay" + faultSuffix,
                    original.deviceId(),
                    original.deviceType(),
                    original.observedAt(),
                    Instant.now(),
                    original.schemaVersion(),
                    TelemetryMessage.SourceProtocol.REPLAY,
                    metrics,
                    rawPayload);
            target.accept(replayed);
            sent++;
            pause(options.interval());
        }
        return new ReplaySummary(source.size(), sent, skipped, spikeInjected,
                multivariateInjected, options.randomSeed());
    }

    public ReplaySummary replayToPipeline(
            List<TelemetryMessage> source, ReplayOptions options) {
        return replay(source, options, pipeline::ingest);
    }

    public ReplaySummary replayToMqtt(
            List<TelemetryMessage> source,
            ReplayOptions options,
            String brokerUri,
            String topicTemplate) {
        MqttClient client = null;
        try {
            client = new MqttClient(
                    brokerUri, MqttClient.generateClientId(), new MemoryPersistence());
            MqttConnectOptions connectOptions = new MqttConnectOptions();
            connectOptions.setCleanSession(true);
            connectOptions.setConnectionTimeout(5);
            client.connect(connectOptions);
            MqttClient connectedClient = client;
            ReplaySummary summary = replay(source, options, message -> {
                try {
                    String topic = topicTemplate.replace("{deviceId}", message.deviceId());
                    byte[] payload = objectMapper.writeValueAsBytes(message);
                    MqttMessage mqttMessage = new MqttMessage(payload);
                    mqttMessage.setQos(1);
                    connectedClient.publish(topic, mqttMessage);
                } catch (Exception error) {
                    throw new IllegalStateException("cannot publish replay MQTT message", error);
                }
            });
            client.disconnect();
            return summary;
        } catch (Exception error) {
            throw new IllegalStateException("cannot replay to MQTT", error);
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception ignored) {
                    // The primary failure, if any, has already been preserved.
                }
            }
        }
    }

    private String faultPayload(String originalRawPayload, Set<String> faults, long seed) {
        Map<String, Object> marker = new LinkedHashMap<>();
        marker.put("originalRawPayload", originalRawPayload);
        marker.put("syntheticFault", true);
        marker.put("faults", faults);
        marker.put("randomSeed", seed);
        try {
            return objectMapper.writeValueAsString(marker);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("cannot mark synthetic replay fault", error);
        }
    }

    private static void pause(Duration interval) {
        if (interval == null || interval.isZero() || interval.isNegative()) {
            return;
        }
        try {
            Thread.sleep(interval.toMillis());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("replay interrupted", error);
        }
    }

    public record ReplayOptions(
            Duration interval,
            double missingRatio,
            Set<Integer> missingIndexes,
            String spikeMetric,
            double spikeFactor,
            Set<Integer> spikeIndexes,
            Map<String, Double> multivariateFactors,
            Set<Integer> multivariateIndexes,
            long randomSeed) {

        public ReplayOptions {
            interval = interval == null ? Duration.ZERO : interval;
            if (missingRatio < 0.0 || missingRatio > 1.0) {
                throw new IllegalArgumentException("missingRatio must be between 0 and 1");
            }
            missingIndexes = missingIndexes == null ? Set.of() : Set.copyOf(missingIndexes);
            spikeIndexes = spikeIndexes == null ? Set.of() : Set.copyOf(spikeIndexes);
            multivariateFactors = multivariateFactors == null
                    ? Map.of() : Map.copyOf(multivariateFactors);
            multivariateIndexes = multivariateIndexes == null
                    ? Set.of() : Set.copyOf(multivariateIndexes);
        }

        public static ReplayOptions normal(long seed) {
            return new ReplayOptions(
                    Duration.ZERO, 0.0, Set.of(), null, 1.0,
                    Set.of(), Map.of(), Set.of(), seed);
        }
    }

    public record ReplaySummary(
            int inputCount,
            int sentCount,
            int skippedCount,
            int spikeInjectedCount,
            int multivariateInjectedCount,
            long randomSeed) {}
}
