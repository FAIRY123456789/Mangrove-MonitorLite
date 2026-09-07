package com.example.mangroves;

import com.example.mangroves.telemetry.TelemetryMessage;
import com.example.mangroves.telemetry.TelemetryPipelineService;
import com.example.mangroves.telemetry.TelemetryReplayTool;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ReplayFaultInjectionTest {
    private final ObjectMapper objectMapper =
            new ObjectMapper().registerModule(new JavaTimeModule());
    private final TelemetryPipelineService adapterOnlyPipeline =
            new TelemetryPipelineService(objectMapper, null, null);
    private final TelemetryReplayTool replay =
            new TelemetryReplayTool(adapterOnlyPipeline, objectMapper);


    @Test
    void loadsSyntheticCsvWithTelemetryFieldStructure() {
        Path csv = Path.of("src", "test", "resources", "sample-telemetry.csv");
        List<TelemetryMessage> messages =
                replay.loadCsv(csv, "camera-demo", "PHENOLOGY_CAMERA");

        assertThat(messages).hasSize(8);
        assertThat(messages.get(0).metrics())
                .containsKeys("ndvi", "gcc", "gvi", "rcc", "bcc", "low_lai", "fvc");
        assertThat(messages.get(0).sourceProtocol())
                .isEqualTo(TelemetryMessage.SourceProtocol.REPLAY);
    }
    @Test
    void injectsMissingSpikeAndMultivariateFaultsWithTraceableMarkers() {
        List<TelemetryMessage> source = sampleMessages(10);
        TelemetryReplayTool.ReplayOptions options = new TelemetryReplayTool.ReplayOptions(
                Duration.ZERO,
                0.0,
                Set.of(1, 2),
                "x",
                10.0,
                Set.of(4),
                Map.of("x", -5.0, "y", 8.0),
                Set.of(7),
                42L);
        List<TelemetryMessage> published = new ArrayList<>();

        TelemetryReplayTool.ReplaySummary summary = replay.replay(source, options, published::add);

        assertThat(summary.inputCount()).isEqualTo(10);
        assertThat(summary.sentCount()).isEqualTo(8);
        assertThat(summary.skippedCount()).isEqualTo(2);
        assertThat(summary.spikeInjectedCount()).isEqualTo(1);
        assertThat(summary.multivariateInjectedCount()).isEqualTo(1);

        TelemetryMessage spike = published.stream()
                .filter(message -> message.messageId().startsWith("source-4:"))
                .findFirst().orElseThrow();
        assertThat(spike.metrics().get("x")).isEqualTo(40.0);
        assertThat(spike.rawPayload()).contains("\"syntheticFault\":true")
                .contains("SUDDEN_CHANGE:x");

        TelemetryMessage multivariate = published.stream()
                .filter(message -> message.messageId().startsWith("source-7:"))
                .findFirst().orElseThrow();
        assertThat(multivariate.metrics()).containsEntry("x", -35.0).containsEntry("y", 64.0);
        assertThat(multivariate.rawPayload()).contains("MULTIVARIATE");
    }

    @Test
    void fixedSeedReplaysConsistentlyAndPublishesThroughRealLocalMqtt() throws Exception {
        List<TelemetryMessage> source = sampleMessages(20);
        TelemetryReplayTool.ReplayOptions randomMissing = new TelemetryReplayTool.ReplayOptions(
                Duration.ZERO, 0.25, Set.of(), null, 1.0,
                Set.of(), Map.of(), Set.of(), 99L);
        List<TelemetryMessage> first = new ArrayList<>();
        List<TelemetryMessage> second = new ArrayList<>();
        TelemetryReplayTool.ReplaySummary firstSummary = replay.replay(
                source, randomMissing, first::add);
        TelemetryReplayTool.ReplaySummary secondSummary = replay.replay(
                source, randomMissing, second::add);

        assertThat(firstSummary).isEqualTo(secondSummary);
        assertThat(first.stream().map(TelemetryMessage::messageId).toList())
                .isEqualTo(second.stream().map(TelemetryMessage::messageId).toList());

        try (LocalMqttBroker broker = new LocalMqttBroker()) {
            String brokerUri = "tcp://127.0.0.1:" + broker.port();
            MqttClient subscriber = new MqttClient(
                    brokerUri, "replay-subscriber-" + UUID.randomUUID(), new MemoryPersistence());
            try {
                CountDownLatch received = new CountDownLatch(2);
                subscriber.setCallback(new org.eclipse.paho.client.mqttv3.MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {}

                    @Override
                    public void messageArrived(
                            String topic, org.eclipse.paho.client.mqttv3.MqttMessage message) {
                        received.countDown();
                    }

                    @Override
                    public void deliveryComplete(
                            org.eclipse.paho.client.mqttv3.IMqttDeliveryToken token) {}
                });
                MqttConnectOptions connect = new MqttConnectOptions();
                connect.setCleanSession(true);
                subscriber.connect(connect);
                subscriber.subscribe("mangrove/+/telemetry", 1);

                TelemetryReplayTool.ReplaySummary mqttSummary = replay.replayToMqtt(
                        source.subList(0, 2), TelemetryReplayTool.ReplayOptions.normal(7L),
                        brokerUri, "mangrove/{deviceId}/telemetry");

                assertThat(mqttSummary.sentCount()).isEqualTo(2);
                assertThat(received.await(5, TimeUnit.SECONDS)).isTrue();
            } finally {
                if (subscriber.isConnected()) {
                    subscriber.disconnect();
                }
                subscriber.close();
            }
        }
    }

    private static List<TelemetryMessage> sampleMessages(int count) {
        List<TelemetryMessage> messages = new ArrayList<>();
        Instant start = Instant.parse("2024-01-01T00:00:00Z");
        for (int index = 0; index < count; index++) {
            messages.add(new TelemetryMessage(
                    "source-" + index,
                    "device-replay",
                    "TEST",
                    start.plusSeconds(index * 5L),
                    start.plusSeconds(index * 5L + 1),
                    "1",
                    TelemetryMessage.SourceProtocol.REPLAY,
                    Map.of("x", (double) index, "y", (double) index + 1),
                    "{\"historical\":true}"));
        }
        return messages;
    }

}