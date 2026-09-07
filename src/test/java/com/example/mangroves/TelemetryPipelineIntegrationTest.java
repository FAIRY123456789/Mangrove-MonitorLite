package com.example.mangroves;

import com.example.mangroves.telemetry.AnomalyDetectionService;
import com.example.mangroves.telemetry.AnomalyResult;
import com.example.mangroves.telemetry.MqttTelemetryGateway;
import com.example.mangroves.telemetry.TelemetryController;
import com.example.mangroves.telemetry.TelemetryMessage;
import com.example.mangroves.telemetry.TelemetryPipelineService;
import com.example.mangroves.telemetry.TelemetryRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class TelemetryPipelineIntegrationTest {
    private ObjectMapper objectMapper;
    private TelemetryRepository repository;
    private TelemetryPipelineService pipeline;


    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:pipeline_" + UUID.randomUUID()
                + ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(dataSource);
        repository = new TelemetryRepository(new JdbcTemplate(dataSource), objectMapper, 5);
        AnomalyDetectionService detector = new AnomalyDetectionService(
                2.0, 8, 4, 6.0, 16, 32, 32, 100, 0.60, 7L);
        pipeline = new TelemetryPipelineService(objectMapper, repository, detector);
    }

    @Test
    void adaptsCleansAndStoresIrgasonCsvAndFlatJsonIdempotently() {
        String irgason = """
                {"head":{"environment":{"station_name":"demo-station","serial_no":"demo-001","model":"CR6"},
                "fields":[{"name":"message"},{"name":"Ux"},{"name":"CO2_density"}]},
                "data":[{"time":"2024-06-07T21:18:50.271","no":110305,
                "vals":["ok","1.25",800.5]}]}
                """;

        List<TelemetryMessage> adapted = pipeline.adaptJson(
                irgason, TelemetryMessage.SourceProtocol.HTTP);
        assertThat(adapted).hasSize(1);
        assertThat(adapted.get(0).metrics())
                .containsEntry("Ux", 1.25)
                .containsEntry("CO2_density", 800.5)
                .doesNotContainKey("message");

        TelemetryPipelineService.IngestOutcome first = pipeline.ingest(adapted.get(0));
        TelemetryPipelineService.IngestOutcome duplicate = pipeline.ingest(adapted.get(0));
        assertThat(first.duplicate()).isFalse();
        assertThat(duplicate.duplicate()).isTrue();
        assertThat(repository.countTelemetry()).isEqualTo(1);

        String csv = """
                ,time,ndvi,gcc,empty
                0,2024-07-01 09:00:28,0.846,0.400,
                1,2024-07-01 10:00:28,0.850,0.405,
                """;
        List<TelemetryMessage> csvMessages = pipeline.adaptCsv(csv, "camera-demo", "PHENOLOGY_CAMERA");
        assertThat(csvMessages).hasSize(2);
        assertThat(csvMessages.get(0).sourceProtocol())
                .isEqualTo(TelemetryMessage.SourceProtocol.REPLAY);
        assertThat(csvMessages.get(0).metrics()).containsOnlyKeys("ndvi", "gcc");

        String flat = """
                {"messageId":"m-flat","deviceId":"d-flat","deviceType":"TEST",
                "observedAt":"2024-07-01T01:00:00Z","schemaVersion":"2",
                "metrics":{"temperature":"25.5","humidity":80},"unknownField":"preserved"}
                """;
        TelemetryMessage flatMessage = pipeline.adaptJson(
                flat, TelemetryMessage.SourceProtocol.MQTT).get(0);
        assertThat(flatMessage.metrics()).containsEntry("temperature", 25.5);
        assertThat(flatMessage.rawPayload()).contains("unknownField");

        assertThatThrownBy(() -> pipeline.adaptJson(
                flat.replace("\"25.5\"", "\"NaN\""), TelemetryMessage.SourceProtocol.MQTT))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("finite");
        assertThatThrownBy(() -> pipeline.adaptJson(
                "{\"deviceId\":\"d\",\"deviceType\":\"T\",\"observedAt\":\"2024-01-01T00:00:00Z\",\"metrics\":{\"x\":1}}",
                TelemetryMessage.SourceProtocol.MQTT))
                .hasMessageContaining("messageId");
    }

    @Test
    void realLocalMqttPublishSubscribeSurvivesInvalidPayload() throws Exception {
        try (LocalMqttBroker broker = new LocalMqttBroker()) {
            String brokerUri = "tcp://127.0.0.1:" + broker.port();
            MqttTelemetryGateway gateway = new MqttTelemetryGateway(
                    pipeline, brokerUri, "mangrove/+/telemetry",
                    "test-subscriber-" + UUID.randomUUID(), true);
            MqttClient publisher = null;
            try {
                gateway.start();
                assertThat(gateway.isConnected()).isTrue();

                publisher = new MqttClient(
                        brokerUri, "test-publisher-" + UUID.randomUUID(), new MemoryPersistence());
                MqttConnectOptions connectOptions = new MqttConnectOptions();
                connectOptions.setCleanSession(true);
                publisher.connect(connectOptions);

                publish(publisher, "mangrove/device-1/telemetry", validMqttPayload("mqtt-1", 1.0));
                await(() -> repository.countTelemetry() == 1, 5);

                publish(publisher, "mangrove/device-1/telemetry", "{invalid-json");
                publish(publisher, "mangrove/device-1/telemetry", validMqttPayload("mqtt-2", 2.0));
                await(() -> repository.countTelemetry() == 2, 5);

                assertThat(gateway.acceptedMessages()).isEqualTo(2);
                assertThat(gateway.lastError()).contains("invalid JSON");
                assertThat(repository.findTelemetry(
                        "device-1", Instant.parse("2024-01-01T00:00:00Z"),
                        Instant.parse("2024-01-01T00:01:00Z"))).hasSize(2);
            } finally {
                if (publisher != null) {
                    if (publisher.isConnected()) {
                        publisher.disconnect();
                    }
                    publisher.close();
                }
                gateway.close();
            }
        }
    }

    @Test    void restApiFiltersTelemetryTimeAndAnomalyType() throws Exception {
        TelemetryMessage message = new TelemetryMessage(
                "rest-1", "rest-device", "TEST",
                Instant.parse("2024-07-01T00:00:00Z"), Instant.parse("2024-07-01T00:00:01Z"),
                "1", TelemetryMessage.SourceProtocol.REPLAY, Map.of("x", 1.0), "{}");
        TelemetryRepository.SaveResult saved = repository.saveTelemetry(message);
        repository.saveAnomaly(saved.telemetryId(), new AnomalyResult(
                saved.telemetryId(), "rest-device", message.observedAt(), "MISSING", null,
                2.0, "sampling-gap-rule", "rule-mad-v1",
                AnomalyResult.DetectionStatus.DETECTED,
                Map.of("expectedIntervalSeconds", 5, "actualIntervalSeconds", 20)));

        MockMvc mvc = MockMvcBuilders.standaloneSetup(new TelemetryController(repository)).build();
        mvc.perform(get("/api/telemetry")
                        .param("deviceId", "rest-device")
                        .param("from", "2024-06-30T23:59:00Z")
                        .param("to", "2024-07-01T00:01:00Z"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].messageId").value("rest-1"))
                .andExpect(jsonPath("$[0].sourceProtocol").value("REPLAY"));

        mvc.perform(get("/api/anomalies")
                        .param("deviceId", "rest-device")
                        .param("type", "MISSING"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].anomalyType").value("MISSING"));

        mvc.perform(get("/api/telemetry/" + saved.telemetryId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.telemetry.messageId").value("rest-1"))
                .andExpect(jsonPath("$.anomalies[0].detectorName").value("sampling-gap-rule"));
    }

    private static String validMqttPayload(String messageId, double value) {
        return """
                {"messageId":"%s","deviceId":"device-1","deviceType":"IRGASON",
                "observedAt":"2024-01-01T00:00:%02dZ","schemaVersion":"1",
                "metrics":{"Ux":%s,"CO2_density":800.0}}
                """.formatted(messageId, (int) value, value);
    }

    private static void publish(MqttClient client, String topic, String payload) throws Exception {
        MqttMessage message = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
        message.setQos(1);
        client.publish(topic, message);
    }


    private static void await(BooleanSupplier condition, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertThat(condition.getAsBoolean()).isTrue();
    }
}
final class LocalMqttBroker implements AutoCloseable {
    private final java.net.ServerSocket serverSocket;
    private final java.util.concurrent.ExecutorService workers =
            java.util.concurrent.Executors.newCachedThreadPool(runnable -> {
                Thread thread = new Thread(runnable, "local-mqtt-test-broker");
                thread.setDaemon(true);
                return thread;
            });
    private final java.util.concurrent.CopyOnWriteArrayList<ClientConnection> clients =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.CopyOnWriteArrayList<Subscription> subscriptions =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicInteger packetIds =
            new java.util.concurrent.atomic.AtomicInteger(1);
    private volatile boolean running = true;

    LocalMqttBroker() throws java.io.IOException {
        serverSocket = new java.net.ServerSocket();
        serverSocket.bind(new java.net.InetSocketAddress(
                java.net.InetAddress.getByName("127.0.0.1"), 0));
        workers.submit(this::acceptLoop);
    }

    int port() {
        return serverSocket.getLocalPort();
    }

    private void acceptLoop() {
        while (running) {
            try {
                java.net.Socket socket = serverSocket.accept();
                ClientConnection client = new ClientConnection(socket);
                clients.add(client);
                workers.submit(() -> handle(client));
            } catch (java.net.SocketException closed) {
                if (running) {
                    throw new IllegalStateException("test broker accept failed", closed);
                }
            } catch (java.io.IOException error) {
                throw new IllegalStateException("test broker accept failed", error);
            }
        }
    }

    private void handle(ClientConnection client) {
        try (client) {
            java.io.InputStream input = client.socket.getInputStream();
            while (running) {
                int header = input.read();
                if (header < 0) {
                    return;
                }
                int remainingLength = readRemainingLength(input);
                byte[] body = input.readNBytes(remainingLength);
                if (body.length != remainingLength) {
                    throw new java.io.EOFException("incomplete MQTT packet");
                }
                int packetType = header >>> 4;
                switch (packetType) {
                    case 1 -> client.send(new byte[]{0x20, 0x02, 0x00, 0x00});
                    case 3 -> handlePublish(client, header, body);
                    case 4 -> { }
                    case 8 -> handleSubscribe(client, body);
                    case 10 -> handleUnsubscribe(client, body);
                    case 12 -> client.send(new byte[]{(byte) 0xD0, 0x00});
                    case 14 -> { return; }
                    default -> throw new java.io.IOException(
                            "unsupported MQTT packet type in test broker: " + packetType);
                }
            }
        } catch (java.io.IOException ignoredWhenClosing) {
            if (running) {
                throw new IllegalStateException("test broker client failed", ignoredWhenClosing);
            }
        } finally {
            clients.remove(client);
            subscriptions.removeIf(subscription -> subscription.client == client);
        }
    }

    private void handleSubscribe(ClientConnection client, byte[] body) throws java.io.IOException {
        int packetId = readUnsignedShort(body, 0);
        int offset = 2;
        java.util.List<Byte> grantedQos = new java.util.ArrayList<>();
        while (offset < body.length) {
            int topicLength = readUnsignedShort(body, offset);
            offset += 2;
            String filter = new String(body, offset, topicLength, java.nio.charset.StandardCharsets.UTF_8);
            offset += topicLength;
            int requestedQos = body[offset++] & 0x03;
            subscriptions.add(new Subscription(filter, client));
            grantedQos.add((byte) Math.min(requestedQos, 1));
        }
        byte[] response = new byte[4 + grantedQos.size()];
        response[0] = (byte) 0x90;
        response[1] = (byte) (2 + grantedQos.size());
        response[2] = (byte) (packetId >>> 8);
        response[3] = (byte) packetId;
        for (int index = 0; index < grantedQos.size(); index++) {
            response[4 + index] = grantedQos.get(index);
        }
        client.send(response);
    }

    private void handleUnsubscribe(ClientConnection client, byte[] body) throws java.io.IOException {
        int packetId = readUnsignedShort(body, 0);
        subscriptions.removeIf(subscription -> subscription.client == client);
        client.send(new byte[]{(byte) 0xB0, 0x02,
                (byte) (packetId >>> 8), (byte) packetId});
    }

    private void handlePublish(ClientConnection publisher, int header, byte[] body)
            throws java.io.IOException {
        int topicLength = readUnsignedShort(body, 0);
        String topic = new String(body, 2, topicLength, java.nio.charset.StandardCharsets.UTF_8);
        int offset = 2 + topicLength;
        int qos = (header >>> 1) & 0x03;
        int packetId = 0;
        if (qos > 0) {
            packetId = readUnsignedShort(body, offset);
            offset += 2;
        }
        byte[] payload = java.util.Arrays.copyOfRange(body, offset, body.length);
        for (Subscription subscription : subscriptions) {
            if (topicMatches(subscription.filter, topic)) {
                subscription.client.publish(topic, payload, nextPacketId());
            }
        }
        if (qos == 1) {
            publisher.send(new byte[]{0x40, 0x02,
                    (byte) (packetId >>> 8), (byte) packetId});
        }
    }

    private int nextPacketId() {
        return packetIds.updateAndGet(current -> current >= 65_535 ? 1 : current + 1);
    }

    private static boolean topicMatches(String filter, String topic) {
        String[] expected = filter.split("/", -1);
        String[] actual = topic.split("/", -1);
        for (int index = 0; index < expected.length; index++) {
            if (expected[index].equals("#")) {
                return index == expected.length - 1;
            }
            if (index >= actual.length
                    || !(expected[index].equals("+") || expected[index].equals(actual[index]))) {
                return false;
            }
        }
        return expected.length == actual.length;
    }

    private static int readUnsignedShort(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
    }

    private static int readRemainingLength(java.io.InputStream input) throws java.io.IOException {
        int multiplier = 1;
        int value = 0;
        int encoded;
        do {
            encoded = input.read();
            if (encoded < 0) {
                throw new java.io.EOFException("missing MQTT remaining length");
            }
            value += (encoded & 0x7F) * multiplier;
            multiplier *= 128;
            if (multiplier > 128 * 128 * 128 * 128) {
                throw new java.io.IOException("invalid MQTT remaining length");
            }
        } while ((encoded & 0x80) != 0);
        return value;
    }

    private static byte[] encodeRemainingLength(int value) {
        java.io.ByteArrayOutputStream encoded = new java.io.ByteArrayOutputStream();
        do {
            int digit = value % 128;
            value /= 128;
            if (value > 0) {
                digit |= 0x80;
            }
            encoded.write(digit);
        } while (value > 0);
        return encoded.toByteArray();
    }

    @Override
    public void close() throws Exception {
        running = false;
        serverSocket.close();
        for (ClientConnection client : clients) {
            client.close();
        }
        workers.shutdownNow();
        workers.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);
    }

    private record Subscription(String filter, ClientConnection client) { }

    private static final class ClientConnection implements AutoCloseable {
        private final java.net.Socket socket;

        private ClientConnection(java.net.Socket socket) {
            this.socket = socket;
        }

        private synchronized void send(byte[] bytes) throws java.io.IOException {
            java.io.OutputStream output = socket.getOutputStream();
            output.write(bytes);
            output.flush();
        }

        private synchronized void publish(String topic, byte[] payload, int packetId)
                throws java.io.IOException {
            byte[] topicBytes = topic.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            java.io.ByteArrayOutputStream body = new java.io.ByteArrayOutputStream();
            body.write((topicBytes.length >>> 8) & 0xFF);
            body.write(topicBytes.length & 0xFF);
            body.write(topicBytes);
            body.write((packetId >>> 8) & 0xFF);
            body.write(packetId & 0xFF);
            body.write(payload);
            java.io.OutputStream output = socket.getOutputStream();
            output.write(0x32);
            output.write(encodeRemainingLength(body.size()));
            body.writeTo(output);
            output.flush();
        }

        @Override
        public void close() throws java.io.IOException {
            socket.close();
        }
    }
}