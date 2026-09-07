package com.example.mangroves;

import com.example.mangroves.telemetry.AnomalyDetectionService;
import com.example.mangroves.telemetry.AnomalyResult;
import com.example.mangroves.telemetry.TelemetryMessage;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AnomalyDetectionTest {

    @Test
    void missingRuleUsesExpectedAndActualIntervals() {
        AnomalyDetectionService detector = detector(16, 7L);
        TelemetryMessage first = message("gap-1", 0, Map.of("x", 1.0, "y", 2.0));
        TelemetryMessage normal = message("gap-2", 5, Map.of("x", 1.1, "y", 2.1));
        TelemetryMessage gap = message("gap-3", 25, Map.of("x", 1.2, "y", 2.2));

        assertThat(detected(detector.detect(first, 5), "MISSING")).isEmpty();
        assertThat(detected(detector.detect(normal, 5), "MISSING")).isEmpty();
        List<AnomalyResult> missing = detected(detector.detect(gap, 5), "MISSING");

        assertThat(missing).hasSize(1);
        assertThat(missing.get(0).evidence())
                .containsEntry("expectedIntervalSeconds", 5L)
                .containsEntry("actualIntervalSeconds", 20L)
                .containsEntry("thresholdMultiplier", 2.0);
        assertThat(missing.get(0).evidence()).containsKeys("missingFrom", "missingTo");
    }

    @Test
    void rollingMadWarmsUpAvoidsFalsePositivesAndDetectsInjectedSpike() {
        AnomalyDetectionService detector = detector(32, 11L);
        int suddenChanges = 0;
        AnomalyResult firstWarmup = null;
        for (int index = 0; index < 8; index++) {
            TelemetryMessage normal = message(
                    "mad-" + index, index * 5L,
                    Map.of("x", 10.0, "y", 20.0 + (index % 2) * 0.1));
            List<AnomalyResult> results = detector.detect(normal, 5);
            if (index == 0) {
                firstWarmup = results.stream()
                        .filter(result -> result.metricName() != null
                                && result.metricName().equals("x"))
                        .findFirst().orElseThrow();
            }
            suddenChanges += detected(results, "SUDDEN_CHANGE").size();
        }

        assertThat(firstWarmup.status()).isEqualTo(AnomalyResult.DetectionStatus.WARMING_UP);
        assertThat(firstWarmup.evidence()).containsKeys("windowSize", "minimumSamples", "threshold");
        assertThat(suddenChanges).isZero();

        List<AnomalyResult> spike = detected(detector.detect(
                message("mad-spike", 40, Map.of("x", 1000.0, "y", 20.0)), 5),
                "SUDDEN_CHANGE");
        assertThat(spike).anyMatch(result -> result.metricName().equals("x"));
        AnomalyResult xSpike = spike.stream()
                .filter(result -> result.metricName().equals("x")).findFirst().orElseThrow();
        assertThat(xSpike.evidence())
                .containsEntry("mad", 0.0)
                .containsEntry("madZero", true)
                .containsEntry("windowSize", 8)
                .containsEntry("threshold", 6.0);
    }

    @Test
    void isolationForestIsReproducibleScoresInjectedOutlierAndSkipsUnsafeInputs() {
        AnomalyDetectionService firstDetector = detector(16, 2024L);
        AnomalyDetectionService secondDetector = detector(16, 2024L);
        List<TelemetryMessage> training = new ArrayList<>();
        for (int index = 0; index < 80; index++) {
            double x = Math.sin(index / 8.0) + (index % 3) * 0.01;
            double y = 2.0 * x + Math.cos(index / 10.0) * 0.05;
            training.add(message(
                    "train-" + index, index * 5L,
                    Map.of("x", x, "y", y, "constant", 1.0)));
        }

        AnomalyDetectionService.TrainingResult firstTraining =
                firstDetector.trainIsolationForest(training);
        AnomalyDetectionService.TrainingResult secondTraining =
                secondDetector.trainIsolationForest(training);
        assertThat(firstTraining.status()).isEqualTo(AnomalyResult.DetectionStatus.SCORED);
        assertThat(firstTraining.features()).containsExactly("x", "y");
        assertThat(firstTraining.modelVersion()).isEqualTo(secondTraining.modelVersion());

        TelemetryMessage normal = message(
                "score-normal", 500, Map.of("x", 0.25, "y", 0.52, "constant", 1.0));
        TelemetryMessage injected = message(
                "score-injected", 505, Map.of("x", 100.0, "y", -100.0, "constant", 1.0));
        AnomalyResult normalScore = firstDetector.scoreIsolationForest(normal);
        AnomalyResult injectedScore = firstDetector.scoreIsolationForest(injected);
        AnomalyResult repeatedScore = secondDetector.scoreIsolationForest(injected);

        assertThat(injectedScore.anomalyScore()).isGreaterThan(normalScore.anomalyScore());
        assertThat(injectedScore.anomalyScore()).isEqualTo(repeatedScore.anomalyScore());
        assertThat(injectedScore.detectorName()).contains("experimental");
        assertThat(injectedScore.evidence())
                .containsEntry("experimental", true)
                .containsKeys("featureOrder", "modelVersion", "trainingSampleSize");

        AnomalyDetectionService insufficient = detector(16, 2024L);
        assertThat(insufficient.trainIsolationForest(training.subList(0, 8)).status())
                .isEqualTo(AnomalyResult.DetectionStatus.SKIPPED_INSUFFICIENT_DATA);
        TelemetryMessage featureMismatch = message(
                "mismatch", 510, Map.of("x", 0.1, "constant", 1.0));
        assertThat(firstDetector.scoreIsolationForest(featureMismatch).status())
                .isEqualTo(AnomalyResult.DetectionStatus.SKIPPED_FEATURE_MISMATCH);
    }

    private static AnomalyDetectionService detector(int minimumTrainingSamples, long seed) {
        return new AnomalyDetectionService(
                2.0, 8, 4, 6.0,
                minimumTrainingSamples, 64, 64, 100, 0.60, seed);
    }

    private static TelemetryMessage message(
            String messageId, long secondOffset, Map<String, Double> metrics) {
        Instant observed = Instant.parse("2024-01-01T00:00:00Z").plusSeconds(secondOffset);
        return new TelemetryMessage(
                messageId, "device-a", "TEST", observed, observed.plusSeconds(1),
                "1", TelemetryMessage.SourceProtocol.REPLAY, metrics, "{}");
    }

    private static List<AnomalyResult> detected(
            List<AnomalyResult> results, String type) {
        return results.stream()
                .filter(result -> result.status() == AnomalyResult.DetectionStatus.DETECTED)
                .filter(result -> result.anomalyType().equals(type))
                .toList();
    }
}