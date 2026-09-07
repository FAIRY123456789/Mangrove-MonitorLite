package com.example.mangroves.telemetry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AnomalyDetectionService implements AnomalyDetector {
    private static final String RULE_VERSION = "rule-mad-v1";
    private static final String IFOREST_NAME = "isolation-forest-experimental";

    private final double missingMultiplier;
    private final int madWindowSize;
    private final int madMinimumSamples;
    private final double madThreshold;
    private final int isolationMinimumSamples;
    private final int isolationTreeCount;
    private final int isolationSampleSize;
    private final int isolationRetrainEvery;
    private final double isolationThreshold;
    private final long randomSeed;

    private final Map<String, Instant> lastObserved = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>> madWindows = new ConcurrentHashMap<>();
    private final Deque<TelemetryMessage> trainingWindow = new ArrayDeque<>();
    private volatile IsolationForestModel isolationModel;
    private int messagesSinceTraining;
    private int trainingSequence;

    public AnomalyDetectionService(
            @Value("${mangrove.anomaly.missing-multiplier:2.0}") double missingMultiplier,
            @Value("${mangrove.anomaly.mad-window:25}") int madWindowSize,
            @Value("${mangrove.anomaly.mad-min-samples:8}") int madMinimumSamples,
            @Value("${mangrove.anomaly.mad-threshold:6.0}") double madThreshold,
            @Value("${mangrove.anomaly.iforest-min-samples:32}") int isolationMinimumSamples,
            @Value("${mangrove.anomaly.iforest-trees:64}") int isolationTreeCount,
            @Value("${mangrove.anomaly.iforest-sample-size:128}") int isolationSampleSize,
            @Value("${mangrove.anomaly.iforest-retrain-every:100}") int isolationRetrainEvery,
            @Value("${mangrove.anomaly.iforest-threshold:0.60}") double isolationThreshold,
            @Value("${mangrove.anomaly.iforest-seed:20240719}") long randomSeed) {
        this.missingMultiplier = missingMultiplier;
        this.madWindowSize = madWindowSize;
        this.madMinimumSamples = madMinimumSamples;
        this.madThreshold = madThreshold;
        this.isolationMinimumSamples = isolationMinimumSamples;
        this.isolationTreeCount = isolationTreeCount;
        this.isolationSampleSize = isolationSampleSize;
        this.isolationRetrainEvery = isolationRetrainEvery;
        this.isolationThreshold = isolationThreshold;
        this.randomSeed = randomSeed;
    }

    @Override
    public List<AnomalyResult> detect(TelemetryMessage message, long expectedIntervalSeconds) {
        List<AnomalyResult> results = new ArrayList<>();
        detectMissing(message, expectedIntervalSeconds).ifPresent(results::add);
        for (Map.Entry<String, Double> metric : message.metrics().entrySet()) {
            AnomalyResult result = detectMad(message, metric.getKey(), metric.getValue());
            if (result != null) {
                results.add(result);
            }
        }
        results.add(scoreIsolationForest(message));
        observeForRetraining(message);
        return results;
    }

    private java.util.Optional<AnomalyResult> detectMissing(
            TelemetryMessage message, long expectedIntervalSeconds) {
        Instant previous = lastObserved.get(message.deviceId());
        if (previous == null || message.observedAt().isAfter(previous)) {
            lastObserved.put(message.deviceId(), message.observedAt());
        }
        if (previous == null || !message.observedAt().isAfter(previous)) {
            return java.util.Optional.empty();
        }
        long actualSeconds = Duration.between(previous, message.observedAt()).getSeconds();
        double allowedSeconds = expectedIntervalSeconds * missingMultiplier;
        if (actualSeconds <= allowedSeconds) {
            return java.util.Optional.empty();
        }
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("expectedIntervalSeconds", expectedIntervalSeconds);
        evidence.put("actualIntervalSeconds", actualSeconds);
        evidence.put("missingFrom", previous.plusSeconds(expectedIntervalSeconds).toString());
        evidence.put("missingTo", message.observedAt().toString());
        evidence.put("thresholdMultiplier", missingMultiplier);
        evidence.put("allowedIntervalSeconds", allowedSeconds);
        return java.util.Optional.of(new AnomalyResult(
                null, message.deviceId(), message.observedAt(), "MISSING", null,
                actualSeconds / Math.max(1.0, allowedSeconds),
                "sampling-gap-rule", RULE_VERSION,
                AnomalyResult.DetectionStatus.DETECTED, Map.copyOf(evidence)));
    }

    private AnomalyResult detectMad(TelemetryMessage message, String metricName, double value) {
        String key = message.deviceId() + "::" + metricName;
        Deque<Double> window = madWindows.computeIfAbsent(key, ignored -> new ArrayDeque<>());
        synchronized (window) {
            if (window.size() < madMinimumSamples) {
                addWindowValue(window, value);
                return new AnomalyResult(
                        null, message.deviceId(), message.observedAt(), "SUDDEN_CHANGE",
                        metricName, 0.0, "rolling-median-mad", RULE_VERSION,
                        AnomalyResult.DetectionStatus.WARMING_UP,
                        Map.of("windowSize", window.size(),
                                "minimumSamples", madMinimumSamples,
                                "threshold", madThreshold));
            }
            List<Double> values = new ArrayList<>(window);
            double median = median(values);
            List<Double> deviations = values.stream()
                    .map(v -> Math.abs(v - median)).sorted().toList();
            double mad = median(deviations);
            boolean madZero = mad == 0.0;
            double fallbackScale = 0.0;
            double robustZ;
            if (madZero) {
                List<Double> nonZeroDeviations = deviations.stream()
                        .filter(deviation -> deviation > 0.0)
                        .toList();
                if (nonZeroDeviations.isEmpty()) {
                    robustZ = value == median ? 0.0 : 1_000_000.0;
                } else {
                    fallbackScale = median(nonZeroDeviations);
                    robustZ = 0.6745 * Math.abs(value - median) / fallbackScale;
                }
            } else {
                robustZ = 0.6745 * Math.abs(value - median) / mad;
            }
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("currentValue", value);
            evidence.put("median", median);
            evidence.put("mad", mad);
            evidence.put("madZero", madZero);
            evidence.put("fallbackScale", fallbackScale);
            evidence.put("windowSize", window.size());
            evidence.put("threshold", madThreshold);
            evidence.put("robustZ", robustZ);
            addWindowValue(window, value);
            if (robustZ >= madThreshold) {
                return new AnomalyResult(
                        null, message.deviceId(), message.observedAt(), "SUDDEN_CHANGE",
                        metricName, robustZ, "rolling-median-mad", RULE_VERSION,
                        AnomalyResult.DetectionStatus.DETECTED, Map.copyOf(evidence));
            }
            return null;
        }
    }

    private void addWindowValue(Deque<Double> window, double value) {
        window.addLast(value);
        while (window.size() > madWindowSize) {
            window.removeFirst();
        }
    }

    public synchronized TrainingResult trainIsolationForest(List<TelemetryMessage> messages) {
        if (messages == null || messages.size() < isolationMinimumSamples) {
            return new TrainingResult(
                    AnomalyResult.DetectionStatus.SKIPPED_INSUFFICIENT_DATA,
                    null, List.of(), messages == null ? 0 : messages.size());
        }
        List<String> features = new ArrayList<>(messages.get(0).metrics().keySet());
        features.removeIf(feature -> messages.stream().anyMatch(
                message -> !message.metrics().containsKey(feature)
                        || !Double.isFinite(message.metrics().get(feature))));
        features.sort(Comparator.naturalOrder());
        features.removeIf(feature -> isConstant(messages, feature));
        if (features.size() < 2) {
            return new TrainingResult(
                    AnomalyResult.DetectionStatus.SKIPPED_INSUFFICIENT_DATA,
                    null, List.copyOf(features), messages.size());
        }
        double[][] rows = new double[messages.size()][features.size()];
        for (int row = 0; row < messages.size(); row++) {
            for (int column = 0; column < features.size(); column++) {
                rows[row][column] = messages.get(row).metrics().get(features.get(column));
            }
        }
        int sampleSize = Math.min(isolationSampleSize, rows.length);
        int maxDepth = (int) Math.ceil(Math.log(sampleSize) / Math.log(2));
        Random random = new Random(randomSeed);
        List<IsolationNode> trees = new ArrayList<>();
        for (int tree = 0; tree < isolationTreeCount; tree++) {
            double[][] sample = sampleWithoutReplacement(rows, sampleSize, random);
            trees.add(buildTree(sample, 0, maxDepth, random));
        }
        String version = "iforest-v1-" + randomSeed + "-"
                + Integer.toHexString(Objects.hash(features, ++trainingSequence));
        isolationModel = new IsolationForestModel(List.copyOf(features), List.copyOf(trees),
                sampleSize, version);
        messagesSinceTraining = 0;
        return new TrainingResult(AnomalyResult.DetectionStatus.SCORED,
                version, List.copyOf(features), messages.size());
    }

    public AnomalyResult scoreIsolationForest(TelemetryMessage message) {
        IsolationForestModel model = isolationModel;
        if (model == null) {
            return new AnomalyResult(
                    null, message.deviceId(), message.observedAt(), "MULTIVARIATE",
                    null, 0.0, IFOREST_NAME, "untrained",
                    AnomalyResult.DetectionStatus.SKIPPED_INSUFFICIENT_DATA,
                    Map.of("minimumSamples", isolationMinimumSamples, "experimental", true));
        }
        double[] features = new double[model.features().size()];
        List<String> missing = new ArrayList<>();
        for (int i = 0; i < model.features().size(); i++) {
            Double value = message.metrics().get(model.features().get(i));
            if (value == null || !Double.isFinite(value)) {
                missing.add(model.features().get(i));
            } else {
                features[i] = value;
            }
        }
        if (!missing.isEmpty()) {
            return new AnomalyResult(
                    null, message.deviceId(), message.observedAt(), "MULTIVARIATE",
                    null, 0.0, IFOREST_NAME, model.version(),
                    AnomalyResult.DetectionStatus.SKIPPED_FEATURE_MISMATCH,
                    Map.of("missingFeatures", List.copyOf(missing),
                            "featureOrder", model.features(), "experimental", true));
        }
        double totalPath = 0.0;
        for (IsolationNode tree : model.trees()) {
            totalPath += pathLength(features, tree, 0);
        }
        double averagePath = totalPath / model.trees().size();
        double score = Math.pow(2.0, -averagePath
                / Math.max(averagePathConstant(model.sampleSize()), 1.0e-9));
        AnomalyResult.DetectionStatus status = score >= isolationThreshold
                ? AnomalyResult.DetectionStatus.DETECTED
                : AnomalyResult.DetectionStatus.SCORED;
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("featureOrder", model.features());
        evidence.put("modelVersion", model.version());
        evidence.put("threshold", isolationThreshold);
        evidence.put("averagePathLength", averagePath);
        evidence.put("trainingSampleSize", model.sampleSize());
        evidence.put("experimental", true);
        return new AnomalyResult(
                null, message.deviceId(), message.observedAt(), "MULTIVARIATE",
                null, score, IFOREST_NAME, model.version(), status, Map.copyOf(evidence));
    }

    private synchronized void observeForRetraining(TelemetryMessage message) {
        trainingWindow.addLast(message);
        while (trainingWindow.size() > Math.max(isolationSampleSize * 2, isolationMinimumSamples)) {
            trainingWindow.removeFirst();
        }
        messagesSinceTraining++;
        if ((isolationModel == null && trainingWindow.size() >= isolationMinimumSamples)
                || (isolationModel != null && messagesSinceTraining >= isolationRetrainEvery)) {
            trainIsolationForest(new ArrayList<>(trainingWindow));
        }
    }

    private boolean isConstant(List<TelemetryMessage> messages, String feature) {
        double first = messages.get(0).metrics().get(feature);
        return messages.stream().allMatch(
                message -> Double.compare(first, message.metrics().get(feature)) == 0);
    }

    private static double median(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int middle = sorted.size() / 2;
        return sorted.size() % 2 == 0
                ? (sorted.get(middle - 1) + sorted.get(middle)) / 2.0
                : sorted.get(middle);
    }

    private static double[][] sampleWithoutReplacement(
            double[][] rows, int sampleSize, Random random) {
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < rows.length; i++) {
            indices.add(i);
        }
        Collections.shuffle(indices, random);
        double[][] sample = new double[sampleSize][];
        for (int i = 0; i < sampleSize; i++) {
            sample[i] = rows[indices.get(i)];
        }
        return sample;
    }

    private static IsolationNode buildTree(
            double[][] rows, int depth, int maxDepth, Random random) {
        if (rows.length <= 1 || depth >= maxDepth) {
            return IsolationNode.leaf(rows.length);
        }
        List<Integer> candidates = new ArrayList<>();
        Map<Integer, double[]> ranges = new HashMap<>();
        for (int feature = 0; feature < rows[0].length; feature++) {
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            for (double[] row : rows) {
                min = Math.min(min, row[feature]);
                max = Math.max(max, row[feature]);
            }
            if (min < max) {
                candidates.add(feature);
                ranges.put(feature, new double[]{min, max});
            }
        }
        if (candidates.isEmpty()) {
            return IsolationNode.leaf(rows.length);
        }
        int feature = candidates.get(random.nextInt(candidates.size()));
        double[] range = ranges.get(feature);
        double split = range[0] + random.nextDouble() * (range[1] - range[0]);
        List<double[]> left = new ArrayList<>();
        List<double[]> right = new ArrayList<>();
        for (double[] row : rows) {
            (row[feature] < split ? left : right).add(row);
        }
        if (left.isEmpty() || right.isEmpty()) {
            return IsolationNode.leaf(rows.length);
        }
        return IsolationNode.branch(feature, split,
                buildTree(left.toArray(double[][]::new), depth + 1, maxDepth, random),
                buildTree(right.toArray(double[][]::new), depth + 1, maxDepth, random));
    }

    private static double pathLength(double[] features, IsolationNode node, int depth) {
        if (node.leaf) {
            return depth + averagePathConstant(node.size);
        }
        return features[node.feature] < node.split
                ? pathLength(features, node.left, depth + 1)
                : pathLength(features, node.right, depth + 1);
    }

    private static double averagePathConstant(int size) {
        if (size <= 1) {
            return 0.0;
        }
        if (size == 2) {
            return 1.0;
        }
        double harmonicApproximation = Math.log(size - 1.0) + 0.5772156649;
        return 2.0 * harmonicApproximation - 2.0 * (size - 1.0) / size;
    }

    public record TrainingResult(
            AnomalyResult.DetectionStatus status,
            String modelVersion,
            List<String> features,
            int sampleCount) {}

    private record IsolationForestModel(
            List<String> features,
            List<IsolationNode> trees,
            int sampleSize,
            String version) {}

    private static final class IsolationNode {
        private final boolean leaf;
        private final int size;
        private final int feature;
        private final double split;
        private final IsolationNode left;
        private final IsolationNode right;

        private IsolationNode(boolean leaf, int size, int feature, double split,
                              IsolationNode left, IsolationNode right) {
            this.leaf = leaf;
            this.size = size;
            this.feature = feature;
            this.split = split;
            this.left = left;
            this.right = right;
        }

        private static IsolationNode leaf(int size) {
            return new IsolationNode(true, size, -1, 0.0, null, null);
        }

        private static IsolationNode branch(
                int feature, double split, IsolationNode left, IsolationNode right) {
            return new IsolationNode(false, 0, feature, split, left, right);
        }
    }
}
