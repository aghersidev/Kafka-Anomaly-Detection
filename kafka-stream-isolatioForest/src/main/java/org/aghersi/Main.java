package org.aghersi;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import smile.anomaly.IsolationForest;

import java.io.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "anomaly-tree-detector1";
    private static final String KAFKA_TOPIC = "augmented";
    private static final String KAFKA_OUTPUT_TOPIC = "anomalies";
    private static final int ROLLING_WINDOW_SIZE = 100;
    private static final Map<String, List<double[]>> featureVectorsMap = new HashMap<>();
    private static final Map<String, List<Double>> anomalyScoresMap = new HashMap<>();
    private static final Instant startTime = Instant.now();
    private static final AtomicLong recordCount = new AtomicLong();
    private static final AtomicLong byteCount = new AtomicLong();
    private static final AtomicLong cantAnomalies = new AtomicLong();

    // Custom LinkedHashMap to store IsolationForest models with a purge policy
    private static final LinkedHashMap<String, IsolationForest> isolationForestCache = new LinkedHashMap<>(1000, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, IsolationForest> eldest) {
            return size() > 1000; // Keep only the 1000 most recently used entries
        }
    };

    public static void main(String[] args) {
        Properties props = createStreamsConfig();
        StreamsBuilder builder = new StreamsBuilder();
        Serde<AugmentedStockData> augmentedStockDataSerde = new AvroSerde<>(AugmentedStockData.class);
        Serde<AnomalyDetectionResult> anomalyDetectionResultSerde = new AvroSerde<>(AnomalyDetectionResult.class);

        KStream<String, AugmentedStockData> stream = builder.stream(
                KAFKA_TOPIC,
                Consumed.with(Serdes.String(), augmentedStockDataSerde)
        );

        stream
                .transformValues(() -> new AnomalyDetectionTransformer())
                .filter(Main::filterAnomalies)
                .mapValues(Main::mapToAnomalyDetectionResult)
                .peek(Main::logMetrics)
                .to(KAFKA_OUTPUT_TOPIC, Produced.with(Serdes.String(), anomalyDetectionResultSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = setupShutdownHook(streams);

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties createStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GROUP);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return props;
    }

    private static boolean filterAnomalies(String key, AnomalyDetectionResult result) {
        double score = result.getScore();
        double dynamicThreshold = result.getDynamicThreshold();
        return score > dynamicThreshold;
    }

    private static AnomalyDetectionResult mapToAnomalyDetectionResult(String key, AnomalyDetectionResult result) {
        cantAnomalies.incrementAndGet();
        result.setDate(Instant.now().toString());
        result.setMethod("Isolation Forest");
        result.setDetectedTime(Instant.now().toString());
        return result;
    }

    private static void logMetrics(String key, AnomalyDetectionResult value) {
        long currentRecords = recordCount.get();
        boolean val = currentRecords > 1000000 && currentRecords < 1001000;
        boolean val2 = currentRecords > 2000000 && currentRecords < 2001000;
        boolean val3 = currentRecords > 3000000 && currentRecords < 3001000;
        boolean val4 = currentRecords > 4000000 && currentRecords < 4001000;
        boolean val5 = currentRecords > 5000000 && currentRecords < 5001000;
        boolean val6 = currentRecords > 6000000 && currentRecords < 6001000;
        boolean val7 = currentRecords > 7000000 && currentRecords < 7001000;
        boolean val8 = currentRecords > 8000000 && currentRecords < 8001000;
        boolean val9 = currentRecords > 9000000 && currentRecords < 9001000;
        boolean val91 = currentRecords > 9250000 && currentRecords < 9251000;
        boolean val92 = currentRecords > 9500000 && currentRecords < 9600000;
        boolean val93 = currentRecords > 9750000 && currentRecords < 9751000;
        boolean val10 = currentRecords > 10250000 && currentRecords < 10251000;
        boolean val11 = currentRecords > 10500000 && currentRecords < 10501000;
        boolean val12 = currentRecords > 10750000 && currentRecords < 10751000;
        boolean val13 = currentRecords >= 9999900;
        boolean val0 = val || val2 || val3 || val4 || val5 || val6 || val7 || val8 || val9 || val10 || val11 || val12 || val13 || val91 || val93 || val92;
        if (!val0) {
            return;
        }
        long currentBytes = byteCount.get();
        long currentAnomalies = cantAnomalies.get();
        Duration elapsed = Duration.between(startTime, Instant.now());
        double secondsElapsed = elapsed.toMillis() / 1000.0;
        System.out.printf("Records: %d | Bytes: %d | Anomalies: %d | Elapsed Time: %.2f sec | Records/sec: %.2f | Bytes/sec: %.2f%n",
                currentRecords, currentBytes, currentAnomalies, secondsElapsed,
                currentRecords / secondsElapsed, currentBytes / secondsElapsed);
    }

    private static CountDownLatch setupShutdownHook(KafkaStreams streams) {
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Shutdown hook triggered");
                long currentRecords = recordCount.get();
                long currentBytes = byteCount.get();
                System.out.printf("Start Time: %s | Records: %d | Bytes: %d%n",
                        startTime, currentRecords, currentBytes);
                streams.close();
                latch.countDown();
            }
        });
        return latch;
    }

    public static class AnomalyDetectionTransformer implements ValueTransformerWithKey<String, AugmentedStockData, AnomalyDetectionResult> {
        private final ExecutorService executorService; // Thread pool for asynchronous training
        private final Set<String> keysBeingTrained = Collections.synchronizedSet(new HashSet<>());

        public AnomalyDetectionTransformer() {
            this.executorService = Executors.newSingleThreadExecutor(); // Single-threaded executor for simplicity
        }

        @Override
        public void init(org.apache.kafka.streams.processor.ProcessorContext context) {
            // No state store initialization needed since we're using the LinkedHashMap
        }

        @Override
        public AnomalyDetectionResult transform(String key, AugmentedStockData value) {
            recordCount.incrementAndGet();

            // Extract features from the incoming record
            double adjClose = value.getAdjClose() != null ? value.getAdjClose() : 0.0;
            double volume = value.getVolume() != null ? value.getVolume() : 0.0;
            double up = value.getUp();
            double down = value.getDown();
            double size = value.getSize();
            double[] features = {adjClose, volume, up, down, size};

            // Add the feature vector to the rolling window
            featureVectorsMap.putIfAbsent(key, new ArrayList<>());
            anomalyScoresMap.putIfAbsent(key, new ArrayList<>());
            List<double[]> keyFeaturesList = featureVectorsMap.get(key);
            keyFeaturesList.add(features);

            // Check if the rolling window size has been exceeded
            if (keyFeaturesList.size() > ROLLING_WINDOW_SIZE && !keysBeingTrained.contains(key)) {
                // Mark this key as being trained
                keysBeingTrained.add(key);

                // Offload the training task to a background thread
                executorService.submit(() -> {
                    try {
                        // Train the IsolationForest model using the current feature vectors
                        IsolationForest trainedForest = IsolationForest.fit(keyFeaturesList.toArray(new double[0][]));

                        // Save the trained model in the LinkedHashMap cache
                        synchronized (isolationForestCache) {
                            isolationForestCache.put(key, trainedForest);
                        }

                        // Clear the feature list only after training is complete
                        synchronized (keyFeaturesList) {
                            keyFeaturesList.clear();
                        }
                    } catch (Exception e) {
                        System.err.println("Error during asynchronous training for key " + key + ": " + e.getMessage());
                    } finally {
                        // Ensure the key is removed from the set of keys being trained
                        keysBeingTrained.remove(key);
                    }
                });
            }

            // Use the current model (if available) for anomaly detection
            IsolationForest forest;
            synchronized (isolationForestCache) {
                forest = isolationForestCache.get(key);
            }
            double score = forest != null ? forest.score(features) : Double.NaN;
            if (!Double.isNaN(score)) {
                List<Double> scores = anomalyScoresMap.get(key);
                scores.add(score);
                if (scores.size() > 2 * ROLLING_WINDOW_SIZE) {
                    anomalyScoresMap.put(key, scores.subList(ROLLING_WINDOW_SIZE, 2 * ROLLING_WINDOW_SIZE));
                }
            }

            // Prepare the result
            AnomalyDetectionResult result = new AnomalyDetectionResult();
            result.setScore(score);
            result.setAdjClose(adjClose);
            result.setDynamicThreshold(calculateDynamicThreshold(key));
            return result;
        }

        @Override
        public void close() {
            // Shutdown the executor service to clean up resources
            executorService.shutdown();
        }
    }

    private static double calculateDynamicThreshold(String key) {
        double dynamicThreshold = 0;
        if (anomalyScoresMap.get(key).size() >= 2) {
            double mean = anomalyScoresMap.get(key).stream()
                    .mapToDouble(Double::doubleValue)
                    .average()
                    .orElse(0);
            double stdDev = Math.sqrt(
                    anomalyScoresMap.get(key).stream()
                            .mapToDouble(s -> Math.pow(s - mean, 2))
                            .average()
                            .orElse(0));
            return mean + 2 * stdDev;
        }
        return 0;
    }

    // Custom Avro Serde
    static class AvroSerde<T extends SpecificRecord> implements Serde<T> {
        private final Class<T> clazz;

        public AvroSerde(Class<T> clazz) {
            this.clazz = clazz;
        }

        @Override
        public Serializer<T> serializer() {
            return (topic, data) -> {
                try {
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    DatumWriter<T> writer = new SpecificDatumWriter<>(clazz);
                    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                    writer.write(data, encoder);
                    encoder.flush();
                    byteCount.addAndGet(out.toByteArray().length);
                    return out.toByteArray();
                } catch (IOException e) {
                    throw new RuntimeException("Serialization error", e);
                }
            };
        }

        @Override
        public Deserializer<T> deserializer() {
            return (topic, data) -> {
                try {
                    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
                    Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
                    return reader.read(null, decoder);
                } catch (IOException e) {
                    throw new RuntimeException("Deserialization error", e);
                }
            };
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
        }

        @Override
        public void close() {
        }
    }
}