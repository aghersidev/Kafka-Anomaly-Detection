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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String KAFKA_GROUP = "stock-processor-app";
    private static final String KAFKA_TOPIC = "input-data";
    private static final String KAFKA_OUTPUT_TOPIC = "augmented";
    private static final AtomicLong byteCount = new AtomicLong();

    public static void main(String[] args) {
        Properties props = createStreamsConfig();
        StreamsBuilder builder = new StreamsBuilder();

        // Create Avro Serdes
        Serde<StockData> stockDataSerde = new AvroSerde<>(StockData.class);
        Serde<AugmentedStockData> augmentedSerde = new AvroSerde<>(AugmentedStockData.class);

        // Build stream with Avro Serdes
        KStream<String, StockData> source = builder.stream(
                KAFKA_TOPIC,
                Consumed.with(Serdes.String(), stockDataSerde)
        );

        KStream<String, AugmentedStockData> processedStream = source.mapValues(stockData -> {
            double open = stockData.getOpen() != null ? stockData.getOpen() : 0.0;
            double close = stockData.getClose() != null ? stockData.getClose() : 0.0;
            double high = stockData.getHigh() != null ? stockData.getHigh() : 0.0;
            double low = stockData.getLow() != null ? stockData.getLow() : 0.0;

            double up = high - Math.max(open, close);
            double down = Math.min(open, close) - low;
            double size = Math.abs(close - open);

            return AugmentedStockData.newBuilder()
                    .setDate(stockData.getDate())
                    .setOpen(stockData.getOpen())
                    .setHigh(stockData.getHigh())
                    .setLow(stockData.getLow())
                    .setClose(stockData.getClose())
                    .setAdjClose(stockData.getAdjClose())
                    .setVolume(stockData.getVolume())
                    .setUp(up)
                    .setDown(down)
                    .setSize(size)
                    .build();
        });

        processedStream.to(
                KAFKA_OUTPUT_TOPIC,
                Produced.with(Serdes.String(), augmentedSerde)
        );

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = setupShutdownHook(streams);

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static Properties createStreamsConfig() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KAFKA_GROUP);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass()); // Fallback
        return props;
    }

    private static CountDownLatch setupShutdownHook(KafkaStreams streams) {
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                System.out.println("Total bytes sent: " + byteCount.get());
                latch.countDown();
            }
        });
        return latch;
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