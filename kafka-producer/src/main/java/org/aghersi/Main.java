package org.aghersi;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
    private static final String SERVICE_ACCOUNT_KEY_FILE = "src/main/resources/drivekafkaproducer-7633fc40fb1d.json";
    private static final String FILE_ID = "1rMtEZocRy9gx9Afhc9FIYgEzanMv_4e9";
    private static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String APPLICATION_NAME = "DriveKafkaProducer";
    private static final String KAFKA_TOPIC = "input-data";
    private static final AtomicLong recordCount = new AtomicLong();
    private static final AtomicLong byteCount = new AtomicLong();

    public static void main(String[] args) {
        KafkaProducer<String, byte[]> producer = createKafkaProducer();
        try {
            Drive driveService = getDriveService();
            try (InputStream inputStream = driveService.files().get(FILE_ID).executeMediaAsInputStream();
                 BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                reader.lines().skip(1).forEach(line -> processAndSend(line, producer));
            }
            producer.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Exiting");
        System.out.println("Total records sent: " + recordCount.get());
        System.out.println("Total bytes sent: " + byteCount.get());
        producer.close();
        System.exit(0);
    }

    private static Drive getDriveService() throws IOException, GeneralSecurityException {
        JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        try (InputStream credentialsStream = Files.newInputStream(Paths.get(SERVICE_ACCOUNT_KEY_FILE))) {
            GoogleCredential credential = GoogleCredential.fromStream(credentialsStream)
                    .createScoped(Collections.singletonList(DriveScopes.DRIVE_READONLY));
            return new Drive.Builder(httpTransport, jsonFactory, credential)
                    .setApplicationName(APPLICATION_NAME)
                    .build();
        }
    }

    private static KafkaProducer<String, byte[]> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static void processAndSend(String line, KafkaProducer<String, byte[]> producer) {
        String[] parts = line.split(",");
        if (parts.length < 8) return;

        try {
            StockData stockData = StockData.newBuilder()
                    .setDate(parts[0])
                    .setOpen(parseDouble(parts[1]))
                    .setHigh(parseDouble(parts[2]))
                    .setLow(parseDouble(parts[3]))
                    .setClose(parseDouble(parts[4]))
                    .setAdjClose(parseDouble(parts[5]))
                    .setVolume(parseDouble(parts[6]))
                    .build();

            DatumWriter<StockData> datumWriter = new SpecificDatumWriter<>(StockData.class);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            datumWriter.write(stockData, encoder);
            encoder.flush();
            byte[] avroBytes = out.toByteArray();

            producer.send(new ProducerRecord<>(KAFKA_TOPIC, parts[7], avroBytes));
            recordCount.incrementAndGet();
            byteCount.addAndGet(avroBytes.length);

        } catch (Exception e) {
            System.err.println("Skipping line: " + line);
        }
    }

    private static Double parseDouble(String value) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}