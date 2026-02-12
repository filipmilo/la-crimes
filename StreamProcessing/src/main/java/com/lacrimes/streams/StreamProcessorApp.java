package com.lacrimes.streams;

import com.lacrimes.streams.config.KafkaConfig;
import com.lacrimes.streams.config.MongoConfig;
import com.lacrimes.streams.model.Call911;
import com.lacrimes.streams.serdes.JsonSerde;
import com.lacrimes.streams.sink.MongoSink;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class StreamProcessorApp {
    private static final Logger logger = LoggerFactory.getLogger(StreamProcessorApp.class);

    public static void main(String[] args) {
        String bootstrapServers = getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String applicationId = getEnv("APPLICATION_ID", "stream-processor-app");
        String inputTopic = getEnv("INPUT_TOPIC", "911-calls");
        String mongoConnectionString = getEnv("MONGODB_CONNECTION_STRING",
                "mongodb://admin:admin123@localhost:27017/la_crimes?authSource=admin");
        String mongoDatabase = getEnv("MONGODB_DATABASE", "la_crimes");
        String mongoCollection = getEnv("MONGODB_COLLECTION", "stream_911_calls");
        int mongoBatchSize = Integer.parseInt(getEnv("MONGODB_BATCH_SIZE", "100"));
        long mongoBatchTimeout = Long.parseLong(getEnv("MONGODB_BATCH_TIMEOUT_MS", "5000"));
        int mongoRetryAttempts = Integer.parseInt(getEnv("MONGODB_RETRY_ATTEMPTS", "3"));

        logger.info("Starting Stream Processor Application");
        logger.info("Kafka Bootstrap Servers: {}", bootstrapServers);
        logger.info("Application ID: {}", applicationId);
        logger.info("Input Topic: {}", inputTopic);
        logger.info("MongoDB Collection: {}.{}", mongoDatabase, mongoCollection);
        logger.info("Batch Size: {}, Timeout: {}ms", mongoBatchSize, mongoBatchTimeout);

        KafkaConfig kafkaConfig = new KafkaConfig(bootstrapServers, applicationId);
        MongoConfig mongoConfig = new MongoConfig(
                mongoConnectionString,
                mongoDatabase,
                mongoCollection,
                mongoBatchSize,
                mongoBatchTimeout,
                mongoRetryAttempts
        );

        MongoSink mongoSink = new MongoSink(
                mongoConfig.getConnectionString(),
                mongoConfig.getDatabase(),
                mongoConfig.getCollection(),
                mongoConfig.getBatchSize(),
                mongoConfig.getBatchTimeoutMs(),
                mongoConfig.getRetryAttempts()
        );

        StreamsBuilder builder = new StreamsBuilder();
        JsonSerde<Call911> call911Serde = new JsonSerde<>(Call911.class);

        KStream<String, Call911> callStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), call911Serde)
        );

        callStream.foreach((key, call) -> {
            try {
                mongoSink.write(call);
                logger.debug("Processed call: {} - {} in {}",
                        call.getCallId(),
                        call.getIncidentType(),
                        call.getLocation().getAreaName());
            } catch (Exception e) {
                logger.error("Failed to process call: {}", call.getCallId(), e);
            }
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaConfig.getStreamsConfig());

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                logger.info("Shutdown signal received");
                streams.close();
                mongoSink.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            logger.info("Stream processor started successfully");
            latch.await();
        } catch (Exception e) {
            logger.error("Stream processor failed", e);
            System.exit(1);
        }

        logger.info("Stream processor stopped");
        System.exit(0);
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }
}
