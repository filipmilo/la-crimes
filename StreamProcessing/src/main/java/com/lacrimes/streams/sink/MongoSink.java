package com.lacrimes.streams.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lacrimes.streams.model.Call911;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MongoSink implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MongoSink.class);
    private static final String PROCESSING_VERSION = "1.0.0";

    private final MongoClient mongoClient;
    private final MongoCollection<Document> collection;
    private final ObjectMapper objectMapper;
    private final List<Document> buffer;
    private final int batchSize;
    private final int retryAttempts;
    private final ScheduledExecutorService scheduler;

    public MongoSink(String connectionString, String database, String collectionName,
                     int batchSize, long batchTimeoutMs, int retryAttempts) {
        this.mongoClient = MongoClients.create(connectionString);
        MongoDatabase db = mongoClient.getDatabase(database);
        this.collection = db.getCollection(collectionName);
        this.objectMapper = new ObjectMapper();
        this.buffer = new ArrayList<>();
        this.batchSize = batchSize;
        this.retryAttempts = retryAttempts;

        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.scheduler.scheduleAtFixedRate(this::flushIfNotEmpty,
                batchTimeoutMs, batchTimeoutMs, TimeUnit.MILLISECONDS);

        logger.info("MongoSink initialized - collection: {}, batch size: {}, timeout: {}ms",
                collectionName, batchSize, batchTimeoutMs);
    }

    public synchronized void write(Call911 call) {
        try {
            Document doc = convertToDocument(call);
            doc.append("processed_at", Instant.now().toString())
               .append("processing_version", PROCESSING_VERSION);

            buffer.add(doc);

            if (buffer.size() >= batchSize) {
                flush();
            }
        } catch (Exception e) {
            logger.error("Failed to buffer call: {}", call.getCallId(), e);
        }
    }

    private synchronized void flushIfNotEmpty() {
        if (!buffer.isEmpty()) {
            flush();
        }
    }

    private void flush() {
        if (buffer.isEmpty()) {
            return;
        }

        List<Document> toWrite = new ArrayList<>(buffer);
        buffer.clear();

        int attempt = 0;
        boolean success = false;

        while (attempt < retryAttempts && !success) {
            try {
                attempt++;
                bulkWrite(toWrite);
                success = true;
                logger.info("Successfully wrote {} documents to MongoDB", toWrite.size());
            } catch (MongoException e) {
                logger.warn("MongoDB write failed (attempt {}/{}): {}",
                        attempt, retryAttempts, e.getMessage());
                if (attempt < retryAttempts) {
                    try {
                        Thread.sleep(1000 * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    logger.error("Failed to write {} documents after {} attempts",
                            toWrite.size(), retryAttempts, e);
                }
            }
        }
    }

    private void bulkWrite(List<Document> documents) {
        List<ReplaceOneModel<Document>> operations = new ArrayList<>();
        ReplaceOptions options = new ReplaceOptions().upsert(true);

        for (Document doc : documents) {
            String callId = doc.getString("call_id");
            Document filter = new Document("_id", callId);
            doc.append("_id", callId);
            operations.add(new ReplaceOneModel<>(filter, doc, options));
        }

        collection.bulkWrite(operations);
    }

    private Document convertToDocument(Call911 call) {
        try {
            String json = objectMapper.writeValueAsString(call);
            return Document.parse(json);
        } catch (Exception e) {
            logger.error("Failed to convert Call911 to Document: {}", call.getCallId(), e);
            throw new RuntimeException("Conversion failed", e);
        }
    }

    @Override
    public void close() {
        logger.info("Closing MongoSink - flushing remaining {} documents", buffer.size());
        flush();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        mongoClient.close();
        logger.info("MongoSink closed");
    }
}
