package com.lacrimes.streams.config;

public class MongoConfig {
    private final String connectionString;
    private final String database;
    private final String collection;
    private final int batchSize;
    private final long batchTimeoutMs;
    private final int retryAttempts;

    public MongoConfig(String connectionString, String database, String collection,
                       int batchSize, long batchTimeoutMs, int retryAttempts) {
        this.connectionString = connectionString;
        this.database = database;
        this.collection = collection;
        this.batchSize = batchSize;
        this.batchTimeoutMs = batchTimeoutMs;
        this.retryAttempts = retryAttempts;
    }

    public String getConnectionString() {
        return connectionString;
    }

    public String getDatabase() {
        return database;
    }

    public String getCollection() {
        return collection;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getBatchTimeoutMs() {
        return batchTimeoutMs;
    }

    public int getRetryAttempts() {
        return retryAttempts;
    }
}
