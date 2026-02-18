package com.lacrimes.streams;

import com.lacrimes.streams.config.KafkaConfig;
import com.lacrimes.streams.config.MongoConfig;
import com.lacrimes.streams.enrichment.BatchEnrichmentLoader;
import com.lacrimes.streams.model.*;
import com.lacrimes.streams.serdes.JsonSerde;
import com.lacrimes.streams.sink.MongoSink;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class StreamProcessorApp {
    private static final Logger logger = LoggerFactory.getLogger(StreamProcessorApp.class);

    static final Duration SLIDE_5_MIN = Duration.ofMinutes(5);
    static final TimeWindows TUMBLING_5_MIN = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5));
    static final TimeWindows TUMBLING_15_MIN = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(15));
    static final TimeWindows HOPPING_60_5 = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(60)).advanceBy(SLIDE_5_MIN);
    static final TimeWindows HOPPING_30_5 = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(30)).advanceBy(SLIDE_5_MIN);
    static final TimeWindows HOPPING_15_5 = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(15)).advanceBy(SLIDE_5_MIN);

    static final Set<String> VIOLENT_TYPES = new HashSet<>(Arrays.asList("ASSAULT", "ROBBERY", "WEAPON VIOLATION"));

    static final int TOTAL_CAP = 50;

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

        Map<String, AreaStats> areaStatsMap = BatchEnrichmentLoader.load(mongoConnectionString, mongoDatabase);
        logger.info("Loaded area stats for {} areas", areaStatsMap.size());

        Map<String, Double> historicalAvg = new HashMap<>();
        areaStatsMap.forEach((area, stats) ->
                historicalAvg.put(area, 5.0 + (stats.getCrimeDensityPercentile() / 100.0) * 10.0));

        KafkaConfig kafkaConfig = new KafkaConfig(bootstrapServers, applicationId);
        MongoConfig mongoConfig = new MongoConfig(
                mongoConnectionString, mongoDatabase, mongoCollection,
                mongoBatchSize, mongoBatchTimeout, mongoRetryAttempts);

        MongoSink originalSink = createSink(mongoConfig, mongoCollection);
        MongoSink enrichedSink = createSink(mongoConfig, "stream_911_calls_enriched");
        MongoSink escalatedSink = createSink(mongoConfig, "stream_escalated_calls");
        MongoSink spikesSink = createSink(mongoConfig, "stream_area_spikes");
        MongoSink degradationSink = createSink(mongoConfig, "stream_response_degradation");
        MongoSink utilizationSink = createSink(mongoConfig, "stream_unit_utilization");
        MongoSink failuresSink = createSink(mongoConfig, "stream_dispatch_failures");

        StreamsBuilder builder = new StreamsBuilder();
        JsonSerde<Call911> call911Serde = new JsonSerde<>(Call911.class);
        JsonSerde<DetectionWindow> detectionWindowSerde = new JsonSerde<>(DetectionWindow.class);
        JsonSerde<ResponseTimeAgg> responseTimeAggSerde = new JsonSerde<>(ResponseTimeAgg.class);
        JsonSerde<UnitCounts> unitCountsSerde = new JsonSerde<>(UnitCounts.class);
        JsonSerde<DispatchStats> dispatchStatsSerde = new JsonSerde<>(DispatchStats.class);

        Instant startupTime = Instant.now();

        KStream<String, Call911> callStream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), call911Serde));

        // NOTE:  Pass-through
        callStream.foreach((key, call) -> {
            try {
                originalSink.write(call.getCallId(), call);
                logger.debug("Processed call: {} - {} in {}",
                        call.getCallId(), call.getIncidentType(),
                        call.getLocation() != null ? call.getLocation().getAreaName() : "unknown");
            } catch (Exception e) {
                logger.error("Failed to write original call: {}", call.getCallId(), e);
            }
        });

        // NOTE: Batch enrichment
        KStream<String, Call911> enrichedStream = callStream.mapValues(call -> {
            if (call.getLocation() == null || call.getLocation().getAreaName() == null) return call;
            AreaStats stats = areaStatsMap.get(call.getLocation().getAreaName().toUpperCase());
            if (stats != null) {
                call.setAreaRiskLevel(stats.getAreaRiskLevel());
                call.setAreaPerformanceTier(stats.getAreaPerformanceTier());
                call.setCrimeDensityCategory(stats.getCrimeDensityCategory());
                call.setResolutionRate(stats.getResolutionRate());
            }
            return call;
        });

        enrichedStream.foreach((key, call) -> {
            try {
                enrichedSink.write(call.getCallId(), call);
            } catch (Exception e) {
                logger.error("Failed to write enriched call: {}", call.getCallId(), e);
            }
        });

        // NOTE: Priority escalation
        enrichedStream
                .filter((k, v) -> v.getPriority() >= 3
                        && "High Risk".equals(v.getAreaRiskLevel())
                        && v.getIncidentType() != null
                        && VIOLENT_TYPES.contains(v.getIncidentType().toUpperCase()))
                .foreach((k, v) -> {
                    try {
                        Map<String, Object> escalated = new HashMap<>();
                        escalated.put("call_id", v.getCallId());
                        escalated.put("original_priority", v.getPriority());
                        escalated.put("escalated_priority", v.getPriority() - 1);
                        escalated.put("incident_type", v.getIncidentType());
                        escalated.put("area_name", v.getLocation() != null ? v.getLocation().getAreaName() : null);
                        escalated.put("area_risk_level", v.getAreaRiskLevel());
                        escalated.put("escalation_reason", "High risk area + violent incident type");
                        escalatedSink.write(v.getCallId(), escalated);
                    } catch (Exception e) {
                        logger.error("Failed to write escalated call: {}", v.getCallId(), e);
                    }
                });

        // NOTE: Group enriched stream by area
        KGroupedStream<String, Call911> byArea = enrichedStream
                .filter((k, v) -> v.getLocation() != null && v.getLocation().getAreaName() != null)
                .groupBy((k, v) -> v.getLocation().getAreaName(),
                        Grouped.with(Serdes.String(), call911Serde));

        // NOTE: Spike detection
        KTable<String, Long> backgroundCount = byArea
                .windowedBy(HOPPING_60_5)
                .count()
                .toStream()
                .selectKey((wk, v) -> wk.key())
                .toTable(Materialized.with(Serdes.String(), Serdes.Long()));

        byArea.windowedBy(TUMBLING_5_MIN)
                .count()
                .toStream()
                .filter((wk, count) -> Duration.between(startupTime, Instant.now()).toMinutes() >= 60)
                .map((wk, count) -> KeyValue.pair(
                        wk.key(),
                        new DetectionWindow(wk.key(), wk.window().start(), wk.window().end(), count)))
                .join(backgroundCount,
                        (dw, bgCount) -> {
                            double baseline = bgCount / 12.0;
                            if (baseline == 0 || dw.getCount() <= 2 * baseline) return null;
                            Map<String, Object> spike = new HashMap<>();
                            spike.put("area_name", dw.getAreaName());
                            spike.put("window_start", dw.getWindowStart());
                            spike.put("window_end", dw.getWindowEnd());
                            spike.put("call_count", dw.getCount());
                            spike.put("baseline", baseline);
                            spike.put("spike_ratio", dw.getCount() / baseline);
                            return spike;
                        },
                        Joined.with(Serdes.String(), detectionWindowSerde, Serdes.Long()))
                .filter((k, v) -> v != null)
                .foreach((k, v) -> {
                    try {
                        spikesSink.write(k + "_" + v.get("window_start"), v);
                    } catch (Exception e) {
                        logger.error("Failed to write spike record for area: {}", k, e);
                    }
                });

        // NOTE: Response time degradation
        KGroupedStream<String, Call911> byAreaDispatched = enrichedStream
                .filter((k, v) -> v.getDispatchInfo() != null
                        && v.getDispatchInfo().isDispatched()
                        && v.getDispatchInfo().getResponseTimeMinutes() > 0
                        && v.getLocation() != null
                        && v.getLocation().getAreaName() != null)
                .groupBy((k, v) -> v.getLocation().getAreaName(),
                        Grouped.with(Serdes.String(), call911Serde));

        byAreaDispatched.windowedBy(HOPPING_30_5)
                .aggregate(
                        ResponseTimeAgg::new,
                        (k, v, agg) -> agg.add(v.getDispatchInfo().getResponseTimeMinutes()),
                        Materialized.with(Serdes.String(), responseTimeAggSerde))
                .toStream()
                .foreach((wk, agg) -> {
                    try {
                        double liveAvg = agg.avg();
                        double histAvg = historicalAvg.getOrDefault(wk.key().toUpperCase(), 8.5);
                        if (liveAvg > 1.5 * histAvg) {
                            Map<String, Object> degradation = new HashMap<>();
                            degradation.put("area_name", wk.key());
                            degradation.put("window_start", wk.window().start());
                            degradation.put("live_avg_minutes", liveAvg);
                            degradation.put("historical_avg_minutes", histAvg);
                            degradation.put("degradation_ratio", liveAvg / histAvg);
                            degradationSink.write(wk.key() + "_" + wk.window().start(), degradation);
                        }
                    } catch (Exception e) {
                        logger.error("Failed to process degradation for area: {}", wk.key(), e);
                    }
                });

        // NOTE: Unit utilization
        enrichedStream
                .filter((k, v) -> v.getDispatchInfo() != null
                        && v.getDispatchInfo().isDispatched()
                        && v.getDispatchInfo().getUnitsDispatched() != null
                        && !v.getDispatchInfo().getUnitsDispatched().isEmpty()
                        && v.getLocation() != null
                        && v.getLocation().getAreaName() != null)
                .flatMap((k, v) -> v.getDispatchInfo().getUnitsDispatched().stream()
                        .map(unit -> KeyValue.pair(v.getLocation().getAreaName(), unit))
                        .collect(Collectors.toList()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TUMBLING_15_MIN)
                .aggregate(
                        UnitCounts::new,
                        (area, unit, counts) -> counts.add(unit),
                        Materialized.with(Serdes.String(), unitCountsSerde))
                .toStream()
                .foreach((wk, counts) -> {
                    try {
                        double utilPct = (counts.total() / (double) TOTAL_CAP) * 100;
                        Map<String, Object> utilization = new HashMap<>();
                        utilization.put("area_name", wk.key());
                        utilization.put("window_start", wk.window().start());
                        utilization.put("lapd_units", counts.getLapd());
                        utilization.put("lafd_units", counts.getLafd());
                        utilization.put("ems_units", counts.getEms());
                        utilization.put("utilization_pct", utilPct);
                        utilizationSink.write(wk.key() + "_" + wk.window().start(), utilization);
                    } catch (Exception e) {
                        logger.error("Failed to write utilization for area: {}", wk.key(), e);
                    }
                });

        // NOTE: Dispatch failure rate (hopping 15-min window, high/medium risk areas)
        byArea.windowedBy(HOPPING_15_5)
                .aggregate(
                        DispatchStats::new,
                        (k, v, stats) -> {
                            boolean dispatched = v.getDispatchInfo() != null && v.getDispatchInfo().isDispatched();
                            return stats.add(dispatched);
                        },
                        Materialized.with(Serdes.String(), dispatchStatsSerde))
                .toStream()
                .filter((wk, stats) -> {
                    AreaStats area = areaStatsMap.get(wk.key().toUpperCase());
                    String riskLevel = area != null && area.getAreaRiskLevel() != null ? area.getAreaRiskLevel() : "";
                    return stats.failureRate() > 0.3
                            && ("High Risk".equals(riskLevel) || "Medium Risk".equals(riskLevel));
                })
                .foreach((wk, stats) -> {
                    try {
                        AreaStats area = areaStatsMap.getOrDefault(wk.key().toUpperCase(), new AreaStats());
                        String riskLevel = area.getAreaRiskLevel() != null ? area.getAreaRiskLevel() : "";
                        Map<String, Object> failure = new HashMap<>();
                        failure.put("area_name", wk.key());
                        failure.put("window_start", wk.window().start());
                        failure.put("failure_count", stats.getFailed());
                        failure.put("total_calls", stats.getTotal());
                        failure.put("failure_rate", stats.failureRate());
                        failure.put("area_risk_level", riskLevel);
                        failuresSink.write(wk.key() + "_" + wk.window().start(), failure);
                    } catch (Exception e) {
                        logger.error("Failed to write dispatch failure for area: {}", wk.key(), e);
                    }
                });

        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaConfig.getStreamsConfig());

        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                logger.info("Shutdown signal received");
                streams.close();
                originalSink.close();
                enrichedSink.close();
                escalatedSink.close();
                spikesSink.close();
                degradationSink.close();
                utilizationSink.close();
                failuresSink.close();
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

    private static MongoSink createSink(MongoConfig config, String collectionName) {
        return new MongoSink(
                config.getConnectionString(),
                config.getDatabase(),
                collectionName,
                config.getBatchSize(),
                config.getBatchTimeoutMs(),
                config.getRetryAttempts());
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }
}
