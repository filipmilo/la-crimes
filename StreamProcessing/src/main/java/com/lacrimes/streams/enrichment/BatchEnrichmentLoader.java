package com.lacrimes.streams.enrichment;

import com.lacrimes.streams.model.AreaStats;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.mongodb.client.model.Sorts.descending;

public class BatchEnrichmentLoader {
    private static final Logger logger = LoggerFactory.getLogger(BatchEnrichmentLoader.class);

    public static Map<String, AreaStats> load(String connectionString, String database) {
        Map<String, AreaStats> areaStatsMap = new HashMap<>();

        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase db = mongoClient.getDatabase(database);

            for (Document doc : db.getCollection("gold_bm_area_summary").find()) {
                String areaName = doc.getString("area_name");
                if (areaName == null) continue;

                AreaStats stats = new AreaStats();
                stats.setAreaPerformanceTier(doc.getString("area_performance_tier"));
                stats.setCrimeDensityCategory(doc.getString("crime_density_category"));

                Object resRate = doc.get("resolution_rate");
                if (resRate instanceof Number) {
                    stats.setResolutionRate(((Number) resRate).doubleValue());
                }

                Object pctile = doc.get("crime_density_percentile");
                if (pctile instanceof Number) {
                    stats.setCrimeDensityPercentile(((Number) pctile).doubleValue());
                }

                areaStatsMap.put(areaName.toUpperCase(), stats);
            }

            Set<String> processedRisk = new HashSet<>();
            for (Document doc : db.getCollection("gold_bm_yoy_crime_trends").find().sort(descending("occurrence_year"))) {
                String areaName = doc.getString("area_name");
                if (areaName == null) continue;

                String key = areaName.toUpperCase();
                if (processedRisk.add(key)) {
                    AreaStats stats = areaStatsMap.computeIfAbsent(key, k -> new AreaStats());
                    stats.setAreaRiskLevel(doc.getString("area_risk_level"));
                }
            }

            logger.info("Loaded batch enrichment data for {} areas", areaStatsMap.size());
        } catch (Exception e) {
            logger.error("Failed to load batch enrichment data", e);
        }

        return areaStatsMap;
    }
}
