#!/bin/bash

ES_HOST="http://localhost:9200"

echo "Creating Elasticsearch indices for gold layer..."

# Job 1: Crime Count by Year (aggregation index)
curl -X PUT "$ES_HOST/gold-crime-count-by-year" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "year": {"type": "integer"},
      "count": {"type": "long"},
      "indexed_at": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "30s"
  }
}' && echo " ✓ gold-crime-count-by-year created"

# Job 2: Business Metrics (full dataset with risk scores)
curl -X PUT "$ES_HOST/gold-business-metrics" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "dr_no": {"type": "keyword"},
      "area_id": {"type": "integer"},
      "area_name": {"type": "keyword"},
      "date_occured": {"type": "date"},
      "date_reported": {"type": "date"},
      "occurrence_year": {"type": "integer"},
      "status_description": {"type": "keyword"},
      "resolution_rate": {"type": "float"},
      "area_performance_tier": {"type": "keyword"},
      "crime_density_percentile": {"type": "float"},
      "crime_density_category": {"type": "keyword"},
      "reporting_lag_days": {"type": "integer"},
      "reporting_speed": {"type": "keyword"},
      "yoy_crime_change_pct": {"type": "float"},
      "crime_trend": {"type": "keyword"},
      "area_risk_score": {"type": "float"},
      "area_risk_level": {"type": "keyword"},
      "indexed_at": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "30s"
  }
}' && echo " ✓ gold-business-metrics created"

# Job 3: Crime Categorization
curl -X PUT "$ES_HOST/gold-crime-categorization" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "dr_no": {"type": "keyword"},
      "date_occured": {"type": "date"},
      "crime_description": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
      "crime_category": {"type": "keyword"},
      "crime_severity": {"type": "keyword"},
      "is_domestic_violence": {"type": "boolean"},
      "is_violent_crime": {"type": "boolean"},
      "is_property_crime": {"type": "boolean"},
      "area_name": {"type": "keyword"},
      "premise_description": {"type": "keyword"},
      "indexed_at": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "30s"
  }
}' && echo " ✓ gold-crime-categorization created"

# Job 4: Categorical Standardization
curl -X PUT "$ES_HOST/gold-categorical-standardization" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "dr_no": {"type": "keyword"},
      "date_occured": {"type": "date"},
      "victim_age": {"type": "integer"},
      "victim_age_group": {"type": "keyword"},
      "victim_sex_clean": {"type": "keyword"},
      "victim_descent_clean": {"type": "keyword"},
      "weapon_category": {"type": "keyword"},
      "area_name": {"type": "keyword"},
      "indexed_at": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "30s"
  }
}' && echo " ✓ gold-categorical-standardization created"

# Job 5: Geographic Enrichment
curl -X PUT "$ES_HOST/gold-geographic-enrichment" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "dr_no": {"type": "keyword"},
      "date_occured": {"type": "date"},
      "location": {"type": "geo_point"},
      "lat": {"type": "float"},
      "lon": {"type": "float"},
      "has_valid_coordinates": {"type": "boolean"},
      "distance_from_center_km": {"type": "float"},
      "geographic_zone": {"type": "keyword"},
      "street_address_clean": {"type": "keyword"},
      "area_name": {"type": "keyword"},
      "area_density_category": {"type": "keyword"},
      "indexed_at": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "30s"
  }
}' && echo " ✓ gold-geographic-enrichment created"

# Job 6: Time Dimensions
curl -X PUT "$ES_HOST/gold-time-dimensions" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "dr_no": {"type": "keyword"},
      "date_occured": {"type": "date"},
      "occurrence_year": {"type": "integer"},
      "occurrence_month": {"type": "integer"},
      "occurrence_day": {"type": "integer"},
      "occurrence_hour": {"type": "integer"},
      "day_of_week": {"type": "keyword"},
      "day_of_week_num": {"type": "integer"},
      "season": {"type": "keyword"},
      "time_of_day": {"type": "keyword"},
      "is_weekend": {"type": "boolean"},
      "area_name": {"type": "keyword"},
      "indexed_at": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "30s"
  }
}' && echo " ✓ gold-time-dimensions created"

# Job 7: Data Quality
curl -X PUT "$ES_HOST/gold-data-quality" -H 'Content-Type: application/json' -d '{
  "mappings": {
    "properties": {
      "dr_no": {"type": "keyword"},
      "date_occured": {"type": "date"},
      "victim_age_clean": {"type": "integer"},
      "victim_age_group": {"type": "keyword"},
      "has_victim_info": {"type": "boolean"},
      "has_location_info": {"type": "boolean"},
      "has_weapon_info": {"type": "boolean"},
      "has_premise_info": {"type": "boolean"},
      "data_completeness_score": {"type": "float"},
      "data_quality_flag": {"type": "keyword"},
      "has_data_issues": {"type": "boolean"},
      "area_name": {"type": "keyword"},
      "indexed_at": {"type": "date"}
    }
  },
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0,
    "refresh_interval": "30s"
  }
}' && echo " ✓ gold-data-quality created"

echo ""
echo "✅ All 7 gold layer indices created successfully!"
echo ""
echo "Note: Stream processing indices (911-calls-YYYY-MM, 911-aggregations-YYYY-MM)"
echo "      are created automatically by the stream processor with dynamic mapping."
