"""
Shared utilities for the production inference and label enrichment pipeline.
"""


###############################################################################
# Imports
###############################################################################

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup


###############################################################################
# Table configuration
###############################################################################

spine_table = f"{catalog}.{database}.gold_abandonment_spine"
customer_profile_table = f"{catalog}.{database}.gold_user_profile"
customer_agg_table = f"{catalog}.{database}.gold_user_behavior_features" 
inference_enriched_table = f"{catalog}.{database}.gold_abandonment_inference_enriched"
fraud_labels_table = f"{catalog}.{database}.bronze_labels"


###############################################################################
# Feature store configuration
###############################################################################

entity_key = "user_id"
timestamp_key = "event_timestamp"

# 1. Perfiles estáticos, demográficos y flags booleanos
# Mezcla de tus categorical_columns, boolean_columns y numéricos no agregados de 07_Utils
profile_feature_names = [
    "age",
    "gender",
    "country",
    "user_type",
    "preferred_device",
    "favourite_category",
    "age_group",
    "loyalty_segment",
    "return_rate",
    "has_app_installed",
    "email_opt_in",
    "push_opt_in"
]

# 2. Agregaciones comportamentales (ventanas deslizantes)
# Todos los prefijos count_, sum_, avg_, max_, distinct_ y ratios de tu 07_Utils
aggregation_feature_names = [
    "count_events_1h",
    "sum_cart_value_1h",
    "avg_cart_value_1h",
    "distinct_categories_1h",
    "count_sessions_24h",
    "sum_cart_value_24h",
    "avg_cart_value_24h",
    "max_cart_value_24h",
    "distinct_categories_24h",
    "count_add_to_cart_24h",
    "count_events_7d",
    "sum_cart_value_7d",
    "distinct_categories_7d",
    "count_events_30d",
    "sum_cart_value_30d",
    "avg_cart_value_30d",
    "num_abandoned_confirmed_30d",
    "cart_value_24h_vs_avg_30d_ratio"
]

profile_lookup = FeatureLookup(
    table_name = customer_profile_table,
    feature_names = profile_feature_names,
    lookup_key = entity_key,
    timestamp_lookup_key = timestamp_key
)

aggregations_lookup = FeatureLookup(
    table_name = customer_agg_table,
    feature_names = aggregation_feature_names,
    lookup_key = entity_key,
    timestamp_lookup_key = timestamp_key
)

feature_lookups = [profile_lookup, aggregations_lookup]

print(f"Profile features ({len(profile_feature_names)}): {profile_feature_names}")
print(f"Aggregation features ({len(aggregation_feature_names)}): {aggregation_feature_names}")
print(f"Total feature columns: {len(profile_feature_names) + len(aggregation_feature_names)}")
print()

print("09_Utils.py script loaded successfully.")
