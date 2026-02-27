"""
Bronze Validation Test Suite

This module verifies the deterministic data quality rules applied in the
Bronze Validation pipeline. It ensures that the Densmore-style contract
and rule logic behave predictably across multiple scenarios.

The suite covers:

1. Rule Application & Row Separation
   - Confirms apply_rules correctly splits valid vs. invalid rows.
   - Ensures DQ results emit one record per rule per batch.

2. Schema Validation
   - Detects missing or incorrect columns relative to EXPECTED_SCHEMA.

3. Required Fields Rule
   - Ensures null identifiers (customerId, contentId, clientId, eventDate)
     are flagged and excluded from valid output.

4. Bufferings Non-Negative Rule
   - Validates that negative player.bufferings values fail the rule.

5. Responses Range Rule
   - Ensures totalDistribution.sourceTraffic.responses stays within [0, 1].

These tests guarantee that Bronze validation produces clean, contract-compliant
data and reliable DQ observability for downstream Silver processing.
"""


from pyspark.sql import Row
from pipeline.src.validation_bronze import apply_rules, EXPECTED_SCHEMA


def test_apply_rules_valid_and_invalid_rows(spark):
    """Ensure apply_rules correctly separates valid and invalid rows."""
    
    df = spark.createDataFrame([
        # VALID ROW
        {
            "customerId": "c1",
            "contentId": "v1",
            "clientId": "x",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": 2, "bufferingTime": 300},
            "totalDistribution": {"sourceTraffic": {"responses": 1}},
            "qualityDistribution": {"720p": 1}
        },
        # INVALID: missing customerId
        {
            "customerId": None,
            "contentId": "v2",
            "clientId": "y",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": 1, "bufferingTime": 200},
            "totalDistribution": {"sourceTraffic": {"responses": 1}},
            "qualityDistribution": {"720p": 1}
        },
        # INVALID: negative bufferings
        {
            "customerId": "c3",
            "contentId": "v3",
            "clientId": "z",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": -1, "bufferingTime": 200},
            "totalDistribution": {"sourceTraffic": {"responses": 1}},
            "qualityDistribution": {"720p": 1}
        },
        # INVALID: responses > 1
        {
            "customerId": "c4",
            "contentId": "v4",
            "clientId": "w",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": 1, "bufferingTime": 200},
            "totalDistribution": {"sourceTraffic": {"responses": 5}},
            "qualityDistribution": {"720p": 1}
        }
    ])

    valid_df, dq_df = apply_rules(df)

    # ---- VALID ROWS ----
    assert valid_df.count() == 1
    row = valid_df.collect()[0]
    assert row.customerId == "c1"
    assert row.contentId == "v1"

    # ---- DQ RESULTS ----
    assert dq_df.count() == 4  # 4 rules

    rule_names = {r.rule_name for r in dq_df.collect()}
    assert rule_names == {
        "schema_validation",
        "required_fields_not_null",
        "bufferings_non_negative",
        "responses_between_0_and_1"
    }


def test_schema_validation_detects_missing_columns(spark):
    """Schema validation should detect missing or wrong-type columns."""
    
    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            # clientId missing
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": 1},
            "totalDistribution": {"sourceTraffic": {"responses": 1}},
            "qualityDistribution": {"720p": 1}
        }
    ])

    _, dq_df = apply_rules(df)

    schema_row = dq_df.filter("rule_name = 'schema_validation'").collect()[0]

    assert schema_row.failed_count == 1
    assert "clientId" in schema_row.missing_cols


def test_required_fields_rule(spark):
    """Required fields rule should catch nulls."""
    
    df = spark.createDataFrame([
        {
            "customerId": None,
            "contentId": "v1",
            "clientId": "x",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": 1},
            "totalDistribution": {"sourceTraffic": {"responses": 1}},
            "qualityDistribution": {"720p": 1}
        }
    ])

    valid_df, dq_df = apply_rules(df)

    assert valid_df.count() == 0

    rule1 = dq_df.filter("rule_name = 'required_fields_not_null'").collect()[0]
    assert rule1.failed_count == 1


def test_bufferings_non_negative_rule(spark):
    """Negative bufferings should fail rule 2."""
    
    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "clientId": "x",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": -5},
            "totalDistribution": {"sourceTraffic": {"responses": 1}},
            "qualityDistribution": {"720p": 1}
        }
    ])

    valid_df, dq_df = apply_rules(df)

    assert valid_df.count() == 0

    rule2 = dq_df.filter("rule_name = 'bufferings_non_negative'").collect()[0]
    assert rule2.failed_count == 1


def test_responses_between_0_and_1_rule(spark):
    """Responses outside [0,1] should fail rule 3."""
    
    df = spark.createDataFrame([
        {
            "customerId": "c1",
            "contentId": "v1",
            "clientId": "x",
            "eventDate": "2024-01-01",
            "timestampInfo": {"server": 100},
            "player": {"bufferings": 1},
            "totalDistribution": {"sourceTraffic": {"responses": 10}},
            "qualityDistribution": {"720p": 1}
        }
    ])

    valid_df, dq_df = apply_rules(df)

    assert valid_df.count() == 0

    rule3 = dq_df.filter("rule_name = 'responses_between_0_and_1'").collect()[0]
    assert rule3.failed_count == 1
