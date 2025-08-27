from schemaValidLib.utils import Utils
from IPython.display import display
from pyspark.sql.functions import col

test_util = Utils()

test_util.widget.displayWidgets()

print(test_util.widget.constant.base_name)

print(test_util.widget.getStackType())

print(test_util.get_requiredProductFamilyNames())

test_util.print_reproducibilty_schemaValidationIssue(50,150,'MALBEC')

df_test = test_util.loadDf()

print(test_util.common_columns)
print(test_util.get_sampleFw(df_test))
extracted_df = test_util.get_extractData(df_test)
display(extracted_df.limit(1))

# Enhanced testing for all three error types
print("\n=== TESTING ALL THREE ERROR TYPES ===")

# Test 1: Test error breakdown functionality
print("\n1. Testing Error Breakdown:")
test_util.get_error_breakdown(extracted_df)

# Test 2: Test eventDetailError (original functionality)
print("\n2. Testing eventDetailError:")
eventDetailErrors = extracted_df.filter(col("eventDetailError").isNotNull())
print(f"EventDetailError count: {eventDetailErrors.count()}")
if eventDetailErrors.count() > 0:
    display(eventDetailErrors.groupBy("eventDetailError").count())

# Test 3: Test originatorDetailError
print("\n3. Testing originatorDetailError:")
originatorDetailErrors = extracted_df.filter(col("originatorDetailError").isNotNull())
print(f"OriginatorDetailError count: {originatorDetailErrors.count()}")
if originatorDetailErrors.count() > 0:
    display(originatorDetailErrors.groupBy("originatorDetailError").count())

# Test 4: Test shadowEventNotificationError
print("\n4. Testing shadowEventNotificationError:")
shadowErrors = extracted_df.filter(col("shadowEventNotificationError").isNotNull())
print(f"ShadowEventNotificationError count: {shadowErrors.count()}")
if shadowErrors.count() > 0:
    display(shadowErrors.groupBy("shadowEventNotificationError").count())

# Test 5: Test combined error detection (any of the three error types)
print("\n5. Testing Combined Error Detection:")
combined_error_condition = (col("eventDetailError").isNotNull() |
                           col("originatorDetailError").isNotNull() |
                           col("shadowEventNotificationError").isNotNull())
all_errors = extracted_df.filter(combined_error_condition)
print(f"Total events with any error: {all_errors.count()}")

# Test 6: Test error-free records (all three error columns must be null)
print("\n6. Testing Error-Free Records:")
error_free_condition = (col("eventDetailError").isNull() &
                       col("originatorDetailError").isNull() &
                       col("shadowEventNotificationError").isNull())
error_free_records = extracted_df.filter(error_free_condition)
print(f"Error-free records: {error_free_records.count()}")

# Test 7: Test enhanced schema validation processing
print("\n7. Testing Enhanced Schema Validation Processing:")
test_util.getSchemaValidationProcessed(extracted_df, otherColumns=[], filterExp=None, joinType=None)

print("\n=== TESTING COMPLETE ===")

test_util.get_columnReqInSchemaValidnReport(df_test)
