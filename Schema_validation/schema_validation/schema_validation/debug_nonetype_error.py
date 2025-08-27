"""
Debug Script for NoneType Error in Schema Validation
This script helps identify where the NoneType error is occurring
"""

from schemaValidLib.utils import Utils
from pyspark.sql.functions import col

def debug_nonetype_error():
    """
    Step-by-step debugging to identify NoneType error source
    """
    print("=== DEBUGGING NONETYPE ERROR ===\n")
    
    try:
        # Step 1: Initialize Utils
        print("1. Initializing Utils...")
        test_util = Utils()
        print("   ✅ Utils initialized successfully")
        
        # Step 2: Check widget initialization
        print("\n2. Checking widget initialization...")
        if test_util.widget is None:
            print("   ❌ Widget is None")
            return
        print("   ✅ Widget initialized successfully")
        
        # Step 3: Check widget methods
        print("\n3. Checking widget methods...")
        try:
            stack_type = test_util.widget.getStackType()
            print(f"   Stack Type: {stack_type}")
        except Exception as e:
            print(f"   ❌ Error getting stack type: {e}")
            
        try:
            platform_family = test_util.widget.getPlatformFamily()
            print(f"   Platform Family: {platform_family}")
        except Exception as e:
            print(f"   ❌ Error getting platform family: {e}")
            
        try:
            error_message = test_util.widget.getErrorMessage()
            print(f"   Error Message: {error_message}")
        except Exception as e:
            print(f"   ❌ Error getting error message: {e}")
        
        # Step 4: Check product family names
        print("\n4. Checking product family names...")
        try:
            product_names = test_util.get_requiredProductFamilyNames()
            print(f"   Product Names: {product_names}")
            print(f"   Type: {type(product_names)}")
        except Exception as e:
            print(f"   ❌ Error getting product family names: {e}")
            
        # Step 5: Check products attribute
        print("\n5. Checking products attribute...")
        if hasattr(test_util, 'products'):
            print(f"   Products: {test_util.products}")
            print(f"   Type: {type(test_util.products)}")
        else:
            print("   ❌ Products attribute not found")
            
        # Step 6: Try to load data (this is often where NoneType errors occur)
        print("\n6. Attempting to load data...")
        try:
            # First display widgets to set values
            print("   Displaying widgets for configuration...")
            test_util.widget.displayWidgets()
            print("   ✅ Widgets displayed successfully")
            
        except Exception as e:
            print(f"   ❌ Error displaying widgets: {e}")
            return
            
        # Step 7: Try loading DataFrame
        print("\n7. Attempting to load DataFrame...")
        try:
            df_test = test_util.loadDf()
            if df_test is None:
                print("   ❌ DataFrame is None")
                return
            print(f"   ✅ DataFrame loaded successfully. Count: {df_test.count()}")
            
        except Exception as e:
            print(f"   ❌ Error loading DataFrame: {e}")
            print(f"   Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            return
            
        # Step 8: Try extracting data
        print("\n8. Attempting to extract data...")
        try:
            extracted_df = test_util.get_extractData(df_test)
            if extracted_df is None:
                print("   ❌ Extracted DataFrame is None")
                return
            print(f"   ✅ Data extracted successfully. Count: {extracted_df.count()}")
            
        except Exception as e:
            print(f"   ❌ Error extracting data: {e}")
            print(f"   Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            return
            
        # Step 9: Try schema validation processing
        print("\n9. Attempting schema validation processing...")
        try:
            test_util.getSchemaValidationProcessed(extracted_df)
            print("   ✅ Schema validation processing completed successfully")
            
        except Exception as e:
            print(f"   ❌ Error in schema validation processing: {e}")
            print(f"   Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            return
            
        print("\n=== DEBUGGING COMPLETE - NO ERRORS FOUND ===")
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_nonetype_error()
