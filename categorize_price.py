# Import required libraries for Pig UDF
from pig_util import outputSchema

# Define the output schema for the UDF
@outputSchema("note:chararray")
def categorize_price(total_price):
    # Check the total_price and categorize it
    if total_price >= 300:
        return "high value"  # If total_price is greater than or equal to 300, categorize as "high value"
    elif 100 <= total_price < 300:
        return "medium"      # If total_price is between 100 (inclusive) and 300 (exclusive), categorize as "medium"
    else:
        return "low value"   # If total_price is less than 100, categorize as "low value"