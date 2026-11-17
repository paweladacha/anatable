import pandas as pd
import numpy as np

data = {
    "customer_id": [101, 102, 103, 104, 101, 105, 106, 102, 107, 108],
    "name": ["Alice Smith", "Bob Johnson", "Charlie Brown", "Diana Prince", "Alice Smith", 
             "Evan Wright", None, "Bob Johnson", "George King", "Hannah Abbott"],
    "city": ["New York", "Chicago", np.nan, "Austin", "New York", 
             "Chicago", "Austin", np.nan, "New York", "Austin"],
    "tier": ["Gold", "Silver", "Gold", "Bronze", "Gold", 
             None, "Bronze", "Silver", "Gold", "Bronze"],
    "order_amount": [120.00, 45.50, 120.00, np.nan, 85.00, 
                     45.50, 30.00, 95.00, 120.00, 30.00]
}

df = pd.DataFrame(data)
print(df)