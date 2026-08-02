import pandas as pd
import numpy as np

# Generate 500,000 telemetry rows
num_records = 500000

# Simulating data skew: "FreightLiner" truck generates 80% of the logs
models = np.random.choice(
    ['FreightLiner', 'TransitVan', 'Sprinter', 'SemiTractor'], 
    num_records, 
    p=[0.8, 0.1, 0.05, 0.05] 
)

data = {
    'vehicle_id': np.random.randint(1000, 9999, num_records),
    'vehicle_model': models,
    'engine_heat': np.random.normal(190, 15, num_records).round(2), # Normal temp distribution
    'speed': np.random.randint(0, 75, num_records),
    'location_lat': np.random.uniform(30.0, 45.0, num_records).round(4),
    'location_lon': np.random.uniform(-120.0, -70.0, num_records).round(4),
    'battery_efficiency': np.random.uniform(70, 100, num_records).round(1)
}

df = pd.DataFrame(data)
df.to_csv('telemetry_data.csv', index=False)
print("Data generated successfully!")