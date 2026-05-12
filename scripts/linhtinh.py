import pandas as pd
import numpy as np
import os

# 1. SETUP - Change this to the folder where your Olist CSVs are
INPUT_PATH = "olist_ecommerce/olist_customers_dataset.csv" 
OUTPUT_FOLDER = "./generated_data"

# Ensure the output folder exists
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

print("--- Starting Local Data Generation ---")

# 2. READ CUSTOMERS
try:
    df_customers = pd.read_csv(INPUT_PATH)
    unique_ids = df_customers['customer_unique_id'].unique()
    print(f"Read {len(unique_ids):,} unique customers.")
except FileNotFoundError:
    print(f"Error: Could not find {INPUT_PATH}. Please check the file path.")
    exit()

# 3. GENERATE CRM DATA
df_crm = pd.DataFrame({"customer_unique_id": unique_ids})

# Create email: user_ + first 8 chars of ID
# We convert to Series to avoid the 'UFuncTypeError' from earlier
df_crm['email'] = df_crm['customer_unique_id'].astype(str).str[:8].apply(lambda x: f"user_{x}@gmail.com")

# Create random phone numbers (+55-XXXXX-XXXX)
p1 = pd.Series(np.random.randint(10000, 99999, size=len(df_crm))).astype(str)
p2 = pd.Series(np.random.randint(1000, 9999, size=len(df_crm))).astype(str)
df_crm['phone'] = "+55-" + p1 + "-" + p2

# Save Locally
df_crm.to_csv(f"{OUTPUT_FOLDER}/crm_identities.csv", index=False)
print(f"✅ CRM data saved to: {OUTPUT_FOLDER}/crm_identities.csv")

# 4. GENERATE HELPDESK DATA (20% Sample)
df_helpdesk = df_crm.sample(frac=0.20, random_state=42)[['email']].copy()

# Add Ticket ID and random ratings
df_helpdesk['ticket_id'] = [f"TKT-{i:06}" for i in range(len(df_helpdesk))]
df_helpdesk['issue_type'] = np.where(np.random.rand(len(df_helpdesk)) < 0.5, 'Late Delivery', 'Damaged Item')
df_helpdesk['satisfaction_rating'] = np.random.randint(1, 6, size=len(df_helpdesk))

# Save Locally
df_helpdesk.to_csv(f"{OUTPUT_FOLDER}/helpdesk_tickets.csv", index=False)
print(f"✅ Helpdesk data saved to: {OUTPUT_FOLDER}/helpdesk_tickets.csv")

print("\nAll done! You can now manually upload these two files to MinIO.")