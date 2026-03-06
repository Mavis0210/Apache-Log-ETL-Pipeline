import pandas as pd
import requests
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

# --- STEP 1: LOAD RAW DATA ---
# We use 'requests' to fetch the file from the internet
def fetch_logs():
 url = "https://raw.githubusercontent.com/elastic/examples/master/Common%20Data%20Formats/apache_logs/apache_logs"
 response = requests.get(url)

# Split the giant text block into a list of individual lines
 lines=response.text.split('\n')
 return lines


# --- STEP 2: PARSE (The "Chop" Method) ---
def parse_logs(lines):
 data = []

 for line in lines:
    # Check if the line is empty (garbage) before processing
    if len(line) > 10:  
        
        # KEY MOMENT: Chop the line every time we see a SPACE
        parts = line.split(' ') 
        
        # Now we pick the specific pieces we want by their Index Number
        # Example Line: 83.149.9.216 - - [17/May/2015...] "GET /home..." 200
        
        row = {
            'ip': parts[0],                          # Index 0 is always IP
            'timestamp': parts[3].replace('[', ''),  # Index 3 is Date (Remove the '[')
            'method': parts[5].replace('"', ''),     # Index 5 is Method (Remove the '"')
            'endpoint': parts[6],                    # Index 6 is the URL
            'status': parts[8],                      # Index 8 is Status Code
            'size': parts[9]                         # Index 9 is File Size
        }
        
        data.append(row)
        
        # --- STEP 3: CREATE TABLE ---
 df = pd.DataFrame(data)
 return df


#--- STEP 4: Transform the Data
def transform_data(df):
    # Converting data types
    df['status']=pd.to_numeric(df['status'],errors='coerce')
    df['size']=pd.to_numeric(df['size'],errors='coerce')
    df['timestamp']=pd.to_datetime(df['timestamp'], format="%d/%b/%Y:%H:%M:%S")
    return df

#---STEP5: Loading the database ((SQLAlchemy))
def load_to_db(df, db_name='production_logs.db'):
    engine=create_engine(f'sqlite:///{db_name}')
    df.to_sql('server_logs',con=engine,if_exists='replace',index=False)
    print(f"Loaded {len(df)} rows into '{db_name}'.")
    return engine

###----Main Execution
if __name__ == "__main__":
    print("Starting Server Log Pipeline...")

    #1. Extract
    raw_lines= fetch_logs()
    print(f" Extracted {len(raw_lines)} raw logs.")

    #2. Parse
    df = parse_logs(raw_lines) 
    print(f" Parsed {len(df)} structured rows.")

    #3. Transform
    clean_df = transform_data(df)

    #4. Load
    engine = load_to_db(clean_df)

    #5. Verify (Query)
    query = "SELECT status, COUNT(*) as Count FROM server_logs GROUP BY status ORDER BY Count DESC"
    status_report = pd.read_sql(query, con=engine)
    query2 = "SELECT ip, COUNT(*) as Request_Count FROM server_logs GROUP BY ip ORDER BY Request_Count DESC"
    ip_report = pd.read_sql(query2, con=engine)
    query3 = "SELECT endpoint, COUNT(endpoint) as Endpoint_Count FROM server_logs GROUP BY endpoint ORDER BY Endpoint_Count DESC"
    endpoint_report = pd.read_sql(query3, con=engine)

    print("\n--- ANALYSIS REPORTS ---")
    print("\nStatus Code Distribution:")
    print(status_report)

    print("\nTop 5 IP Addresses:")
    print(ip_report.head())
    print("\nTop 5 Endpoints:")
    print(endpoint_report.head())
    

    #---VISUALIZE (The Report)
    errors = clean_df[clean_df['status'] >= 400] 
    plt.figure(figsize=(10, 6))
    # Create a Bar Chart of the Top 10 Broken Pages
    top_errors = errors['status'].value_counts().head(10).index
    sns.countplot(data=errors, y='status', order=top_errors, palette='Reds',legend=False)
    plt.title('Popular Errors')
    plt.xlabel('Count of Errors')
    plt.ylabel('Status')
    plt.show()