import pandas as pd
import os
import glob
from datetime import datetime

def extract_date_from_folder(folder_path):
    """
    Extract date from folder name like 'BA900_2022-01-01_zipcsv'
    """
    folder_name = os.path.basename(folder_path)
    try:
        date_str = folder_name.split('_')[1]  # Gets '2022-01-01'
        return datetime.strptime(date_str, '%Y-%m-%d').strftime('%Y-%m')
    except:
        return folder_name

def extract_bank_npl_data(csv_file_path, extracted_date):
    """
    Extract NPL data from a single BA900 CSV file
    """
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except:
        return None
    
    # Extract institution name from the file
    institution = "Unknown"
    for line in lines[:5]:  # Check first 5 lines
        if line.lower().startswith('institution'):
            institution = line.split(',')[1].strip() if ',' in line else "Unknown"
            break
    
    gross_loans = 0
    credit_impairments = 0
    
    # Extract loan and impairment data
    for line in lines:
        line_parts = line.split(',')
        line_lower = line.lower()
        
        # GROSS LOANS - Sum from key loan categories
        if any(keyword in line_lower for keyword in [
            'mortgage advances (total',
            'instalment debtors, suspensive sales and leases (total',
            'creditcard debtors (total', 
            'overdrafts, loans and advances: private sector (total'
        ]):
            if len(line_parts) > 6:
                try:
                    amount = float(line_parts[6].strip()) if line_parts[6].strip() else 0
                    gross_loans += amount
                except:
                    pass
        
        # CREDIT IMPAIRMENTS - Item 194
        elif 'credit impairments in respect of loans and advances' in line_lower:
            if len(line_parts) > 6:
                try:
                    credit_impairments = abs(float(line_parts[6].strip())) if line_parts[6].strip() else 0
                except:
                    pass
    
    # Calculate NPL ratio
    npl_ratio = (credit_impairments / gross_loans * 100) if gross_loans > 0 else 0
    
    return {
        'institution': institution,
        'date': extracted_date,
        'file_path': csv_file_path,
        'gross_loans': gross_loans,
        'credit_impairments': credit_impairments,
        'npl_ratio': npl_ratio
    }

def process_all_files(data_directory='test_data'):
    """
    Process ALL CSV files in the test_data directory structure
    """
    results = []
    processed_count = 0
    error_count = 0
    
    print(f"🔍 Scanning directory: {data_directory}")
    
    # Walk through all subdirectories
    for root, dirs, files in os.walk(data_directory):
        # Skip if not a BA900 folder
        if 'BA900_' not in os.path.basename(root):
            continue
            
        # Extract date from folder name
        folder_date = extract_date_from_folder(root)
        print(f"\n📁 Processing folder: {os.path.basename(root)} -> {folder_date}")
        
        # Process all CSV files in this folder
        csv_files = [f for f in files if f.endswith('.csv') and 'TOTAL' not in f.upper()]
        
        for csv_file in csv_files:
            csv_path = os.path.join(root, csv_file)
            
            try:
                data = extract_bank_npl_data(csv_path, folder_date)
                if data and data['gross_loans'] > 0:  # Only include banks with actual loan data
                    results.append(data)
                    processed_count += 1
                    print(f"  ✓ {data['institution'][:30]:<30} NPL: {data['npl_ratio']:.2f}%")
                else:
                    print(f"  ⚠ {csv_file:<30} No loan data found")
                    
            except Exception as e:
                error_count += 1
                print(f"  ✗ {csv_file:<30} Error: {str(e)[:50]}")
    
    # Create DataFrame
    if results:
        df = pd.DataFrame(results)
        
        # Clean and standardize data
        df['institution'] = df['institution'].str.strip().str.upper()
        df['date'] = pd.to_datetime(df['date'], format='%Y-%m', errors='coerce')
        
        # Remove duplicates and sort
        df = df.drop_duplicates(subset=['institution', 'date'])
        df = df.sort_values(['date', 'institution'])
        
        # Save results
        df.to_csv('npl_data_all_banks.csv', index=False)
        
        print(f"\n📊 EXTRACTION COMPLETE!")
        print(f"✓ Successfully processed: {processed_count} files")
        print(f"✗ Errors: {error_count} files")
        print(f"🏦 Unique banks: {df['institution'].nunique()}")
        print(f"📅 Time periods: {len(df['date'].unique())} months")
        print(f"📈 Average NPL ratio: {df['npl_ratio'].mean():.2f}%")
        
        # Show summary by time period
        print(f"\n📅 DATA BY TIME PERIOD:")
        summary = df.groupby('date').agg({
            'institution': 'count',
            'npl_ratio': 'mean'
        }).round(2)
        summary.columns = ['Banks', 'Avg_NPL_Ratio']
        print(summary.to_string())
        
        return df
    else:
        print("❌ No data extracted!")
        return pd.DataFrame()

def extract_npl():
    """
    Main execution function
    """
    print("🚀 Starting bulk NPL data extraction...")
    
    # Process all files
    df_npl = process_all_files('test_data')
    
    if not df_npl.empty:
        # Display sample results
        print(f"\n📋 SAMPLE RESULTS:")
        print(df_npl[['institution', 'date', 'gross_loans', 'npl_ratio']].head(10).to_string(index=False))
        
        # Export for analysis
        print(f"\n💾 Data saved to: npl_data_all_banks.csv")
        print(f"Ready for ML model with {len(df_npl)} bank-date observations!")
        
        return df_npl
    else:
        print("❌ No data to export")
        return None

if __name__ == "__main__":
    df = extract_npl()