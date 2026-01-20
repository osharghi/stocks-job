import pandas as pd
import glob
import os
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

def analyze_and_export_growth(directory_path=".", window_days=5, min_price=5.0):
    all_files = glob.glob(os.path.join(directory_path, "./daily_data/*.csv"))
    valid_data_list = []

    print("Scanning files and loading price data...")
    for file in all_files:
        try:
            # Check for required columns
            cols = pd.read_csv(file, nrows=0).columns.str.strip().str.lower()
            if 'date' in cols and 'adjclose' in cols:
                df_temp = pd.read_csv(file)
                df_temp.columns = df_temp.columns.str.strip()
                valid_data_list.append(df_temp)
        except Exception:
            continue

    if not valid_data_list:
        print("No valid price history files found.")
        return

    # Combine data
    df = pd.concat(valid_data_list, ignore_index=True)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(['ticker', 'date'])

    # 1. Calculate Growth and Extract Latest Metadata
    def get_stats(group):
        if len(group) < window_days:
            return None
        
        # Get the very last row for current metrics
        latest_row = group.iloc[-1]
        
        # Growth calculation using adjClose
        start_adj = group['adjClose'].iloc[-window_days]
        end_adj = latest_row['adjClose']
        growth = (end_adj - start_adj) / start_adj if start_adj > 0 else 0
        
        # Return all requested columns from the most recent day
        return pd.Series({
            'date': latest_row['date'],
            'close': latest_row['close'],
            'volume': latest_row['volume'],
            'adjClose': latest_row['adjClose'],
            'splitFactor': latest_row.get('splitFactor', 1.0),
            'marketCap': latest_row.get('marketCap', 'N/A'),
            'enterpriseVal': latest_row.get('enterpriseVal', 'N/A'),
            'peRatio': latest_row.get('peRatio', 'N/A'),
            'pbRatio': latest_row.get('pbRatio', 'N/A'),
            'trailingPEG1Y': latest_row.get('trailingPEG1Y', 'N/A'),
            '5D_Growth_Pct': growth
        })

    print("Processing and filtering tickers...")
    stats = df.groupby('ticker').apply(get_stats)
    
    # Filter: Price > $5 and Growth between 2% and 10%
    mask = (stats['close'] > min_price) & (stats['5D_Growth_Pct'] >= 0.02) & (stats['5D_Growth_Pct'] <= 0.10)
    top_50 = stats[mask].sort_values('5D_Growth_Pct', ascending=False).head(50)

    if top_50.empty:
        print("No tickers found matching the criteria.")
        return

    # --- SAVE TO CSV ---
    # Create a copy and format for the final CSV output
    csv_output = top_50.reset_index() # Move ticker from index to column
    csv_output['5D_Growth_Pct'] = (csv_output['5D_Growth_Pct'] * 100).round(2)
    
    csv_output.to_csv('./rank_results/top_50_growth_tickers.csv', index=False)
    print("CSV file created: 'top_50_growth_tickers.csv'")

    # --- GENERATE PDF ---
    print("Generating PDF plots...")
    with PdfPages('./rank_results/ticker_growth_reports.pdf') as pdf:
        ticker_list = top_50.index.tolist()
        for i in range(0, len(ticker_list), 4):
            fig, axes = plt.subplots(2, 2, figsize=(12, 10))
            plt.subplots_adjust(hspace=0.4, wspace=0.3)
            
            chunk = ticker_list[i:i+4]
            for j, ticker in enumerate(chunk):
                ax = axes[j//2, j%2]
                ticker_df = df[df['ticker'] == ticker].tail(30)
                growth_val = top_50.loc[ticker, '5D_Growth_Pct'] * 100
                
                ax.plot(ticker_df['date'], ticker_df['adjClose'], marker='o', markersize=3, color='#2ecc71')
                ax.set_title(f"{ticker} | 5D Growth: {growth_val:.2f}%")
                ax.grid(True, linestyle='--', alpha=0.6)
                plt.setp(ax.get_xticklabels(), rotation=30)
            
            for k in range(len(chunk), 4):
                fig.delaxes(axes[k//2, k%2])
                
            pdf.savefig(fig)
            plt.close()

    print("PDF file created: 'ticker_growth_reports.pdf'")
    return top_50

# Run
final_list = analyze_and_export_growth()