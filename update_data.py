import yfinance as yf
import pandas as pd
from supabase import create_client
import os

# --- 設定 (ご自身のものに書き換えてください) ---
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")
supabase = create_client(url, key)

# 取得銘柄
symbols = {
    "USDJPY": "USDJPY=X",
    "EURUSD": "EURUSD=X",
    "US10Y": "^TNX",
    "JP10Y": "JP10Y=X", # または適切なティッカー
    "EURJPY": "EURJPY=X"
}

def upload_historical_data():
    for label, ticker in symbols.items():
        print(f"Fetching {label}...")
        # 過去10年分の日足を取得
        df = yf.download(ticker, period="10y", interval="1d")
        
        # データを整形してSupabaseへ
        data_to_insert = []
        for index, row in df.iterrows():
            # 終値(Close)を取得
            val = float(row['Close'])
            if pd.isna(val): continue
            
            data_to_insert.append({
                "label": label,
                "value": val,
                "created_at": index.strftime('%Y-%m-%d')
            })
        
        # 1000件ずつ分割してアップロード
        for i in range(0, len(data_to_insert), 1000):
            supabase.table("market_data").insert(data_to_insert[i:i+1000]).execute()
            print(f"Uploaded {i} rows for {label}")

if __name__ == "__main__":
    upload_historical_data()
