import yfinance as yf
import pandas_datareader.data as web
from supabase import create_client
import os
import pandas as pd
from datetime import datetime

# --- 1. Supabaseの接続設定 ---
# GitHubのSettings > Secretsで設定した名前（SUPABASE_URL, SUPABASE_KEY）を読み込みます
url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_KEY")

# 接続クライアントの作成
if url and key:
    supabase = create_client(url, key)
else:
    print("エラー: GitHubのSecretsにURLまたはKEYが設定されていません。")
    exit(1)

def fetch_and_upload():
    print(f"--- 取得開始: {datetime.now()} ---")
    
    # --- 2. 為替と米金利の取得 (Yahoo Finance) ---
    yf_symbols = {
        "USDJPY": "USDJPY=X",
        "EURUSD": "EURUSD=X",
        "EURJPY": "EURJPY=X",
        "US10Y": "^TNX"
    }
    
    for label, ticker in yf_symbols.items():
        try:
            # 最新の1分足データを取得
            df = yf.download(ticker, period="1d", interval="1m", progress=False)
            if not df.empty:
                # 最新の終値を取得（警告回避のための処理）
                val = df['Close'].iloc[-1]
                if isinstance(val, pd.Series):
                    val = val.iloc[0]
                val = float(val)
                
                # Supabaseへデータを挿入
                supabase.table("market_data").insert({"label": label, "value": val}).execute()
                print(f"成功(YF): {label} = {val}")
        except Exception as e:
            print(f"エラー(YF) {label}: {e}")

    # --- 3. 日本・欧州金利 & 政策金利の取得 (FRED) ---
    fred_symbols = {
        "JP10Y": "IRLTLT01JPM156N", # 日本国債10年
        "EU10Y": "IRLTLT01EZM156N", # ユーロ圏国債10年
        "US_RATE": "DFF",           # 米政策金利
        "JP_RATE": "INTDSRJPM193N"  # 日政策金利
    }

    for label, ticker in fred_symbols.items():
        try:
            # FREDからデータを取得
            df = web.DataReader(ticker, 'fred', start='2024-01-01')
            if not df.empty:
                # 欠損値を除いた最新の値を取得
                val = float(df[ticker].dropna().iloc[-1])
                
                # Supabaseへデータを挿入
                supabase.table("market_data").insert({"label": label, "value": val}).execute()
                print(f"成功(FRED): {label} = {val}")
        except Exception as e:
            print(f"警告(FRED) {label}: 現在取得できません")

if __name__ == "__main__":
    fetch_and_upload()
    print("--- 処理終了 ---")
