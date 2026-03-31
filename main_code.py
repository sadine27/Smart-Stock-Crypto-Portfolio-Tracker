import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv
import time

load_dotenv()

def extract_data():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    headers =  {
        "x-cg-demo-api-key" : os.environ.get("coin_geko_api_key")
    }
    response = requests.get(url,headers=headers)
    data = response.json()
    return(data)

# data = extract_data()

def make_DataJson(data):
    with open("coin_list.json","w") as f:
        json.dump(data,f,indent=4)
    print("coin_list.json created successfully")
    return()

# make_DataJson(data)

def select_protfolio_coins(data):
    req_coins = []
    entry = {}
    protfolio = pd.read_csv("./raw_protfolio.csv")
    for Ticker in protfolio["Ticker"]:
        for coin in data:
            if Ticker.lower() == coin["symbol"].lower():
                entry = {
                    "id" : coin.get("id","Not Found"),
                    "Ticker" : coin.get("symbol","Not Found").upper(),
                    "name" : coin.get("name","Not Found"),
                    "current price per coin (USD)" : coin.get("current_price","Not Found"),
                    "highest in last 24 hours per coin (USD)" : coin.get("high_24h","Not Found"),
                    "lowest in last 24 hours per coin (USD)" : coin.get("low_24h","Not Found"),
                    "price changes in last 24 hours per coin (USD)" : coin.get("price_change_24h","Not Found"),
                    "price change % : last 24 hours per coin" : coin.get("price_change_percentage_24h","Not Found"),
                    "last updated" : coin.get("last_updated","Not Found")
                }
                req_coins.append(entry)
    with open("coin_list.json","w") as f:
        json.dump(req_coins,f,indent=4)
    print("coin_list.json updated successfully")
    return(req_coins)

# req_coins = select_protfolio_coins(data)

def make_CoinCSV(req_coins):
    coins = pd.DataFrame(req_coins)
    raw_protfolio = pd.read_csv("raw_protfolio.csv")
    coins = pd.merge(coins,raw_protfolio,how="left",on="Ticker")
    coins["your current value (USD)"] = coins["Quantity"] * coins["current price per coin (USD)"]
    coins["your highest in last 24 hours (USD)"] = coins["highest in last 24 hours per coin (USD)"] * coins["Quantity"]
    coins["your lowest in last 24 hours (USD)"] = coins["lowest in last 24 hours per coin (USD)"] * coins["Quantity"]
    coins["your price changes in last 24 hours (USD)"] = coins["price changes in last 24 hours per coin (USD)"] * coins["Quantity"]
    coins["your price changes in last 24 hours % (USD)"] = coins["price change % : last 24 hours per coin"] * coins["Quantity"]
    coins["income statement (USD)"] = (coins["current price per coin (USD)"] - coins["Average_Purchase_Price"])*coins["Quantity"]
    coins["income statement % "] = ((coins["current price per coin (USD)"] - coins["Average_Purchase_Price"])*100)/coins["Average_Purchase_Price"]
    net_income_statement = coins["income statement (USD)"].sum()
    protfolio_coins = coins["your current value (USD)"].sum()
    coins = coins.drop("highest in last 24 hours per coin (USD)",axis=1)
    coins = coins.drop("lowest in last 24 hours per coin (USD)",axis=1)
    coins = coins.drop("price changes in last 24 hours per coin (USD)",axis=1)
    coins = coins.drop("price change % : last 24 hours per coin",axis=1)
    coins.to_csv("Coin_Info.csv",index=False)
    conclusion = {
        "net income statement" : net_income_statement,
        "protfolio cryptocurrency" : protfolio_coins
    }
    print("File Successfully Created")
    return(conclusion)

# conclusion  = make_CoinCSV(req_coins)

def extract_stock():
    master_data = []
    data_df = pd.read_csv("raw_protfolio.csv")
    data_df = data_df[data_df["Asset_Type"] == "Stock"]
    for stock in data_df["Ticker"].items():
        stock_symbol = stock[1]
        payload = {
            "apikey" : os.environ.get("alpha_vintage_api_key"),
            "function" : "TIME_SERIES_DAILY",
            "symbol" : stock_symbol
        }
        r = requests.get("https://www.alphavantage.co/query",params=payload)
        time.sleep(1)
        print("extraction successful")
        data = r.json()
        master_data.append(data)
    with open ("stock_list.json","w") as f:
        json.dump(master_data,f,indent=4)
    return(master_data)

master_data = extract_stock()

def refine_data(master_data):
    req_data = []
    for stock in master_data:
        Time = stock.get("Meta Data").get("3. Last Refreshed","Not Found")
        entry = {
            "Ticker" : stock.get("Meta Data").get("2. Symbol","Not Found"),
            "Time" : stock.get("Meta Data").get("3. Last Refreshed","Not Found"),
            "open" : stock.get("Time Series (Daily)").get(Time).get("1. open","Not Found"),
            "high" : stock.get("Time Series (Daily)").get(Time).get("2. high","Not Found"),
            "low" : stock.get("Time Series (Daily)").get(Time).get("3. low","Not Found"),
            "close" : stock.get("Time Series (Daily)").get(Time).get("4. close","Not Found"),
            "volume" : stock.get("Time Series (Daily)").get(Time).get("5. volume","Not Found")   
        }
        req_data.append(entry)
    return(req_data)

req_data = refine_data(master_data)

def create_csv(req_data):
    Stock_Info = pd.DataFrame(req_data)
    raw_protfolio = pd.read_csv("raw_protfolio.csv")
    Stock_Info["close"] = pd.to_numeric(Stock_Info["close"])
    Stock_Info["open"] = pd.to_numeric(Stock_Info["open"])
    Stock_Info = pd.merge(Stock_Info,raw_protfolio,how="left",on="Ticker")
    Stock_Info["Income Statement (USD)"] = (Stock_Info["close"] - Stock_Info["Average_Purchase_Price"])*Stock_Info["Quantity"]
    Stock_Info["Income Statement %"] = (Stock_Info["Income Statement (USD)"]*100)/(Stock_Info["Average_Purchase_Price"]*Stock_Info["Quantity"])
    net_income_statement = Stock_Info["Income Statement (USD)"].sum()
    protfolio = (Stock_Info["close"] * Stock_Info["Quantity"]).sum()
    conclusionI = {
        "net income statement" : net_income_statement,
        "protfolio stock" : protfolio
    }
    print(Stock_Info)
    Stock_Info.to_csv("Stock_Info.csv",index=False)
    return(conclusionI)

conclusionI = create_csv(req_data)

# def Merge_Csv():


def web_hook(conclusion,conclusionI):
    with open ("Coin_Info.csv","rb") as f:
        files = {
            "data" : ("Coin_Info.csv",f,"text/csv")
        }
        payload = {
            "net income statement cryptocurrency" : conclusion["net income statement"],
            "protfolio cryptocurrency" : conclusion["protfolio cryptocurrency"]
        }
        response = requests.post(os.environ.get("n8n_web_hook"),files=files,data=payload)
    return()

# web_hook(conclusion)