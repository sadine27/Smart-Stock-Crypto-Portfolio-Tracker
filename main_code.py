import requests
import pandas as pd
import json
import os
from dotenv import load_dotenv

load_dotenv()

def extract_data():
    url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
    headers =  {
        "x-cg-demo-api-key" : os.environ.get("coin_geko_api_key")
    }
    response = requests.get(url,headers=headers)
    data = response.json()
    return(data)

data = extract_data()

def make_DataJson(data):
    with open("coin_list.json","w") as f:
        json.dump(data,f,indent=4)
    print("coin_list.json created successfully")
    return()

make_DataJson(data)

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

req_coins = select_protfolio_coins(data)

def make_CoinCSV(req_coins):
    coins = pd.DataFrame(req_coins)
    raw_protfolio = pd.read_csv("raw_protfolio.csv")
    coins = pd.merge(coins,raw_protfolio,how="left",on="Ticker")
    coins["your current value (USD)"] = coins["Quantity"] * coins["current price per coin (USD)"]
    coins["your highest in last 24 hours (USD)"] = coins["highest in last 24 hours per coin (USD)"] * coins["Quantity"]
    coins["your lowest in last 24 hours (USD)"] = coins["lowest in last 24 hours per coin (USD)"] * coins["Quantity"]
    coins["your price changes in last 24 hours (USD)"] = coins["price changes in last 24 hours per coin (USD)"] * coins["Quantity"]
    coins["your price changes in last 24 hours % (USD)"] = coins["price change % : last 24 hours per coin"] * coins["Quantity"]
    coins["income statement (USD)"] = (coins["current price per coin (USD)"] - coins["Average_Purchase_Price"])
    coins["income statement % (USD)"] = ((coins["current price per coin (USD)"] - coins["Average_Purchase_Price"])*100)/coins["Average_Purchase_Price"]
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

conclusion  = make_CoinCSV(req_coins)

def web_hook(conclusion):
    with open ("Coin_Info.csv","rb") as f:
        files = {
            "data" : ("Coin_Info.csv",f,"text/csv")
        }
        payload = {
            "net income statement" : conclusion[0],
            "protfolio cryptocurrency" : conclusion[1]
        }
        response = requests.post(os.environ.get("n8n_web_hook"),files=files,data=payload)
    return()

web_hook()
    

    
