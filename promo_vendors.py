from pymongo import MongoClient
from dotenv import load_dotenv
import pandas as pd
import os
import time

start_time = time.time()

load_dotenv()
# ----------------------------------------------------------------------------------------------#

mongo_promo_db = MongoClient(os.getenv("botit_promo_connection_string"))["promoprod"]
print("***** Connected to MongoDB 'Promo' successfully! *****")

print("# ------------------------------------------ #")


Redemption_transactions_pipeline = [{"$match": {"type": "Redemption", "success": True}}]
print("Loading 'Redemption_transactions' Data...")
Redemption_transactions_collection = mongo_promo_db["transactions"]
Redemption_transactionsdf = pd.DataFrame(
    list(Redemption_transactions_collection.aggregate(Redemption_transactions_pipeline))
)
print(
    f"'Redemption_transactionsdf' Data loaded: {len(Redemption_transactionsdf)} rows, {len(Redemption_transactionsdf.columns)} columns."
)

print("# ------------------------------------------ #")

Validation_transactions_pipeline = [
    {"$match": {"type": "Validation", "success": False}}
]

print("Loading 'Validation_transactions' Data...")
Validation_transactions_collection = mongo_promo_db["transactions"]
Validation_transactionsdf = pd.DataFrame(
    list(Validation_transactions_collection.aggregate(Validation_transactions_pipeline))
)
print(
    f"'Validation_transactionsdf' Data loaded: {len(Validation_transactionsdf)} rows, {len(Validation_transactionsdf.columns)} columns."
)

print("# ------------------------------------------ #")

Reversible_transactions_pipeline = [{"$match": {"type": "Reversible"}}]

print("Loading 'Reversible_transactions' Data...")
Reversible_transactions_collection = mongo_promo_db["transactions"]
Reversible_transactionsdf = pd.DataFrame(
    list(Reversible_transactions_collection.aggregate(Reversible_transactions_pipeline))
)
print(
    f"'Reversible_transactionsdf' Data loaded: {len(Reversible_transactionsdf)} rows, {len(Reversible_transactionsdf.columns)} columns."
)
Reversible_transactionsdf.head()

print("# ------------------------------------------ #")

print("# ------------------------------------------ #")

print("Loading 'promocode_names' Data...")
promocode_names_collection = mongo_promo_db["promocodes"]
promocode_namesdf = pd.DataFrame(list(promocode_names_collection.find()))
print(
    f"'promocode_namesdf' Data loaded: {len(promocode_namesdf)} rows, {len(promocode_namesdf.columns)} columns."
)

promocode_namesdf["_id"] = promocode_namesdf["_id"].astype(str)
Validation_transactionsdf["promoCodeId"] = Validation_transactionsdf[
    "promoCodeId"
].astype(str)
Validation_transactionsdfaaa = pd.merge(
    Validation_transactionsdf,
    promocode_namesdf,
    left_on="promoCodeId",
    right_on="_id",
    how="left",
)

Redemption_transactionsdf["promoCodeId"] = Redemption_transactionsdf[
    "promoCodeId"
].astype(str)
Redemption_transactionsdf = pd.merge(
    Redemption_transactionsdf,
    promocode_namesdf,
    left_on="promoCodeId",
    right_on="_id",
    how="left",
)

# ----------------------------------------------------------------------------------------------#

mongo_connection_string = os.getenv("botit_mongo_connection_string")
mongo_client = MongoClient(mongo_connection_string)
mongo_db = mongo_client["botitprod"]
print("***** Connected to MongoDB 'Production' successfully! *****")

print("# ------------------------------------------ #")

orders_pipeline = [
    {
        "$project": {
            "_id": 1,
            "_vendor": 1,
            "_cart": 1,
            "price.total": 1,
            "createdAt": 1,
        }
    }
]
print("Loading 'Orders' Data...")
orders_collection = mongo_db["Orders"]
ordersdf = pd.DataFrame(list(orders_collection.aggregate(orders_pipeline)))
print(f"'Orders' Data loaded: {len(ordersdf)} rows, {len(ordersdf.columns)} columns.")

print("# ------------------------------------------ #")

carts_pipeline = [
    {
        "$project": {
            "_id": 1,
            "_vendor": 1,
            "createdAt": 1,
        }
    }
]
print("Loading 'Carts' Data...")
carts_collection = mongo_db["Carts"]
cartsdf = pd.DataFrame(list(carts_collection.aggregate(carts_pipeline)))
print(f"'Carts' Data loaded: {len(cartsdf)} rows, {len(cartsdf.columns)} columns.")

print("# ------------------------------------------ #")

Vendors_pipeline = [
    {
        "$project": {
            "_id": 1,
            "name.en": 1,
            "shoppingCategory": 1,
            "integration.system": 1,
        }
    }
]
print("Loading 'Vendors' Data...")
vendors_collection = mongo_db["Vendors"]
vendorsdf = pd.DataFrame(list(vendors_collection.aggregate(Vendors_pipeline)))
print(
    f"'Vendors' Data loaded: {len(vendorsdf)} rows, {len(vendorsdf.columns)} columns."
)
print("# ------------------------------------------ #")
# ----------------------------------------------------------------------------------------------#
# promoproddf = pd.read_csv("promoprod.transactions.csv")
# print(len(promoproddf))

Validation_transactionsdf["cartId"] = Validation_transactionsdf["cartId"].astype(str)
cartsdf["_id"] = cartsdf["_id"].astype(str)

promo_cartsdf = pd.merge(
    Validation_transactionsdf, cartsdf, left_on="cartId", right_on="_id", how="left"
)

promo_cartsdf["_vendor"] = promo_cartsdf["_vendor"].astype(str)
vendorsdf["_id"] = vendorsdf["_id"].astype(str)

promo_carts_vendorsdf = pd.merge(
    promo_cartsdf, vendorsdf, left_on="_vendor", right_on="_id", how="left"
)

promo_carts_vendorsdf["vendorname"] = promo_carts_vendorsdf["name"].apply(
    lambda x: x.get("en") if isinstance(x, dict) else None
)

promo_carts_vendorsdf = promo_carts_vendorsdf[
    [
        "_id_x",
        "userId",
        "cartId",
        "type",
        "success",
        "vendorname",
        "shoppingCategory",
        "createdAt_x",
    ]
]

# ----------------------------------------------------------------------------------------------#


Redemption_transactionsdf["cartId"] = Redemption_transactionsdf["cartId"].astype(str)
ordersdf["_cart"] = ordersdf["_cart"].astype(str)

promo_ordersdf = pd.merge(
    Redemption_transactionsdf, ordersdf, left_on="cartId", right_on="_cart", how="left"
)

promo_ordersdf = promo_ordersdf.rename(
    columns={
        "_id_x": "Redemption_transactions_Id",
        "_id_y": "promocodes_Id",
        "_id": "Orders_Id",
        "cartId": "transaction_cartId",
        "_cart": "order_cartId",
    }
)
promo_ordersdf.head()

promo_ordersdf["_vendor"] = promo_ordersdf["_vendor"].astype(str)
vendorsdf["_id"] = vendorsdf["_id"].astype(str)

promo_orders_vendorsdf = pd.merge(
    promo_ordersdf, vendorsdf, left_on="_vendor", right_on="_id", how="left"
)

promo_orders_vendorsdf = promo_orders_vendorsdf.rename(
    columns={
        "_id_x": "transaction_id",
        "_id_y": "order_id",
        "_vendor": "vendor_id",
        "_cart": "cart_id",
    }
)

promo_orders_vendorsdf["vendorname"] = promo_orders_vendorsdf["name"].apply(
    lambda x: x.get("en") if isinstance(x, dict) else None
)
promo_orders_vendorsdf["totalPrice"] = promo_orders_vendorsdf["price"].apply(
    lambda x: x.get("total") if isinstance(x, dict) else None
)

promo_orders_vendorsdf = promo_orders_vendorsdf[
    [
        "Redemption_transactions_Id",
        "label",
        "type_x",
        "success",
        "userId",
        "Orders_Id",
        "vendorname",
        "shoppingCategory",
        "promoDiscount",
        "totalPrice",
        "createdAt_y",
        "createdAt_x",
        "code",
        "type_y",
        "value",
        "specialType",
    ]
]

promo_orders_vendorsdf = promo_orders_vendorsdf.rename(
    columns={
        "shoppingCategory": "Vendor_ShoppingCategory",
        "promoDiscount": "Promo_Discount",
        "totalPrice": "Total_Price",
        "createdAt_y": "Orders_createdAt",
        "createdAt_x": "transaction_createdAt",
        "code": "Promo_Code",
        "type_y": "promo_type",
        "value": "Promo_Value",
    }
)
Reversible_transactionsdf["reversedTransactionId"] = Reversible_transactionsdf[
    "reversedTransactionId"
].astype(str)
promo_orders_vendorsdf["Redemption_transactions_Id"] = promo_orders_vendorsdf[
    "Redemption_transactions_Id"
].astype(str)
promo_orders_vendorsdf = promo_orders_vendorsdf.merge(
    Reversible_transactionsdf[["reversedTransactionId"]],
    left_on="Redemption_transactions_Id",
    right_on="reversedTransactionId",
    how="left",
    indicator=True,
)

promo_orders_vendorsdf["cancelled"] = promo_orders_vendorsdf["_merge"].apply(
    lambda x: "Cancelled" if x == "both" else " "
)
promo_orders_vendorsdf.drop(columns=["_merge"], inplace=True)
promo_orders_vendorsdf


print(
    f"Size of 'promo_carts_vendorsdf': {promo_carts_vendorsdf.shape[0]} rows, {promo_carts_vendorsdf.shape[1]} columns"
)
print(
    f"Size of 'promo_orders_vendorsdf': {promo_orders_vendorsdf.shape[0]} rows, {promo_orders_vendorsdf.shape[1]} columns"
)

promo_orders_vendorsdf = promo_orders_vendorsdf.astype(str)
promo_carts_vendorsdf = promo_carts_vendorsdf.astype(str)
print("# ------------------------------------------ #")

promo_carts_vendorsdf.to_csv("promo_data_csv/falseValidation.csv", index=False)
promo_orders_vendorsdf.to_csv("promo_data_csv/trueRedemption.csv", index=False)


## ----------------------------------------------------- Upload To Google Sheets ----------------------------------------------------- ##
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

print("Uploading To Google Sheets...")

# Set up the Google Sheets credentials and client
service_account = os.getenv("SERVICE_ACCOUNT")
google_sheet_id = os.getenv("GOOGLE_SHEET_ID")

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(service_account, scope)
client = gspread.authorize(creds)

spreadsheet = client.open_by_key(google_sheet_id)

# Get current timestamp
current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

datasets = [
    (promo_carts_vendorsdf, f"falseValidation"),
    (promo_orders_vendorsdf, f"trueRedemption"),
]

for df, sheet_name in datasets:
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="26")

    worksheet.update([df.columns.values.tolist()] + df.values.tolist())

print("Upload Completed!")

end_time = time.time()
elapsed_time = end_time - start_time
print(f"Script execution time: {elapsed_time:.2f} seconds")
print("# ------------------------------------------ #")