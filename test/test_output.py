import pandas as pd

with open("../rawdata","r") as f:
    df = pd.read_table(f, sep=",", index_col=False, error_bad_lines=False, encoding="utf-8",header=None)

df["orderId"]=df[0].str.slice(25)
df["product"]=df[1].str.slice(22).str.replace("'","")
df["price"]=df[2].str.slice(20).astype("double")
df["quantity"]=df[3].str.slice(23).str.replace(")","").astype("int")
df["quantity"]=df[3].str.slice(23).str.replace(")","").astype("int")
df["value"] = df["price"] * df["quantity"]

df = df[["orderId","product","price","quantity","value"]]

df = df.sort_values(by = ["product","orderId"])
print(df)