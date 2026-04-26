import pandas as pd

url = "https://brn-etl-0311.s3.us-east-1.amazonaws.com/MOCK_DATA.csv"

df = pd.read_csv(url)
print(f"1. CSV lido: {len(df)} linhas")

df['card_number'] = df['card_number'].astype(str)
print("2. card_number convertido")

df['date'] = pd.to_datetime(df['date'])
print("3. date convertido")

antes = len(df)
df = df[(df['amount'].notna()) & (df['amount'] >= 0)]
depois = len(df)
print(f"4. Removidas {antes - depois} linhas inválidas")

df.to_csv('transacoes_limpas.csv', index=False)
print("5. Arquivo salvo: transacoes_limpas.csv")

print(f'Total de linhas após limpeza: {len(df)}')