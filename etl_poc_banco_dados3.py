import pandas as pd
import psycopg2

host = "192.000.00.000"
banco = "poc_mvc"
usuario = "postgres"
senha = "postgres"

df = pd.read_csv('transacoes_limpas.csv')
print(f'Lidos {len(df)} registros')

antes = len(df)
df = df[df['transaction_status'] == 'accepted']
depois = len(df)
print(f'Registros accepted: {depois}')
print(f'Registros denied (ignorados): {antes - depois}')

conn = psycopg2.connect(host=host, database=banco, user=usuario, password=senha)
cursor = conn.cursor()

novos = 0
for _, row in df.iterrows():
    if row['transaction_status'] == 'accepted':
        cursor.execute("""
            INSERT INTO transactions VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            row['transaction_id'], row['flag'], row['card_number'], row['amount'],
            row['date'], row['merchant_id'], row['transaction_type'], row['transaction_status']
        ))
        novos += cursor.rowcount

conn.commit()
cursor.close()
conn.close()

print('='*40)
print(f"Inseridos: {novos}")
print(f"Já existiam: {len(df) - novos}")
print(f"Denied ignorados: {antes - depois}")
print('='*40)
