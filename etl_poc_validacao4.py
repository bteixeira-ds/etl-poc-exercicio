import pandas as pd 
import psycopg2
import logging

logging.basicConfig(filename='etl_transactions.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s')

host = "192.000.00.000"
banco = "poc_mvc"
usuario = "postgres"
senha = "postgres"
porta = "5432"

df = pd.read_csv('transacoes_limpas.csv')
print(f'Total: {len(df)}')

df['valido'] = True

for i, row in df.iterrows():
    erros = []
    
    # Regras
    if row['flag'] not in [1,2,3,4,5]:
        erros.append('flag invalida')
    if len(str(row['card_number'])) != 16:
        erros.append('cartao invalido')
    if row['amount'] <= 0:
        erros.append('amount invalido')
    if row['transaction_type'] not in ['C','D']:
        erros.append('tipo invalido')
    if row['transaction_status'] not in ['accepted','denied']:
        erros.append('status invalido')
    
    if erros:
        df.at[i, 'valido'] = False
        logging.warning(f"Linha {i}: {erros}")

validos = df[df['valido'] == True]
invalidos = df[df['valido'] == False]

invalidos.to_csv('dados_invalidos.csv', index=False)
print(f"Invalidos: {len(invalidos)} salvos em dados_invalidos.csv")

if len(validos) > 0:
    conn = psycopg2.connect(host=host, database=banco, user=usuario, password=senha)
    cursor = conn.cursor()
    
    for _, row in validos.iterrows():
        cursor.execute("""
            INSERT INTO transactions VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            row['transaction_id'], row['flag'], row['card_number'], row['amount'],
            row['date'], row['merchant_id'], row['transaction_type'], row['transaction_status']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inseridos no banco: {len(validos)}")
    
print(f"\nResumo: {len(validos)} válidos, {len(invalidos)} inválidos")
print("Log salvo em etl_transactions.log")
