import requests

url = "https://brn-etl-0311.s3.us-east-1.amazonaws.com/MOCK_DATA.csv"

print("Baixando arquivo...")

resposta = requests.get(url)
linhas = resposta.text.split("\n")

print("Primeiras 5 linhas: ")
for i in range(5):  
    print(linhas[i])