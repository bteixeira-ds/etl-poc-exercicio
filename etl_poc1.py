import requests

url = "https://...csv"

print("Baixando arquivo...")

resposta = requests.get(url)
linhas = resposta.text.split("\n")

print("Primeiras 5 linhas: ")
for i in range(5):  
    print(linhas[i])
