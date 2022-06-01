# Scrit Avro para serialização de dados em python

#Imports

from asyncore import read
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

# Criando o schema
schema = avro.schema.parse(open("Projeto0001/schema_projeto001.avsc", "rb").read()) 

# Gera o arquivo para gravação com a serialização
writer = DataFileWriter(open("Projeto0001/schema_projeto001.avro", "wb"), DatumWriter(),schema)

# Adiciona os Dados
writer.append({"codigo_pedido": 1, "nome_cliente": "Vinicius Venancio","mes_compra": "05", "valor_compra": 5350})
writer.append({"codigo_pedido": 2, "nome_cliente": "Carol Traldi","mes_compra": "05", "valor_compra": 12000})

# Fecha o arquivo
writer.close()

# Leitura dos dados para verificar o resultado 
reader = DataFileReader(open("Projeto0001/schema_projeto001.avro", "rb"), DatumReader())
for i in reader:
    print(i)
    
reader.close()
