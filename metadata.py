# Scrit Avro para gerar arquivo de metadados em python

#Imports

from asyncore import read
import avro.schema
import copy
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json


schema = avro.schema.parse(open("Projeto0001/schema_projeto001.avsc", "rb").read()) 

# Read data from an avro file
with open("Projeto0001/schema_projeto001.avro", 'rb') as f:
    reader = DataFileReader(f, DatumReader())
    metadata = copy.deepcopy(reader.meta)
    print(metadata)
    schema_from_file = json.loads(metadata['avro.schema'])
    users = [user for user in reader]
    reader.close()


#print(f'Schema that we specified:\n {schema}')
#print(f'Schema from users.avro file:\n {schema_from_file}')
#print(f'Users:\n {users}')