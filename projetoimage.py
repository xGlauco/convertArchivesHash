import mysql.connector
import os
import shutil
from hashlib import md5

database = 'teste'
host = 'localhost'
archives = 'C:/Users/teste/teste/Teste/'

connection = mysql.connector.connect(user='root', host=host, database=database, password='')
cursor = connection.cursor()

extensions = ['.jpg', '.jpeg', '.png', '.tiff', '.bmp', '.gif']

#Converte cada arquivo para hash
def convert(archives):
    for a in os.listdir(archives):
        name_archive = a
        ext = name_archive[name_archive.rfind('.') : len(name_archive)].lower()
        name_hash = md5(name_archive.encode('utf-8')).hexdigest()
        name_hash_with_ext = name_hash + ext

        migrate(archives, name_archive, name_hash, ext)
        insert(name_archive, name_hash_with_ext, ext)

    connection.commit()
    cursor.close()

# Faz a separação dos arquivos em anexos e imagens, separando por pastas
def migrate(archives, name_archive, name_hash, ext):

    if not os.path.exists(archives + 'Anexos Convertidos'):
        os.makedirs(archives + 'Anexos Convertidos')
    
    if not os.path.exists(archives + 'Imagens Convertidas'):
        os.makedirs(archives + 'Imagens Convertidas')

    if ext in extensions:
        archivesDestiny = archives + 'Imagens Convertidas/'
        shutil.copy2(archives + name_archive, archivesDestiny + name_hash + ext)
    else:
        archivesDestiny = archives + 'Anexos Convertidos/'
        shutil.copy2(archives + name_archive, archivesDestiny + name_hash + ext)

# Insere as informações no banco de dados
def insert(name_archive, name_hash_with_ext, ext):
    createTable = """CREATE TABLE IF NOT EXISTS v_arquivos(
    name_original VARCHAR(400),
    name_hash VARCHAR(400),
    path_origin TEXT,
    type CHAR(1));"""
    cursor.execute(createTable)
    path_origin = os.path.join(archives, name_archive)
    query = """INSERT INTO v_arquivos (name_original, name_hash, path_origin, type) VALUES (%s, %s, %s, %s)"""
    typ = 'I' if ext in extensions else 'A'
    values = (name_archive, name_hash_with_ext, path_origin, typ)

    try:
        cursor.execute(query, values)
    except Exception as e:
        print(f'Erro ao inserir o dado {e}')

    if ext in extensions:
        print(f'Imagem {name_archive} separada, convertida e inserido no BD')
    else:
        print(f'Arquivo {name_archive} separado, convertido e inserido no BD')

convert(archives)
print('Finalizado!')
connection.close()