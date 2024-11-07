import json
import csv

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_data = 'data_processed/data.csv'

def readJson(path):
    with open(path_json, 'r') as file:
        file_json = json.load(file)
    return file_json

def readCSV(path):
    with open(path_csv, 'r') as file:
        spamreader = csv.DictReader(file, delimiter=',')
        file_csv = []
        for row in spamreader:
            file_csv.append(row)
        return file_csv    
    
def leitura_dados(path, type):
    if type == 'csv':
        data = readCSV(path)
    
    elif type == 'json':
        data = readJson(path)
    return data
        
def getColumns(data):
    return data[0].keys()

def rename_columns(data, key_mapping):
    csv_transform = []

    for old_dict in data:
        dict_temp = {}
        for old_key, value in old_dict.items():
            dict_temp[key_mapping[old_key]] = value
        
        csv_transform.append(dict_temp)


    return csv_transform

def compHeader(file, header):
    print(f"{file} columns: [{header}]")
    print(f"Header length: {len(header)}")
    
def join(dataA, dataB):
    combined_list = []
    combined_list.extend(dataA)
    combined_list.extend(dataB)
    return combined_list

def transformData(mergedData, tableHeader):
    # Treatment for non existent columns in CSV
    table_combined_list = []
    for row in mergedData:
        line = []
        for column in tableHeader:
            line.append(row.get(column, 'Indisponivel'))
        table_combined_list.append(line)
    return table_combined_list


def saveData(processedData, processedFilePath):
    with open(processedFilePath, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows(processedData)
        
#----------------------------------------------------------------
#
#   - Step 1: Read file
#
#----------------------------------------------------------------

json_raw = leitura_dados(path_json, 'json')
csv_raw = leitura_dados(path_csv, 'csv')


#----------------------------------------------------------------
#
#   - Step 2: Columns compare
#
#----------------------------------------------------------------

column_json_raw = getColumns(json_raw)
column_csv_raw = getColumns(csv_raw)
print ('column_json_raw: ', column_json_raw)
print ('column_csv_raw: ', column_csv_raw)


#----------------------------------------------------------------
#
#   - Step 3: Data transform
#
#----------------------------------------------------------------
key_mapping = {
        'Nome do Item': 'Nome do Produto',
        'Classificacao do Produto' : 'Categoria do Produto',
        'Valor em Reais (R$)' : 'Preço do Produto (R$)',
        'Quantidade em Estoque' : 'Quantidade em Estoque',
        'Nome da Loja' : 'Filial',
        'Data da Venda' : 'Data da Venda'         
                }

prep_csv = rename_columns(csv_raw, key_mapping)

#----------------------------------------------------------------
#
#   - Step 4: Validate data
#
#----------------------------------------------------------------
print("******************************************************************")
compHeader('Arquivo JSON',column_json_raw)
compHeader('Arquivo CSV',column_csv_raw)

#----------------------------------------------------------------
#
#   - Step 5: Merge / Join data
#
#----------------------------------------------------------------
print("_________________________________________________________________")
mergeData = join(json_raw, prep_csv)

print('Length json data  : ', len(json_raw))
print('Length csv data   : ', len(csv_raw))
print('------------------------------------')
print('Length merged data: ', len(mergeData))
print("******************************************************************")

#----------------------------------------------------------------
#
#   - Step 6: Result File
#
#----------------------------------------------------------------

#headerFile = chaves_dict = ['Nome do Produto','Categoria do Produto','Preço do Produto (R$)','Quantidade em Estoque','Filial','Data da Venda']
headerFile = getColumns(prep_csv)
processed_data = transformData(mergeData, headerFile)
saveData(processed_data, path_data)



print('Program terminated with success!')
