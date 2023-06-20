import multiprocessing
from db.mongo import db_01, db_02
import matplotlib.pyplot as plt
from datetime import datetime
import json

veiculos_por_marca = []
propietarios_por_documento = []

def processar_lote(registros):

    for documento in registros:
        marca = documento['marca']

        registro_existente = next((registro for registro in veiculos_por_marca if registro['_id'] == marca), None)
        
        if registro_existente:
            registro_existente['count'] += 1
        else:
            veiculos_por_marca.append({'_id': marca, 'count': 1})


        propietario = documento['cpf_ou_cnpj_propietario']

        registro_existente = next((registro for registro in propietarios_por_documento if registro['_id'] == propietario), None)
        
        if registro_existente:
            registro_existente['count'] += 1
        else:
            propietarios_por_documento.append({'_id': propietario,'token': documento['pripietario_nome'], 'count': 1})

    return veiculos_por_marca, propietarios_por_documento

if __name__ == '__main__':
    collection_db_01 = db_01["detran_rn"]
    collection_db_02 = db_02["detran_rn"]
    collection_dados_obtidos = db_02["dados_obtidos"]

    batch_size = 1000
    total_registros_01 = collection_db_01.count_documents({})
    total_registros_02 = collection_db_02.count_documents({})

    total_registros = total_registros_01 + total_registros_02

    registros_recuperados = 0
    # num_processos = multiprocessing.cpu_count()  # Número de processos a serem utilizados
    num_processos = 4
    pool = multiprocessing.Pool(processes=num_processos)

    while registros_recuperados < total_registros:

        resultados_veiculos = []
        resultados_propietarios = []

        registros_01 = list(collection_db_01.find().skip(registros_recuperados).limit(batch_size))
        registros_02 = list(collection_db_02.find().skip(registros_recuperados).limit(batch_size))

        registros = registros_01 + registros_02

        # Dividir registros em lotes menores para cada processo
        registros_divididos = [registros[i:i+batch_size] for i in range(0, len(registros), batch_size)]

        # Processar lotes em paralelo
        resultados_lote = pool.map(processar_lote, registros_divididos)

        for veiculos_por_marca, propietarios_por_documento in resultados_lote:
            resultados_veiculos.extend(veiculos_por_marca)
            resultados_propietarios.extend(propietarios_por_documento)

        with open("json_doc_propietarios_por_documento.json", "r") as arquivo:
            dados_existentes_propietario = json.load(arquivo)

        with open("json_veiculos_por_marca.json", "r") as arquivo:
            dados_existentes_marca = json.load(arquivo)

        # Modifica a estrutura de dados existente (adiciona um novo item)
        
        dados_existentes_propietario["propietario"].append(resultados_propietarios)

        dados_existentes_marca["marcas"].append(resultados_veiculos)

        json_resultados_veiculos = json.dumps(dados_existentes_marca, indent=4)
        json_resultados_propietarios = json.dumps(dados_existentes_propietario, indent=4)

        with open('json_doc_propietarios_por_documento.json', 'w') as arquivo:
            arquivo.write(json_resultados_propietarios)

        with open('json_veiculos_por_marca.json', 'w') as arquivo:
            arquivo.write(json_resultados_veiculos)

        # collection_dados_obtidos.insert_one({
        #     "resultados_veiculos": resultados_veiculos,
        #     "registros_num": registros_recuperados
        # })

        # collection_dados_obtidos.insert_one({
        #     "resultados_propietarios": resultados_propietarios,
        #     "registros_num": registros_recuperados
        # })

        registros_recuperados += len(registros)

        print(f"TOTAL:{total_registros} | RODOU:{registros_recuperados}")

    pool.close()
    pool.join()

    # Restante do código para processar os resultados coletados

    marcas = [registro['_id'] for registro in veiculos_por_marca]
    contagem_veiculos = [registro['count'] for registro in veiculos_por_marca]

    veiculos_por_marca.sort(key=lambda x: x['count'], reverse=True)

    propietarios_por_documento.sort(key=lambda x: x['count'], reverse=True)

    marcas_frequentes = [registro['_id'] for registro in veiculos_por_marca[:5]]
    contagem_frequentes = [registro['count'] for registro in veiculos_por_marca[:5]]

    propietarios_frequentes = [registro['token'] for registro in propietarios_por_documento[:5]]
    contagem_propietarios_frequentes = [registro['count'] for registro in propietarios_por_documento[:5]]

    # marcas = [documento['_id'] for documento in veiculos_por_marca]
    # quantidades = [documento['count'] for documento in veiculos_por_marca]

    # collection_db_02.insert_one({
    #     "veiculos_por_marca": veiculos_por_marca,
    #     "data": datetime.utcnow()
    # })
    # collection_db_02.insert_one({
    #     "propietarios_por_documento": propietarios_por_documento,
    #     "data": datetime.utcnow()
    # })

    doc_veiculos_por_marca = {
        "veiculos_por_marca": veiculos_por_marca
    }

    doc_propietarios_por_documento = {
        "propietarios_por_documento": propietarios_por_documento
    }

    json_doc_propietarios_por_documento = json.dumps(doc_propietarios_por_documento, indent=4)
    json_veiculos_por_marca = json.dumps(doc_veiculos_por_marca, indent=4)

    with open('json_doc_propietarios_por_documento.json', 'w') as arquivo:
        arquivo.write(json_doc_propietarios_por_documento)

    with open('json_veiculos_por_marca.json', 'w') as arquivo:
        arquivo.write(json_veiculos_por_marca)

    plt.pie(contagem_frequentes, labels=marcas_frequentes, autopct='%1.1f%%')
    plt.title('Marcas de Veiculos Mais Frequentes no RN')
    plt.show()
