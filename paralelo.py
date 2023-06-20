import multiprocessing
from db.mongo import db_01, db_02
import matplotlib.pyplot as plt
from datetime import datetime
import json

resultados_veiculos = []
resultados_propietarios = []

def processar_lote(registros):

    for documento in registros:
        marca = documento['marca']

        registro_existente = next((registro for registro in resultados_veiculos if registro['_id'] == marca), None)
        
        if registro_existente:
            registro_existente['count'] += 1
        else:
            resultados_veiculos.append({'_id': marca, 'count': 1})


        propietario = documento['cpf_ou_cnpj_propietario']

        registro_existente = next((registro for registro in resultados_propietarios if registro['_id'] == propietario), None)
        
        if registro_existente:
            registro_existente['count'] += 1
        else:
            resultados_propietarios.append({'_id': propietario,'token': documento['pripietario_nome'], 'count': 1})

    return resultados_veiculos, resultados_propietarios

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

    resultados_veiculos = []
    resultados_propietarios = []

    while registros_recuperados < total_registros:
        try:
            registros_01 = list(collection_db_01.find().skip(registros_recuperados).limit(batch_size))
            registros_02 = list(collection_db_02.find().skip(registros_recuperados).limit(batch_size))

            registros = registros_01 + registros_02

            # Dividir registros em lotes menores para cada processo
            registros_divididos = [registros[i:i+batch_size] for i in range(0, len(registros), batch_size)]

            # Processar lotes em paralelo
            resultados_lote = pool.map(processar_lote, registros_divididos)

            for resultados_veiculos, resultados_propietarios in resultados_lote:
                resultados_veiculos.extend(resultados_veiculos)
                resultados_propietarios.extend(resultados_propietarios)

            registros_recuperados += len(registros)


            print(f"TOTAL:{total_registros} | RODOU:{registros_recuperados}")
        except:
            break

    pool.close()
    pool.join()

    marcas = [registro['_id'] for registro in resultados_veiculos]
    contagem_veiculos = [registro['count'] for registro in resultados_veiculos]

    resultados_veiculos.sort(key=lambda x: x['count'], reverse=True)

    resultados_propietarios.sort(key=lambda x: x['count'], reverse=True)

    marcas_frequentes = [registro['_id'] for registro in resultados_veiculos[:5]]
    contagem_frequentes = [registro['count'] for registro in resultados_veiculos[:5]]

    propietarios_frequentes = [registro['token'] for registro in resultados_propietarios[:5]]
    contagem_propietarios_frequentes = [registro['count'] for registro in resultados_propietarios[:5]]

    doc_resultados_veiculos = {
        "resultados_veiculos": resultados_veiculos
    }

    doc_resultados_propietarios = {
        "resultados_propietarios": resultados_propietarios
    }

    json_doc_resultados_propietarios = json.dumps(doc_resultados_propietarios, indent=4)
    json_resultados_veiculos = json.dumps(doc_resultados_veiculos, indent=4)

    with open('json_doc_resultados_propietarios.json', 'w') as arquivo:
        arquivo.write(json_doc_resultados_propietarios)

    with open('json_resultados_veiculos.json', 'w') as arquivo:
        arquivo.write(json_resultados_veiculos)

    plt.pie(contagem_frequentes, labels=marcas_frequentes, autopct='%1.1f%%')
    plt.title('Marcas de Veiculos Mais Frequentes no RN')
    plt.show()
