from db.mongo import db_01, db_02
import matplotlib.pyplot as plt
from datetime import datetime

collection_db_01 = db_01["detran_rn"]
collection_db_02 = db_02["detran_rn"]

batch_size = 1000
total_registros_01 = collection_db_01.count_documents({})
total_registros_02 = collection_db_02.count_documents({})

total_registros = total_registros_01+total_registros_02

registros_recuperados = 0

veiculos_por_marca = []

propietarios_por_documento = []

while registros_recuperados < total_registros:
    cursor = collection_db_02.find().skip(registros_recuperados).limit(batch_size)

    cursor_2 = collection_db_01.find().skip(registros_recuperados).limit(batch_size)

    for documento in cursor:
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


    for documento in cursor_2:
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


    registros_recuperados += batch_size

    print(f"TOTAL:{total_registros} | RODOU:{registros_recuperados}")

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

collection_db_02.insert_one({
    "veiculos_por_marca": veiculos_por_marca,
    "data": datetime.utcnow()
})
collection_db_02.insert_one({
    "propietarios_por_documento": propietarios_por_documento,
    "data": datetime.utcnow()
})

plt.pie(contagem_frequentes, labels=marcas_frequentes, autopct='%1.1f%%')
plt.title('Marcas de Veiculos Mais Frequentes no RN')
plt.show()
