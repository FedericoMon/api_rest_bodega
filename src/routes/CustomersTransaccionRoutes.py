from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('customers_transaccion_blueprint', __name__)



@main.route("", methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_Transaccion_clients():
    try:
        conexion = get_connection()
        cursor = conexion.cursor(pymysql.cursors.DictCursor)

        # Parámetros de paginación
        start = int(request.args.get('start', 0))  # Índice inicial
        length = int(request.args.get('length', 10))  # Número de registros por página


        # Filtros personalizados
        fecha_inicio = request.args.get("fecha_inicio")
        fecha_fin = request.args.get("fecha_fin")
        cliente_id  = request.args.get("cliente_id")
        cliente_id = int(cliente_id) if cliente_id else ""

        tipo_transaccion_id = request.args.get("tipo_transaccion_id")
        tipo_transaccion_id = int(tipo_transaccion_id) if tipo_transaccion_id else ""


        # Construcción de la consulta base
        sql_base = """
            SELECT 
                c.id, 
                c.FECHA, 
                DATE_FORMAT(c.FECHA, '%%Y-%%m-%%d') AS FECHA_FORMATO, 
                c.clientes_id, 
                c.tipo_transaccion_id, 
                c.Descripcion, 
                c.peso, 
                c.PU, 
                c.IMPORTE, 
                c.ABONO, 
                c.PRECIO_PROVEDOR, 
                c.Ganancia, 
                c.NUM_CUARTOS, 
                c.TIPO_CANAL_ID, 
                c.STATUS_CUENTA_ID, 
                cli.nombres AS `CLIENTES`, 
                tip_trans.nombre AS `TIPO_TRANSACCION`, 
                tipcan.nombre AS `TIPO_CANAL`, 
                stat.nombre AS `STATUS_CUENTA`, 
                c.tipo_pago_id, 
                tippag.nombre AS `TIPO_PAGO`
            FROM 
                transaccion_clientes c 
            JOIN 
                clientes cli ON c.clientes_id = cli.id
            JOIN 
                tipo_transaccion tip_trans ON c.tipo_transaccion_id = tip_trans.id
            JOIN 
                tipo_canal tipcan ON c.TIPO_CANAL_ID = tipcan.id
            JOIN 
                status_cuenta stat ON c.STATUS_CUENTA_ID = stat.id
            JOIN 
                tipo_pago tippag ON c.tipo_pago_id = tippag.id
            WHERE 1 = 1
        """

        # Agregar condiciones según los filtros
        params = []


        if fecha_inicio:
            sql_base += " AND c.FECHA >= %s"
            params.append(fecha_inicio)

        if fecha_fin:
            sql_base += " AND c.FECHA <= %s"
            params.append(fecha_fin)


        if cliente_id:
            sql_base += " AND c.clientes_id= %s"
            params.append(cliente_id)

        if tipo_transaccion_id:
            sql_base += " AND c.tipo_transaccion_id = %s"
            params.append(tipo_transaccion_id)

        # Cláusula ORDER BY, LIMIT y OFFSET para paginación
        sql_paginated = sql_base + " ORDER BY c.FECHA DESC LIMIT %s OFFSET %s"
        params.extend([length, start])

            # Ejecutar la consulta paginada
        cursor.execute(sql_paginated, params)
        datos_trans_clientes = cursor.fetchall()

                    # Contar el total de registros filtrados
        sql_count = "SELECT COUNT(*) AS total FROM (" + sql_base + ") AS filtered_records"
        cursor.execute(sql_count, params[:-2])  # Excluir LIMIT y OFFSET de los parámetros
        total_filtered = cursor.fetchone()["total"]

            # Contar el total de registros sin filtrar
        cursor.execute("SELECT COUNT(*) AS total FROM transaccion_clientes")
        total_records = cursor.fetchone()["total"]


                        # **Calcular el total_importe solo con los clientes mostrados en la página**
        total_importe_query = f"SELECT SUM(IMPORTE) AS total_importe FROM ({sql_paginated}) AS subconsulta"
        cursor.execute(total_importe_query, params)
        resultado_importe = cursor.fetchone()
        total_importe = resultado_importe["total_importe"] if resultado_importe and resultado_importe["total_importe"] else 0.00


                                # **Calcular el total_abonos solo con los clientes mostrados en la página**
        total_abonos_query = f"SELECT SUM(ABONO) AS total_abonos FROM ({sql_paginated}) AS subconsulta"
        cursor.execute(total_abonos_query, params)
        resultado_abonos = cursor.fetchone()
        total_abonos = resultado_abonos["total_abonos"] if resultado_abonos and resultado_abonos["total_abonos"] else 0.00


        # Procesar los resultados
        transacciones_clients = []
        for fila in datos_trans_clientes:
            transaccione_clients = {
                "id": fila["id"],
                "fecha": fila["FECHA"],
                "clientes_id": fila["clientes_id"],
                "tipo_transaccion_id": fila["tipo_transaccion_id"],
                "Descripcion": fila["Descripcion"],
                "peso": fila["peso"],
                "pu": fila["PU"],
                "importe": fila["IMPORTE"],
                "abono": fila["ABONO"],
                "precio_provedor": fila["PRECIO_PROVEDOR"],
                "Ganancia": fila["Ganancia"],
                "Num_cuartos": fila["NUM_CUARTOS"],
                "Tipo_canal_id": fila["TIPO_CANAL_ID"],
                "Status_cuenta_id": fila["STATUS_CUENTA_ID"],
                "nombres_clientes": fila["CLIENTES"],
                "tipo_transaccion": fila["TIPO_TRANSACCION"],
                "tipo_canal": fila["TIPO_CANAL"],
                "Status_cuenta": fila["STATUS_CUENTA"],
                "tipo_pago_id": fila["tipo_pago_id"],
                "tipo_pago": fila["TIPO_PAGO"],
                "fecha_formateada": fila["FECHA_FORMATO"]
            }
            transacciones_clients.append(transaccione_clients)


        cursor.close()
        conexion.close()
        # Responder en el formato esperado por DataTables
        return jsonify({
            "draw": int(request.args.get("draw", 0)),
            "recordsTotal": total_records,
            "recordsFiltered": total_filtered,
            "data": transacciones_clients,
            "totalImporte": total_importe,
            "totalAbonos": total_abonos

        })

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})



@main.route("/<int:codigo>",methods=["DELETE"],strict_slashes=False)
@cross_origin()
def eliminar_transacc_clients(codigo):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql = "DELETE FROM transaccion_clientes WHERE id = %s"
        cursor.execute(sql, (codigo,))
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Transaccion Cliente Eliminada."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})


# @main.route('', methods=['POST'],strict_slashes=False)
# @cross_origin()
# def createTransCustomer():
#     try:
#         # 2. Intentar obtener el JSON del cuerpo de la solicitud
#         data = request.get_json(force=True)
#         print("Datos recibidos:", data)

#         # 3. Lógica condicional para determinar si es creación o actualización
#         if 'id' in data:
#             return updateTransCustomer(data)
#         else:
#             return createTransCustomer(data)
#     except Exception as ex:
#         Logger.add_to_log("error", str(ex))
#         Logger.add_to_log("error", traceback.format_exc())
#         return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})






# def createTransCustomer(data):
#     try:
#         conexion = get_connection()
#         cursor = conexion.cursor()
#         sql="INSERT INTO `transaccion_clientes` (`id`, `FECHA`, `clientes_id`, `tipo_transaccion_id`, `Descripcion`, `peso`, `PU`, `IMPORTE`, `ABONO`, `PRECIO_PROVEDOR`, `Ganancia`, `NUM_CUARTOS`, `TIPO_CANAL_ID`, `STATUS_CUENTA_ID`, `tipo_pago_id`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
#                 # Obtener valores del JSON
#         # Obtener el valor de la fecha desde el JSON
#         fecha = data('fecha', None)
#         # Si la fecha está vacía o no existe, usar la fecha actual
#         if not fecha or fecha.strip() == "":
#             fecha = datetime.now().strftime("%Y-%m-%d")
#         peso = data('peso', '').strip()
#         abono = data('abono', '').strip()
#         price = data('price', '').strip()
#         importe = data('Importe', None)
#         # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
#         if isinstance(importe, str):
#             importe = importe.strip()
#         precio_prov = data('precio_prov', '').strip()
#         Ganancia = data('Ganancia', None)
#                 # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
#         if isinstance(Ganancia, str):
#             Ganancia = Ganancia.strip()
#         # Convertir peso y abono, asignando 0 si están vacíos
#         num_cuartos = data('num_cuartos', None)
#         # Verificar si 'num_cuartos' es una cadena y aplicar strip, o usar directamente su valor
#         if isinstance(num_cuartos, str):
#             num_cuartos = num_cuartos.strip()
#         peso = float(peso) if peso else 0.0
#         abono = float(abono) if abono else 0.0
#         price = float(price) if price else 0.0
#         importe = float(importe) if importe else 0.0
#         precio_prov = float(precio_prov) if precio_prov else 0.0
#         Ganancia = float(Ganancia) if Ganancia else 0.0
#         num_cuartos = int(num_cuartos) if num_cuartos else 0
#         values = (
#             fecha, 
#             int(data['cliente_id']), 
#             int(data['tipo_transaccion_id']), 
#             data['Descripcion'], 
#             peso, 
#             price, 
#             importe, 
#             abono,
#             precio_prov,
#             Ganancia,
#             num_cuartos,
#             int(data['tipo_canal_id']),
#             int(data['status_cuenta_id']),
#             int(data['tipo_pago_id'])            


#         )
#         cursor.execute(sql, values)
#         # Confirmar la acción de inserción
#         conexion.commit()
#         cursor.close()
#         conexion.close()
#         return jsonify({ "mensaje":"Transaccion Cliente Guardada."})

#     except Exception as ex:
#         Logger.add_to_log("error", str(ex))
#         Logger.add_to_log("error", traceback.format_exc())
#         return jsonify({'message': ex, 'success': False})


# def updateTransCustomer(data):

#     try:
#         conexion = get_connection()
#         cursor = conexion.cursor()
#         sql="UPDATE `transaccion_clientes` SET  `FECHA`=%s, `clientes_id`=%s, `tipo_transaccion_id`=%s, `Descripcion`=%s, `peso`=%s, `PU`=%s, `IMPORTE`=%s, `ABONO`=%s, `PRECIO_PROVEDOR`=%s, `Ganancia`=%s, `NUM_CUARTOS`=%s, `TIPO_CANAL_ID`=%s, `STATUS_CUENTA_ID`=%s, `tipo_pago_id`=%s  WHERE `transaccion_clientes`.`id` = %s;"
#                     # Obtener valores del JSON
#         # Obtener el valor de la fecha desde el JSON
#         fecha = data('fecha', None)
#         # Si la fecha está vacía o no existe, usar la fecha actual
#         if not fecha or fecha.strip() == "":
#             fecha = datetime.now().strftime("%Y-%m-%d")
#         peso = data('peso', '').strip()
#         abono = data('abono', '').strip()
#         price = data('price', '').strip()
#         importe = data('Importe', None)
#         # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
#         if isinstance(importe, str):
#             importe = importe.strip()
#         precio_prov = data('precio_prov', '').strip()
#         Ganancia = data('Ganancia', None)
#                 # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
#         if isinstance(Ganancia, str):
#             Ganancia = Ganancia.strip()
#         # Convertir peso y abono, asignando 0 si están vacíos
#         num_cuartos = data('num_cuartos', None)
#         # Verificar si 'num_cuartos' es una cadena y aplicar strip, o usar directamente su valor
#         if isinstance(num_cuartos, str):
#             num_cuartos = num_cuartos.strip()
#         peso = float(peso) if peso else 0.0
#         abono = float(abono) if abono else 0.0
#         price = float(price) if price else 0.0
#         importe = float(importe) if importe else 0.0
#         precio_prov = float(precio_prov) if precio_prov else 0.0
#         Ganancia = float(Ganancia) if Ganancia else 0.0
#         num_cuartos = int(num_cuartos) if num_cuartos else 0

#         values = (
#             fecha, 
#             int(data['cliente_id']), 
#             int(data['tipo_transaccion_id']), 
#             data['Descripcion'], 
#             peso, 
#             price, 
#             importe, 
#             abono,
#             precio_prov,
#             Ganancia,
#             num_cuartos,
#             int(data['tipo_canal_id']),
#             int(data['status_cuenta_id']),
#             int(data['tipo_pago_id']),
#             int(data['id'])
#         )
#         cursor.execute(sql, values)
#         # Confirmar la acción de inserción
#         conexion.commit()
#         cursor.close()
#         conexion.close()
#         return jsonify({ "mensaje":"Cliente Actualizado."})

#     except Exception as ex:
#         Logger.add_to_log("error", str(ex))
#         Logger.add_to_log("error", traceback.format_exc())
#         return jsonify({'message': ex, 'success': False})



@main.route('', methods=['POST'],strict_slashes=False)
@cross_origin()
def createTransCustomer():
    if 'id' in request.json:
        updateTransCustomer()
    else:
        createTransCustomer()
    return "ok"

def createTransCustomer():
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="INSERT INTO `transaccion_clientes` (`id`, `FECHA`, `clientes_id`, `tipo_transaccion_id`, `Descripcion`, `peso`, `PU`, `IMPORTE`, `ABONO`, `PRECIO_PROVEDOR`, `Ganancia`, `NUM_CUARTOS`, `TIPO_CANAL_ID`, `STATUS_CUENTA_ID`, `tipo_pago_id`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                # Obtener valores del JSON
        # Obtener el valor de la fecha desde el JSON
        fecha = request.json.get('fecha', None)
        # Si la fecha está vacía o no existe, usar la fecha actual
        if not fecha or fecha.strip() == "":
            fecha = datetime.now().strftime("%Y-%m-%d")
        peso = request.json.get('peso', '').strip()
        abono = request.json.get('abono', '').strip()
        price = request.json.get('price', '').strip()
        importe = request.json.get('Importe', None)
        # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(importe, str):
            importe = importe.strip()
        precio_prov = request.json.get('precio_prov', '').strip()
        Ganancia = request.json.get('Ganancia', None)
                # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(Ganancia, str):
            Ganancia = Ganancia.strip()
        # Convertir peso y abono, asignando 0 si están vacíos
        num_cuartos = request.json.get('num_cuartos', None)
        # Verificar si 'num_cuartos' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(num_cuartos, str):
            num_cuartos = num_cuartos.strip()
        peso = float(peso) if peso else 0.0
        abono = float(abono) if abono else 0.0
        price = float(price) if price else 0.0
        importe = float(importe) if importe else 0.0
        precio_prov = float(precio_prov) if precio_prov else 0.0
        Ganancia = float(Ganancia) if Ganancia else 0.0
        num_cuartos = int(num_cuartos) if num_cuartos else 0
        values = (
            fecha, 
            int(request.json['cliente_id']), 
            int(request.json['tipo_transaccion_id']), 
            request.json['Descripcion'], 
            peso, 
            price, 
            importe, 
            abono,
            precio_prov,
            Ganancia,
            num_cuartos,
            int(request.json['tipo_canal_id']),
            int(request.json['status_cuenta_id']),
            int(request.json['tipo_pago_id'])            


        )
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Transaccion Cliente Guardada."})

    except Exception as ex:
        # Imprime el error en la consola para depuración
        print("Error:", ex)
        return jsonify({"mensaje":ex})


def updateTransCustomer():

    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="UPDATE `transaccion_clientes` SET  `FECHA`=%s, `clientes_id`=%s, `tipo_transaccion_id`=%s, `Descripcion`=%s, `peso`=%s, `PU`=%s, `IMPORTE`=%s, `ABONO`=%s, `PRECIO_PROVEDOR`=%s, `Ganancia`=%s, `NUM_CUARTOS`=%s, `TIPO_CANAL_ID`=%s, `STATUS_CUENTA_ID`=%s, `tipo_pago_id`=%s  WHERE `transaccion_clientes`.`id` = %s;"
                    # Obtener valores del JSON
        # Obtener el valor de la fecha desde el JSON
        fecha = request.json.get('fecha', None)
        # Si la fecha está vacía o no existe, usar la fecha actual
        if not fecha or fecha.strip() == "":
            fecha = datetime.now().strftime("%Y-%m-%d")
        peso = request.json.get('peso', '').strip()
        abono = request.json.get('abono', '').strip()
        price = request.json.get('price', '').strip()
        importe = request.json.get('Importe', None)
        # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(importe, str):
            importe = importe.strip()
        precio_prov = request.json.get('precio_prov', '').strip()
        Ganancia = request.json.get('Ganancia', None)
                # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(Ganancia, str):
            Ganancia = Ganancia.strip()
        # Convertir peso y abono, asignando 0 si están vacíos
        num_cuartos = request.json.get('num_cuartos', None)
        # Verificar si 'num_cuartos' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(num_cuartos, str):
            num_cuartos = num_cuartos.strip()
        peso = float(peso) if peso else 0.0
        abono = float(abono) if abono else 0.0
        price = float(price) if price else 0.0
        importe = float(importe) if importe else 0.0
        precio_prov = float(precio_prov) if precio_prov else 0.0
        Ganancia = float(Ganancia) if Ganancia else 0.0
        num_cuartos = int(num_cuartos) if num_cuartos else 0

        values = (
            fecha, 
            int(request.json['cliente_id']), 
            int(request.json['tipo_transaccion_id']), 
            request.json['Descripcion'], 
            peso, 
            price, 
            importe, 
            abono,
            precio_prov,
            Ganancia,
            num_cuartos,
            int(request.json['tipo_canal_id']),
            int(request.json['status_cuenta_id']),
            int(request.json['tipo_pago_id']),
            int(request.json['id'])
        )
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Cliente Actualizado."})

    except Exception as ex:
        # Imprime el error en la consola para depuración
        print("Error:", ex)
        return jsonify({"mensaje":ex})



