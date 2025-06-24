from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback

import pymysql
# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('providers_transaccion_blueprint', __name__)



@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_Transaccion_provedores():
    try:
        conexion = get_connection()
        cursor = conexion.cursor(pymysql.cursors.DictCursor)
                # Parámetros de paginación
        start = int(request.args.get('start', 0))  # Índice inicial
        length = int(request.args.get('length', 10))  # Número de registros por página


        # Filtros personalizados
        fecha_inicio_prov = request.args.get("fecha_inicio_prov")
        fecha_fin_prov = request.args.get("fecha_fin_prov")
        provedor_id  = request.args.get("provedor_id")
        provedor_id = int(provedor_id) if provedor_id else ""

        tipo_transaccion_prov_id = request.args.get("tipo_transaccion_prov_id")
        tipo_transaccion_prov_id = int(tipo_transaccion_prov_id) if tipo_transaccion_prov_id else ""




        sql_base = """
            SELECT 
                c.id, 
                c.fecha, 
                DATE_FORMAT(c.fecha, '%%Y-%%m-%%d') AS FECHA_FORMATO, 
                c.provedor_id, 
                c.tipo_transaccion_prov_id, 
                c.descripcion,
                c.num_canales, 
                c.peso, 
                c.precio, 
                c.importe, 
                c.abono,
                c.deuda_anterior, 
                c.tipo_canal_id, 
                c.tipo_pago_id,
                c.status_cuenta_id, 
                prov.nombres AS `PROVEDORES`, 
                tip_trans_prov.nombre AS `TIPO_TRANSACCION_PROV`, 
                tipcan.nombre AS `TIPO_CANAL`, 
                stat.nombre AS `STATUS_CUENTA`, 
                tippag.nombre AS `TIPO_PAGO`
            FROM 
                transaccion_provedores c 
            JOIN 
                provedores prov ON c.provedor_id = prov.id
            JOIN 
                tipo_transaccion_prov tip_trans_prov ON c.tipo_transaccion_prov_id = tip_trans_prov.id
            JOIN 
                tipo_canal tipcan ON c.tipo_canal_id = tipcan.id
            JOIN 
                status_cuenta stat ON c.status_cuenta_id = stat.id
            JOIN 
                tipo_pago tippag ON c.tipo_pago_id = tippag.id
                            WHERE 1 = 1
        """

        # Agregar condiciones según los filtros
        params = []


        if fecha_inicio_prov:
            sql_base += " AND c.fecha >= %s"
            params.append(fecha_inicio_prov)

        if fecha_fin_prov:
            sql_base += " AND c.fecha <= %s"
            params.append(fecha_fin_prov)


        if provedor_id:
            sql_base += " AND c.provedor_id= %s"
            params.append(provedor_id)

        if tipo_transaccion_prov_id:
            sql_base += " AND c.tipo_transaccion_prov_id = %s"
            params.append(tipo_transaccion_prov_id)


                # Cláusula ORDER BY, LIMIT y OFFSET para paginación
        sql_paginated = sql_base + " ORDER BY c.fecha DESC LIMIT %s OFFSET %s"
        params.extend([length, start])

            # Ejecutar la consulta paginada
        cursor.execute(sql_paginated, params)
        datos_transaccion_providers = cursor.fetchall()

                            # Contar el total de registros filtrados
        sql_count = "SELECT COUNT(*) AS total FROM (" + sql_base + ") AS filtered_records"
        cursor.execute(sql_count, params[:-2])  # Excluir LIMIT y OFFSET de los parámetros
        total_filtered = cursor.fetchone()["total"]

            # Contar el total de registros sin filtrar
        cursor.execute("SELECT COUNT(*) AS total FROM transaccion_provedores")
        total_records = cursor.fetchone()["total"]

                                # **Calcular el total_importe solo con los clientes mostrados en la página**
        total_importe_query = f"SELECT SUM(importe) AS total_importe FROM ({sql_paginated}) AS subconsulta"
        cursor.execute(total_importe_query, params)
        resultado_importe = cursor.fetchone()
        total_importe = resultado_importe["total_importe"] if resultado_importe and resultado_importe["total_importe"] else 0.00


                                # **Calcular el total_abonos solo con los clientes mostrados en la página**
        total_abonos_query = f"SELECT SUM(abono) AS total_abonos FROM ({sql_paginated}) AS subconsulta"
        cursor.execute(total_abonos_query, params)
        resultado_abonos = cursor.fetchone()
        total_abonos = resultado_abonos["total_abonos"] if resultado_abonos and resultado_abonos["total_abonos"] else 0.00


        transacciones_providers = []
        for fila in datos_transaccion_providers:
            transacciones_provider = {
                "id": fila["id"],
                "fecha": fila["fecha"],
                "providers_id": fila["provedor_id"],
                "tipo_transaccion_prov_id": fila["tipo_transaccion_prov_id"],
                "Descripcion": fila["descripcion"],
                "num_canales": fila["num_canales"],
                "peso": fila["peso"],
                "precio": fila["precio"],
                "importe": fila["importe"],
                "abono": fila["abono"],
                "deuda_anterior": fila["deuda_anterior"],
                "tipo_canal_id": fila["tipo_canal_id"],
                "status_cuenta_id": fila["status_cuenta_id"],
                "nombres_providers": fila["PROVEDORES"],
                "tipo_transaccion_prov": fila["TIPO_TRANSACCION_PROV"],
                "tipo_canal": fila["TIPO_CANAL"],
                "status_cuenta": fila["STATUS_CUENTA"],
                "tipo_pago_id": fila["tipo_pago_id"],
                "tipo_pago": fila["TIPO_PAGO"],
                "fecha_formateada": fila["FECHA_FORMATO"]
            }
            transacciones_providers.append(transacciones_provider)


        cursor.close()
        conexion.close()
        # Responder en el formato esperado por DataTables
        return jsonify({
            "draw": int(request.args.get("draw", 0)),
            "recordsTotal": total_records,
            "recordsFiltered": total_filtered,
            "data": transacciones_providers,
            "totalImporte": total_importe,
            "totalAbonos": total_abonos
        })


    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})



@main.route("/<int:codigo>",methods=["DELETE"],strict_slashes=False)
@cross_origin()
def eliminar_transacc_providers(codigo):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql = "DELETE FROM transaccion_provedores WHERE id = %s"
        cursor.execute(sql, (codigo,))
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Transaccion Provedor Eliminada."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})




@main.route('', methods=['POST'],strict_slashes=False)
@cross_origin()
def createTransProviders():
    if 'id' in request.json:
        updateTransProvider()
    else:
        createTransProvider()
    return "ok"

def createTransProvider():
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="INSERT INTO `transaccion_provedores` (`id`, `fecha`, `provedor_id`, `tipo_transaccion_prov_id`, `descripcion`,`num_canales`, `peso`, `precio`, `Importe`, `abono`, `tipo_canal_id`, `tipo_pago_id`, `status_cuenta_id`, `deuda_anterior`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
                # Obtener valores del JSON
        # Obtener el valor de la fecha desde el JSON
        fecha = request.json.get('fecha', None)
        # Si la fecha está vacía o no existe, usar la fecha actual
        if not fecha or fecha.strip() == "":
            fecha = datetime.now().strftime("%Y-%m-%d")
        peso = request.json.get('peso', '').strip()
        abono = request.json.get('abono', '').strip()
        price = request.json.get('price', '').strip()
        deuda_ant = request.json.get('deuda_ant', '').strip()
        importe = request.json.get('Importe', None)
        # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(importe, str):
            importe = importe.strip()
        # Convertir peso y abono, asignando 0 si están vacíos
        num_canales = request.json.get('num_canales', None)
        # Verificar si 'num_canales' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(num_canales, str):
            num_canales = num_canales.strip()
        peso = float(peso) if peso else 0.0
        abono = float(abono) if abono else 0.0
        price = float(price) if price else 0.0
        importe = float(importe) if importe else 0.0
        deuda_ant = float(deuda_ant) if deuda_ant else 0.0
        num_canales = int(num_canales) if num_canales else 0
        values = (
            fecha, 
            int(request.json['provedor_id']), 
            int(request.json['tipo_transaccion_id']), 
            request.json['Descripcion'], 
            num_canales,
            peso, 
            price, 
            importe, 
            abono,
            int(request.json['tipo_canal_id']),
            int(request.json['tipo_pago_id']),            
            int(request.json['status_cuenta_id']),
            deuda_ant
        )
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Transaccion Provedor Guardada."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})

def updateTransProvider():

    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="UPDATE `transaccion_provedores` SET  `fecha`=%s, `provedor_id`=%s, `tipo_transaccion_prov_id`=%s, `descripcion`=%s,`num_canales`=%s, `peso`=%s, `precio`=%s, `Importe`=%s, `abono`=%s, `tipo_canal_id`=%s, `tipo_pago_id`=%s, `status_cuenta_id`=%s, `deuda_anterior`=%s   WHERE `transaccion_provedores`.`id` = %s;"
                # Obtener valores del JSON
        # Obtener el valor de la fecha desde el JSON
        fecha = request.json.get('fecha', None)
        # Si la fecha está vacía o no existe, usar la fecha actual
        if not fecha or fecha.strip() == "":
            fecha = datetime.now().strftime("%Y-%m-%d")
        peso = request.json.get('peso', '').strip()
        abono = request.json.get('abono', '').strip()
        price = request.json.get('price', '').strip()
        deuda_ant = request.json.get('deuda_ant', '').strip()
        importe = request.json.get('Importe', None)
        # Verificar si 'Importe' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(importe, str):
            importe = importe.strip()
        # Convertir peso y abono, asignando 0 si están vacíos
        num_canales = request.json.get('num_canales', None)
        # Verificar si 'num_canales' es una cadena y aplicar strip, o usar directamente su valor
        if isinstance(num_canales, str):
            num_canales = num_canales.strip()
        peso = float(peso) if peso else 0.0
        abono = float(abono) if abono else 0.0
        price = float(price) if price else 0.0
        importe = float(importe) if importe else 0.0
        deuda_ant = float(deuda_ant) if deuda_ant else 0.0
        num_canales = int(num_canales) if num_canales else 0 
        values = (
            fecha, 
            int(request.json['provedor_id']), 
            int(request.json['tipo_transaccion_id']), 
            request.json['Descripcion'], 
            num_canales,
            peso, 
            price, 
            importe, 
            abono,
            int(request.json['tipo_canal_id']),
            int(request.json['tipo_pago_id']),            
            int(request.json['status_cuenta_id']),
            deuda_ant,
            int(request.json['id'])

        )                   
                    
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Transaccion Provedor Actualizada."})

    except Exception as ex:
        # Imprime el error en la consola para depuración
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})