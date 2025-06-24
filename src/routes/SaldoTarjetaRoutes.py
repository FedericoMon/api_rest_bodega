from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql
from datetime import datetime

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('tarjeta_blueprint', __name__)


@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_cuenta_tarjeta():
    try:
        conexion = get_connection()
        cursor = conexion.cursor(pymysql.cursors.DictCursor)

        # Parámetros de paginación
        start = int(request.args.get('start', 0))  # Índice inicial
        length = int(request.args.get('length', 10))  # Número de registros por página


        # Filtros personalizados
        fecha_inicio = request.args.get("fecha_inicio")
        fecha_fin = request.args.get("fecha_fin")

        # Construcción de la consulta base
        sql_base = """
            SELECT 
                c.id, 
                c.fecha, 
                DATE_FORMAT(c.fecha, '%%Y-%%m-%%d') AS FECHA_FORMATO, 
                c.abono, 
                c.retiro, 
                c.transferencias,
                c.disponible, 
                c.total

            FROM 
                tabla_tarjeta c 
            WHERE 1 = 1
        """

        # Agregar condiciones según los filtros
        params = []


        if fecha_inicio:
            sql_base += " AND c.fecha >= %s"
            params.append(fecha_inicio)

        if fecha_fin:
            sql_base += " AND c.fecha <= %s"
            params.append(fecha_fin)


        # Cláusula ORDER BY, LIMIT y OFFSET para paginación
        sql_paginated = sql_base + " ORDER BY c.fecha DESC LIMIT %s OFFSET %s"
        params.extend([length, start])

        # Ejecutar la consulta paginada
        cursor.execute(sql_paginated, params)
        datos_tarjeta = cursor.fetchall()

        # Contar el total de registros filtrados
        sql_count = "SELECT COUNT(*) AS total FROM (" + sql_base + ") AS filtered_records"
        cursor.execute(sql_count, params[:-2])  # Excluir LIMIT y OFFSET de los parámetros
        total_filtered = cursor.fetchone()["total"]

            # Contar el total de registros sin filtrar
        cursor.execute("SELECT COUNT(*) AS total FROM tabla_tarjeta")
        total_records = cursor.fetchone()["total"]

        # Procesar los resultados
        tarjeta = []
        for fila in datos_tarjeta:
            contabilida = {
                "id": fila["id"],
                "fecha": fila["fecha"],
                "abono": fila["abono"],
                "retiro": fila["retiro"],
                "transferencias": fila["transferencias"],
                "disponible": fila["disponible"],
                "total": fila["total"],
                "fecha_formateada": fila["FECHA_FORMATO"]
            }
            tarjeta.append(contabilida)


        cursor.close()
        conexion.close()
        # Responder en el formato esperado por DataTables
        return jsonify({
            "draw": int(request.args.get("draw", 0)),
            "recordsTotal": total_records,
            "recordsFiltered": total_filtered,
            "data": tarjeta
        })


    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})




@main.route("/<int:codigo>",methods=["DELETE"],strict_slashes=False)
@cross_origin()
def eliminar_tarjeta(codigo):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql = "DELETE FROM tabla_tarjeta WHERE id = %s"
        cursor.execute(sql, (codigo,))
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"registro eliminado."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})



@main.route('', methods=['POST'],strict_slashes=False)
@cross_origin()
def insertar_tarjeta():
        try:
                    # Extraer datos del JSON recibido
            fecha = request.json.get('fecha', None)

            disponible = request.json.get('disponible', '').strip()
            disponible = float(disponible) if disponible else 0.0


            if not fecha:
                return jsonify({"error": "La fecha es un parámetro obligatorio"}), 400
            conexion = get_connection()
            cursor = conexion.cursor(pymysql.cursors.DictCursor)

                    # Query SQL para insertar o actualizar la contabilidad del día
            sql = """
            INSERT INTO tabla_tarjeta (
                fecha, abono, retiro, transferencias,
		disponible, total)

            SELECT 
                %s AS fecha, 
                COALESCE((SELECT SUM(ABONO) FROM transaccion_clientes WHERE FECHA = %s AND tipo_pago_id = 3), 0.00) AS abono,
                COALESCE((SELECT SUM(retiro_tarjeta) FROM contabilidad_dia WHERE fecha = %s), 0.00) AS retiro,
		COALESCE((SELECT SUM(abono) FROM transaccion_provedores WHERE fecha= %s AND tipo_pago_id = 3), 0.00) AS transferencias,
                %s AS disponible,  
		(COALESCE((SELECT SUM(ABONO) FROM transaccion_clientes WHERE FECHA = %s AND tipo_pago_id = 3), 0.00) 
		- COALESCE((SELECT SUM(retiro_tarjeta) FROM contabilidad_dia WHERE fecha = %s), 0.00)
		- COALESCE((SELECT SUM(abono) FROM transaccion_provedores WHERE fecha= %s AND tipo_pago_id = 3), 0.00)
		+ %s) AS total
            FROM DUAL
            ON DUPLICATE KEY UPDATE 
                abono = VALUES(abono),
                retiro = VALUES(retiro),
                transferencias = VALUES(transferencias),
                disponible = VALUES(disponible),
                total = VALUES(total);
            """

            values = (
                fecha,
                fecha,
                fecha,
                fecha,
                disponible, 
                fecha,
                fecha,
                fecha,
                disponible


            )

                    # Ejecutar la consulta SQL

            cursor.execute(sql, values)
                    # Confirmar la acción de inserción
            conexion.commit()
            cursor.close()
            conexion.close()
            return jsonify({"mensaje": "Registro insertado correctamente"}), 200

        except Exception as ex:
            Logger.add_to_log("error", str(ex))
            Logger.add_to_log("error", traceback.format_exc())
            return jsonify({'message': ex, 'success': False})

