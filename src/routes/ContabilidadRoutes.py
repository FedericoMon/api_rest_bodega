from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql
from datetime import datetime

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('contabilidad_blueprint', __name__)



@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_contabilidad():
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
                c.abono_clientes, 
                c.retiro_tarjeta, 
                c.abono_provedores, 
                c.disponible, 
                c.gastos, 
                c.frio, 
                c.ganancias_ventas, 
                c.ganancias_pesadas, 
                c.ganancias_totales, 
                c.sobrante_dia,
                c.acumulado_dia,
                c.abono_extra,
                c.ventas

            FROM 
                contabilidad_dia c 
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
        datos_contabilidad = cursor.fetchall()

        # Contar el total de registros filtrados
        sql_count = "SELECT COUNT(*) AS total FROM (" + sql_base + ") AS filtered_records"
        cursor.execute(sql_count, params[:-2])  # Excluir LIMIT y OFFSET de los parámetros
        total_filtered = cursor.fetchone()["total"]

            # Contar el total de registros sin filtrar
        cursor.execute("SELECT COUNT(*) AS total FROM contabilidad_dia")
        total_records = cursor.fetchone()["total"]

        # Procesar los resultados
        contabilidad = []
        for fila in datos_contabilidad:
            contabilida = {
                "id": fila["id"],
                "fecha": fila["fecha"],
                "abono_clientes": fila["abono_clientes"],
                "retiro_tarjeta": fila["retiro_tarjeta"],
                "abono_provedores": fila["abono_provedores"],
                "disponible": fila["disponible"],
                "gastos": fila["gastos"],
                "frio": fila["frio"],
                "ganancias_ventas": fila["ganancias_ventas"],
                "ganancias_pesadas": fila["ganancias_pesadas"],
                "ganancias_totales": fila["ganancias_totales"],
                "sobrante_dia": fila["sobrante_dia"],
                "acumulado_dia": fila["acumulado_dia"],
                "abono_extra": fila["abono_extra"],
                "fecha_formateada": fila["FECHA_FORMATO"],
                "ventas": fila["ventas"]
            }
            contabilidad.append(contabilida)


        cursor.close()
        conexion.close()
        # Responder en el formato esperado por DataTables
        return jsonify({
            "draw": int(request.args.get("draw", 0)),
            "recordsTotal": total_records,
            "recordsFiltered": total_filtered,
            "data": contabilidad
        })


    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})



@main.route("/<int:codigo>",methods=["DELETE"],strict_slashes=False)
@cross_origin()
def eliminar_contabilidad(codigo):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql = "DELETE FROM contabilidad_dia WHERE id = %s"
        cursor.execute(sql, (codigo,))
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Contabilidad eliminada."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})



@main.route('', methods=['POST'],strict_slashes=False)
@cross_origin()
def insertar_contabilidad():
        try:
                    # Extraer datos del JSON recibido
            fecha = request.json.get('fecha', None)

            retiro_tarjeta = request.json.get('retiro_tarjeta', '').strip()
            retiro_tarjeta = float(retiro_tarjeta) if retiro_tarjeta else 0.0

            disponible = request.json.get('disponible', '').strip()
            disponible = float(disponible) if disponible else 0.0

            frio = request.json.get('frio', '').strip()
            frio = float(frio) if frio else 0.0

            ganancias_pesadas = request.json.get('ganancias_pesadas', '').strip()
            ganancias_pesadas = float(ganancias_pesadas) if ganancias_pesadas else 0.0

            abono_extra = request.json.get('abono_extra', '').strip()
            abono_extra = float(abono_extra) if abono_extra else 0.0

            if not fecha:
                return jsonify({"error": "La fecha es un parámetro obligatorio"}), 400
            conexion = get_connection()
            cursor = conexion.cursor(pymysql.cursors.DictCursor)

                    # Query SQL para insertar o actualizar la contabilidad del día
            sql = """
            INSERT INTO contabilidad_dia (
                fecha, retiro_tarjeta, abono_clientes, abono_provedores, disponible, gastos, 
                frio, ganancias_ventas, ganancias_pesadas, ganancias_totales, 
                sobrante_dia, acumulado_dia, abono_extra, ventas
            )
            SELECT 
                %s AS fecha, 
                %s AS retiro_tarjeta,  
                COALESCE((SELECT SUM(ABONO) FROM transaccion_clientes WHERE FECHA = %s AND tipo_pago_id = 0), 0.00) AS abono_clientes,
                COALESCE((SELECT SUM(abono) FROM transaccion_provedores WHERE fecha = %s AND tipo_pago_id = 0), 0.00) AS abono_provedores,
                %s AS disponible,
                COALESCE((SELECT total_gastos FROM tabla_gastos WHERE fecha = %s), 0.00) AS gastos,
                %s AS frio,  
                COALESCE((SELECT SUM(Ganancia) FROM transaccion_clientes WHERE FECHA = %s), 0.00) AS ganancias_ventas,
                %s AS ganancias_pesadas,  
                (COALESCE((SELECT SUM(Ganancia) FROM transaccion_clientes WHERE FECHA = %s), 0.00) 
                + %s
                + %s
                - COALESCE((SELECT total_gastos FROM tabla_gastos WHERE fecha = %s), 0.00)
                + %s ) AS ganancias_totales, 
                (%s 
                + COALESCE((SELECT SUM(ABONO) FROM transaccion_clientes WHERE FECHA = %s AND tipo_pago_id = 0), 0.00)
                + %s
                - COALESCE((SELECT SUM(abono) FROM transaccion_provedores WHERE fecha = %s AND tipo_pago_id = 0), 0.00)
                - COALESCE((SELECT total_gastos FROM tabla_gastos WHERE fecha = %s), 0.00)
                + %s
                ) AS sobrante_dia,  
                ((%s 
                + COALESCE((SELECT SUM(ABONO) FROM transaccion_clientes WHERE FECHA = %s AND tipo_pago_id = 0), 0.00)
                + %s
                - COALESCE((SELECT SUM(abono) FROM transaccion_provedores WHERE fecha = %s AND tipo_pago_id = 0), 0.00)
                - COALESCE((SELECT total_gastos FROM tabla_gastos WHERE fecha = %s), 0.00)
                + %s)
                + %s) AS acumulado_dia,
                %s AS abono_extra,
                COALESCE((SELECT SUM(IMPORTE) FROM transaccion_clientes WHERE FECHA = %s), 0.00) AS ventas  
            FROM DUAL
            ON DUPLICATE KEY UPDATE 
                retiro_tarjeta = VALUES(retiro_tarjeta),
                abono_clientes = VALUES(abono_clientes),
                abono_provedores = VALUES(abono_provedores),
                disponible = VALUES(disponible),
                gastos = VALUES(gastos),
                frio = VALUES(frio),
                ganancias_ventas = VALUES(ganancias_ventas),
                ganancias_pesadas = VALUES(ganancias_pesadas),
                ganancias_totales = VALUES(ganancias_totales),
                sobrante_dia = VALUES(sobrante_dia),
                acumulado_dia = VALUES(acumulado_dia),
                abono_extra = VALUES(abono_extra),
                ventas = VALUES(ventas);
            """

            values = (
                fecha, 
                retiro_tarjeta, 
                fecha, 
                fecha, 
                disponible, 
                fecha, 
                frio, 
                fecha, 
                ganancias_pesadas, 
                fecha, 
                ganancias_pesadas, 
                frio,
                fecha,
                abono_extra, 
                retiro_tarjeta, 
                fecha, 
                frio, 
                fecha, 
                fecha, 
                abono_extra, 
                retiro_tarjeta, 
                fecha, 
                frio, 
                fecha, 
                fecha, 
                abono_extra, 
                disponible, 
                abono_extra,
                fecha

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