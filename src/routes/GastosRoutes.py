from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql
from datetime import datetime

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('gastos_blueprint', __name__)



@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_gastos():
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
                c.comida, 
                c.gasolina, 
                c.luz, 
                c.renta, 
                c.sueldos, 
                c.internet, 
                c.honorarios_externos, 
                c.otros, 
                c.descripcion_otros, 
                c.total_gastos
            FROM 
                tabla_gastos c 
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


        # Cláusula ORDER BY, LIMIT y OFFSET para paginación
        sql_paginated = sql_base + " ORDER BY c.fecha DESC LIMIT %s OFFSET %s"
        params.extend([length, start])

        # Ejecutar la consulta paginada
        cursor.execute(sql_paginated, params)
        datos_gastos = cursor.fetchall()

        # Contar el total de registros filtrados
        sql_count = "SELECT COUNT(*) AS total FROM (" + sql_base + ") AS filtered_records"
        cursor.execute(sql_count, params[:-2])  # Excluir LIMIT y OFFSET de los parámetros
        total_filtered = cursor.fetchone()["total"]

            # Contar el total de registros sin filtrar
        cursor.execute("SELECT COUNT(*) AS total FROM tabla_gastos")
        total_records = cursor.fetchone()["total"]

        # Procesar los resultados
        gastos = []
        for fila in datos_gastos:
            gasto = {
                "id": fila["id"],
                "fecha": fila["fecha"],
                "comida": fila["comida"],
                "gasolina": fila["gasolina"],
                "luz": fila["luz"],
                "renta": fila["renta"],
                "sueldos": fila["sueldos"],
                "internet": fila["internet"],
                "honorarios_externos": fila["honorarios_externos"],
                "otros": fila["otros"],
                "descripcion_otros": fila["descripcion_otros"],
                "total_gastos": fila["total_gastos"],
                "fecha_formateada": fila["FECHA_FORMATO"]
            }
            gastos.append(gasto)


        cursor.close()
        conexion.close()
        # Responder en el formato esperado por DataTables
        return jsonify({
            "draw": int(request.args.get("draw", 0)),
            "recordsTotal": total_records,
            "recordsFiltered": total_filtered,
            "data": gastos
        })


    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})

@main.route('', methods=['POST'],strict_slashes=False)
@cross_origin()
def insertar_gasto():

    try:
         # Obtener valores del JSON
        # Obtener el valor de la fecha desde el JSON
                            # Extraer datos del JSON recibido
        fecha = request.json.get('fecha', None)
            
        comida = request.json.get('comida', '').strip()
        comida = float(comida) if comida else 0.0

        gasolina = request.json.get('gasolina', '').strip()
        gasolina = float(gasolina) if gasolina else 0.0

        luz = request.json.get('luz', '').strip()
        luz = float(luz) if luz else 0.0

        renta = request.json.get('renta', '').strip()
        renta = float(renta) if renta else 0.0

        sueldos = request.json.get('sueldos', '').strip()
        sueldos = float(sueldos) if sueldos else 0.0

        internet = request.json.get('internet', '').strip()
        internet = float(internet) if internet else 0.0

        honorarios_externos = request.json.get('honorarios_externos', '').strip()
        honorarios_externos = float(honorarios_externos) if honorarios_externos else 0.0

        otros = request.json.get('otros', '').strip()
        otros = float(otros) if otros else 0.0

        total_gastos = request.json.get('total_gastos', None)
        if isinstance(total_gastos, str):
            total_gastos = total_gastos.strip()
        total_gastos = float(total_gastos) if total_gastos else 0.0

        if not fecha:
            return jsonify({"error": "La fecha es un parámetro obligatorio"}), 400
        conexion = get_connection()
        cursor = conexion.cursor(pymysql.cursors.DictCursor)
        sql = """
                INSERT INTO tabla_gastos (
                    fecha, comida, gasolina, luz, renta, sueldos, internet, honorarios_externos,
            otros, descripcion_otros, total_gastos)
                SELECT 
            %s AS fecha, 
            %s AS comida,  
            %s AS gasolina,
            %s AS luz,
            %s AS renta,
            %s AS sueldos,
            %s AS internet,
            %s AS honorarios_externos,
            %s AS otros,
            %s AS descripcion_otros,
            %s AS total_gastos
                FROM DUAL
                ON DUPLICATE KEY UPDATE 
                    comida = VALUES(comida),
                    gasolina = VALUES(gasolina),
                    luz = VALUES(luz),
                    renta = VALUES(renta),
                    sueldos = VALUES(sueldos),
                    internet = VALUES(internet),
                    honorarios_externos = VALUES(honorarios_externos),
                    otros = VALUES(otros),
                    descripcion_otros = VALUES(descripcion_otros),
                    total_gastos = VALUES(total_gastos);
                """

        values = (
            fecha, 
            comida, 
            gasolina, 
            luz, 
            renta,
            sueldos,
            internet,
            honorarios_externos,
            otros,
            request.json['descripcion_otros'],
            total_gastos

        )

        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Gasto insertado."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})


@main.route("/<int:codigo>",methods=["DELETE"],strict_slashes=False)
@cross_origin()
def eliminar_gasto(codigo):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql = "DELETE FROM tabla_gastos WHERE id = %s"
        cursor.execute(sql, (codigo,))
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Gasto eliminado."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})