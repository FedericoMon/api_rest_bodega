from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql.cursors
from datetime import datetime

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('grafico_gastos_blueprint', __name__)

@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_gastos():
    try:
        conexion = get_connection()
        cursor = conexion.cursor(pymysql.cursors.DictCursor)
                # Filtros personalizados
        fecha_inicio = request.args.get('fecha_inicio_gastos')
        fecha_fin = request.args.get('fecha_fin_gastos')
        frecuencia  = request.args.get("frecuencia_gastos")
        

        if frecuencia == "Diaria":
            # Construcción de la consulta base
            params = []
            sql_diario = """
                SELECT 
                    Cont.fecha AS fecha,
                    Cont.gastos AS gastos
                FROM contabilidad_dia Cont
                    WHERE 1 = 1
            """
            if fecha_inicio:
                sql_diario += " AND fecha >= %s"
                params.append(fecha_inicio)

            if fecha_fin:
                sql_diario += " AND fecha <= %s"
                params.append(fecha_fin)

            sql = sql_diario + """
                ORDER BY fecha ASC;
            """

            cursor.execute(sql, params)
            ganancias_diarass = cursor.fetchall()

            lista_fecha_diaras = []
            lista_gastos= []
            for fila in ganancias_diarass:
                fecha_diaria = fila["fecha"].strftime('%Y-%m-%d')  
                gastos= fila["gastos"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_gastos.append(gastos)
            cursor.close()
            conexion.close()
            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Gastos']
                },
                'grid': {
                    'left': '3%',
                    'right': '4%',
                    'bottom': '3%',
                    'containLabel': True
                },
                'toolbox': {
                    'feature': {
                    'saveAsImage': {}
                    }
                },
                'xAxis': {
                    'type': 'category',
                    'boundaryGap': False,
                    'data': lista_fecha_diaras,
                          'axisLabel': {
                    'rotate': 90 
                            }
                },
                'yAxis': {
                    'type': 'value',
                "axisLabel": { "formatter": '${value}' }

                },
                'series': [
                    {
                    'name': 'Gastos',
                    'type': 'line',
                    'data': lista_gastos
                    }
                        ]
                }
            # Responder en el formato esperado por DataTables
            return jsonify({
                "data": chart

                 })
        if frecuencia == "Semanal":
             # Construcción de la consulta base
            params_semanal = []
            sql_semanal = """
                    SELECT
                        YEARWEEK(fecha, 1) AS ano_semana,
                        STR_TO_DATE(CONCAT(YEAR(fecha), WEEK(fecha, 1), ' Monday'), '%%X%%V %%W') AS inicio_semana,
                        SUM(gastos) AS gastos_semanal
                    FROM contabilidad_dia
                    WHERE 1 = 1
            """
            if fecha_inicio:
                sql_semanal += " AND fecha >= %s"
                params_semanal.append(fecha_inicio)

            if fecha_fin:
                sql_semanal += " AND fecha <= %s"
                params_semanal.append(fecha_fin)

            sql = sql_semanal + """
                GROUP BY YEARWEEK(fecha, 1)
                ORDER BY inicio_semana ASC;
            """

            cursor.execute(sql, params_semanal)
            ganancias_diarass = cursor.fetchall()

            lista_fecha_diaras = []
            lista_gastos = []
            for fila in ganancias_diarass:
                fecha_diaria = fila["inicio_semana"].strftime('%Y-%m-%d')  
                gastos= fila["gastos_semanal"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_gastos.append(gastos)
            cursor.close()
            conexion.close()
            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Gastos']
                },
                'grid': {
                    'left': '3%',
                    'right': '4%',
                    'bottom': '3%',
                    'containLabel': True
                },
                'toolbox': {
                    'feature': {
                    'saveAsImage': {}
                    }
                },
                'xAxis': {
                    'type': 'category',
                    'boundaryGap': False,
                    'data': lista_fecha_diaras,
                          'axisLabel': {
                    'rotate': 90 
                            }
                },
                'yAxis': {
                    'type': 'value',
                "axisLabel": { "formatter": '${value}' }

                },
                'series': [
                    {
                    'name': 'Gastos',
                    'type': 'line',
                    'data': lista_gastos
                    }
                        ]
                }
            # Responder en el formato esperado por DataTables
            return jsonify({
                "data": chart

                 })

        if frecuencia == "Mensual":
             # Construcción de la consulta base
            params_semanal = []
            sql_semanal = """
                SELECT
                YEAR(fecha) AS año,
                MONTH(fecha) AS mes,
                DATE_FORMAT(fecha, '%%Y-%%m') AS periodo_mensual,
                SUM(gastos) AS gastos_mensual
                FROM contabilidad_dia
                    WHERE 1 = 1
            """
            if fecha_inicio:
                sql_semanal += " AND fecha >= %s"
                params_semanal.append(fecha_inicio)

            if fecha_fin:
                sql_semanal += " AND fecha <= %s"
                params_semanal.append(fecha_fin)

            sql = sql_semanal + """
                GROUP BY YEAR(fecha), MONTH(fecha)
                ORDER BY periodo_mensual ASC;
            """

            cursor.execute(sql, params_semanal)
            ganancias_diarass = cursor.fetchall()

            lista_fecha_diaras = []
            lista_gastos = []
            for fila in ganancias_diarass:
                fecha_diaria = fila["periodo_mensual"] 
                gastos= fila["gastos_mensual"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_gastos.append(gastos)
            cursor.close()
            conexion.close()
            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Gastos']
                },
                'grid': {
                    'left': '3%',
                    'right': '4%',
                    'bottom': '3%',
                    'containLabel': True
                },
                'toolbox': {
                    'feature': {
                    'saveAsImage': {}
                    }
                },
                'xAxis': {
                    'type': 'category',
                    'boundaryGap': False,
                    'data': lista_fecha_diaras,
                          'axisLabel': {
                    'rotate': 90 
                            }
                },
                'yAxis': {
                    'type': 'value',
                "axisLabel": { "formatter": '${value}' }

                },
                'series': [
                    {
                    'name': 'Gastos',
                    'type': 'line',
                    'data': lista_gastos
                    }
                        ]
                }
            # Responder en el formato esperado por DataTables
            return jsonify({
                "data": chart

                 })

        if frecuencia == "Anual":
             # Construcción de la consulta base
            params_semanal = []
            sql_semanal = """
                    SELECT
                    YEAR(fecha) AS año,
                    SUM(gastos) AS gastos_anual
                    FROM contabilidad_dia
                    WHERE 1 = 1
            """
            if fecha_inicio:
                sql_semanal += " AND fecha >= %s"
                params_semanal.append(fecha_inicio)

            if fecha_fin:
                sql_semanal += " AND fecha <= %s"
                params_semanal.append(fecha_fin)

            sql = sql_semanal + """
                    GROUP BY YEAR(fecha)
                    ORDER BY año;
            """

            cursor.execute(sql, params_semanal)
            ganancias_diarass = cursor.fetchall()

            lista_fecha_diaras = []
            lista_gastos= []
            for fila in ganancias_diarass:
                fecha_diaria = fila["año"] 
                gastos= fila["gastos_anual"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_gastos.append(gastos)
            cursor.close()
            conexion.close()
            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Gastos']
                },
                'grid': {
                    'left': '3%',
                    'right': '4%',
                    'bottom': '3%',
                    'containLabel': True
                },
                'toolbox': {
                    'feature': {
                    'saveAsImage': {}
                    }
                },
                'xAxis': {
                    'type': 'category',
                    'boundaryGap': False,
                    'data': lista_fecha_diaras,
                          'axisLabel': {
                    'rotate': 90 
                            }
                },
                'yAxis': {
                    'type': 'value',
                "axisLabel": { "formatter": '${value}' }

                },
                'series': [
                    {
                    'name': 'Gastos',
                    'type': 'line',
                    'data': lista_gastos
                    }]
                }
            # Responder en el formato esperado por DataTables
            return jsonify({
                "data": chart

                 })


    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})









