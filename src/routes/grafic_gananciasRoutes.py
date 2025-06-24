from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql.cursors
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('grafico_ganancias_blueprint', __name__)

@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_ganancias():
    try:
        conexion = get_connection()
        cursor = conexion.cursor(pymysql.cursors.DictCursor)
                # Filtros personalizados
        fecha_inicio = request.args.get('fecha_inicio_ganancia')
        fecha_fin = request.args.get('fecha_fin_ganancia')
        frecuencia  = request.args.get("frecuencia_ganancia")
        

        if frecuencia == "Diaria":
            # Construcción de la consulta base
            params = []
            sql_diario = """
                SELECT 
                    Cont.fecha AS fecha,
                    Cont.ganancias_ventas AS ganancias_ventas,
                    Cont.ganancias_totales AS ganancias_totales
                FROM contabilidad_dia Cont
                    WHERE 1 = 1
            """

            sql_ventas = """
                SELECT 
                    Cont.fecha AS fecha,
                    Cont.ventas AS ventas
                FROM contabilidad_dia Cont
                    WHERE 1 = 1
            """
            if fecha_inicio:
                sql_diario += " AND fecha >= %s"
                sql_ventas += " AND fecha >= %s"
                params.append(fecha_inicio)

            if fecha_fin:
                sql_diario += " AND fecha <= %s"
                sql_ventas += " AND fecha <= %s"
                params.append(fecha_fin)

            sql = sql_diario + """
                ORDER BY fecha ASC;
            """

            sqlv = sql_ventas + """
                ORDER BY fecha ASC;
            """


            cursor.execute(sql, params)
            ganancias_diarass = cursor.fetchall()

            cursor.execute(sqlv, params)
            ventas_diarass = cursor.fetchall()

            lista_fecha_diaras = []
            lista_ganancias_ventas = []
            lista_ganancias_totales = []
            for fila in ganancias_diarass:
                fecha_diaria = fila["fecha"].strftime('%Y-%m-%d')  
                ganancias_ventas= fila["ganancias_ventas"]
                ganancias_totales= fila["ganancias_totales"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_ganancias_ventas.append(ganancias_ventas)
                lista_ganancias_totales.append(ganancias_totales)
            lista_ventas=[]
            for fila in ventas_diarass:
                ventas=fila["ventas"]
                lista_ventas.append(ventas)
            cursor.close()
            conexion.close()
         #   rendimiento=list(map(lambda x,y: (x-y) ))
            rendimiento=[]
            dos_decimales = Decimal("0.01")
            for i in range(0,len(lista_ventas)):
                if lista_ventas[i]!=0:
                    rend= (lista_ganancias_ventas[i]/lista_ventas[i])*Decimal("100.00")
                    rend = rend.quantize(dos_decimales, rounding=ROUND_HALF_UP)
                    rendimiento.append(rend)
                else:
                    rendimiento.append(Decimal("0.00"))
            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Ganancia Neta', 'Ganancias Ventas', "Ventas","rendimiento"]
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
                    'name': 'Ganancia Neta',
                    'type': 'line',
                    'data': lista_ganancias_totales
                    },
                    {
                    'name': 'Ganancias Ventas',
                    'type': 'line',
                    'data': lista_ganancias_ventas
                    },
                    {
                    'name': 'Ventas',
                    'type': 'line',
                    'data': lista_ventas
                    },
                    {
                    'name': 'rendimiento',
                    'type': 'line',
                    'data': rendimiento
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
                        SUM(ganancias_ventas) AS ganancias_ventas_semana,
                        SUM(ganancias_totales) AS ganancias_totales_semana
                    FROM contabilidad_dia
                    WHERE 1 = 1
            """

            sql_ventas = """
                    SELECT
                        YEARWEEK(fecha, 1) AS ano_semana,
                        STR_TO_DATE(CONCAT(YEAR(fecha), WEEK(fecha, 1), ' Monday'), '%%X%%V %%W') AS inicio_semana,
                        SUM(ventas) AS ventas_semanal
                    FROM contabilidad_dia
                    WHERE 1 = 1
            """

            if fecha_inicio:
                sql_semanal += " AND fecha >= %s"
                sql_ventas += " AND fecha >= %s"
                params_semanal.append(fecha_inicio)

            if fecha_fin:
                sql_semanal += " AND fecha <= %s"
                sql_ventas += " AND fecha <= %s"
                params_semanal.append(fecha_fin)

            sql = sql_semanal + """
                GROUP BY YEARWEEK(fecha, 1)
                ORDER BY inicio_semana ASC;
            """
            sqlv = sql_ventas + """
                GROUP BY YEARWEEK(fecha, 1)
                ORDER BY inicio_semana ASC;
            """

            cursor.execute(sql, params_semanal)
            ganancias_diarass = cursor.fetchall()

            cursor.execute(sqlv, params_semanal)
            ventas_diarass= cursor.fetchall()

            lista_fecha_diaras = []
            lista_ganancias_ventas = []
            lista_ganancias_totales = []
            for fila in ganancias_diarass:
                fecha_diaria = fila["inicio_semana"].strftime('%Y-%m-%d')  
                ganancias_ventas= fila["ganancias_ventas_semana"]
                ganancias_totales= fila["ganancias_totales_semana"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_ganancias_ventas.append(ganancias_ventas)
                lista_ganancias_totales.append(ganancias_totales)
            lista_ventas=[]
            for fila in ventas_diarass:
                ventas=fila["ventas_semanal"]
                lista_ventas.append(ventas)
            cursor.close()
            conexion.close()
         #   rendimiento=list(map(lambda x,y: (x-y) ))
            rendimiento=[]
            dos_decimales = Decimal("0.01")
            for i in range(0,len(lista_ventas)):
                if lista_ventas[i]!=0:
                    rend= (lista_ganancias_ventas[i]/lista_ventas[i])*Decimal("100.00")
                    rend = rend.quantize(dos_decimales, rounding=ROUND_HALF_UP)
                    rendimiento.append(rend)
                else:
                    rendimiento.append(Decimal("0.00"))
            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Ganancia Neta', 'Ganancias Ventas',"Ventas","rendimiento"]
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
                    'name': 'Ganancia Neta',
                    'type': 'line',
                    'data': lista_ganancias_totales
                    },
                    {
                    'name': 'Ganancias Ventas',
                    'type': 'line',
                    'data': lista_ganancias_ventas
                    },
                    {
                    'name': 'Ventas',
                    'type': 'line',
                    'data': lista_ventas
                    },
                    {
                    'name': 'rendimiento',
                    'type': 'line',
                    'data': rendimiento
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
                SUM(ganancias_ventas) AS ganancias_ventas_mes,
                SUM(ganancias_totales) AS ganancias_totales_mes
                FROM contabilidad_dia
                    WHERE 1 = 1
            """

            sql_ventas = """
                SELECT
                YEAR(fecha) AS año,
                MONTH(fecha) AS mes,
                DATE_FORMAT(fecha, '%%Y-%%m') AS periodo_mensual,
                SUM(ventas) AS ventas_mes
                FROM contabilidad_dia
                    WHERE 1 = 1
            """
            if fecha_inicio:
                sql_semanal += " AND fecha >= %s"
                sql_ventas += " AND fecha >= %s"
                params_semanal.append(fecha_inicio)

            if fecha_fin:
                sql_semanal += " AND fecha <= %s"
                sql_ventas += " AND fecha <= %s"
                params_semanal.append(fecha_fin)

            sql = sql_semanal + """
                GROUP BY YEAR(fecha), MONTH(fecha)
                ORDER BY periodo_mensual ASC;
            """

            sqlv = sql_ventas + """
                GROUP BY YEAR(fecha), MONTH(fecha)
                ORDER BY periodo_mensual ASC;
            """

            cursor.execute(sql, params_semanal)
            ganancias_diarass = cursor.fetchall()

            cursor.execute(sqlv, params_semanal)
            ventas_diarass = cursor.fetchall()

            lista_fecha_diaras = []
            lista_ganancias_ventas = []
            lista_ganancias_totales = []
            for fila in ganancias_diarass:
                fecha_diaria = fila["periodo_mensual"] 
                ganancias_ventas= fila["ganancias_ventas_mes"]
                ganancias_totales= fila["ganancias_totales_mes"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_ganancias_ventas.append(ganancias_ventas)
                lista_ganancias_totales.append(ganancias_totales)
            lista_ventas=[]
            for fila in ventas_diarass:
                ventas=fila["ventas_mes"]
                lista_ventas.append(ventas)
            cursor.close()
            conexion.close()
         #   rendimiento=list(map(lambda x,y: (x-y) ))
            rendimiento=[]
            dos_decimales = Decimal("0.01")
            for i in range(0,len(lista_ventas)):
                if lista_ventas[i]!=0:
                    rend= (lista_ganancias_ventas[i]/lista_ventas[i])*Decimal("100.00")
                    rend = rend.quantize(dos_decimales, rounding=ROUND_HALF_UP)
                    rendimiento.append(rend)
                else:
                    rendimiento.append(Decimal("0.00"))

            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Ganancia Neta', 'Ganancias Ventas',"Ventas","rendimiento"]
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
                    'name': 'Ganancia Neta',
                    'type': 'line',
                    'data': lista_ganancias_totales
                    },
                    {
                    'name': 'Ganancias Ventas',
                    'type': 'line',
                    'data': lista_ganancias_ventas
                    },
                    {
                    'name': 'Ventas',
                    'type': 'line',
                    'data': lista_ventas
                    },
                    {
                    'name': 'rendimiento',
                    'type': 'line',
                    'data': rendimiento
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
                    SUM(ganancias_ventas) AS ganancias_ventas_anual,
                    SUM(ganancias_totales) AS ganancias_totales_anual
                    FROM contabilidad_dia
                    WHERE 1 = 1
            """

            sql_ventas = """
                    SELECT
                    YEAR(fecha) AS año,
                    SUM(ventas) AS ventas_anual
                    FROM contabilidad_dia
                    WHERE 1 = 1
            """

            if fecha_inicio:
                sql_semanal += " AND fecha >= %s"
                sql_ventas += " AND fecha >= %s"
                params_semanal.append(fecha_inicio)

            if fecha_fin:
                sql_semanal += " AND fecha <= %s"
                sql_ventas += " AND fecha <= %s"
                params_semanal.append(fecha_fin)

            sql = sql_semanal + """
                    GROUP BY YEAR(fecha)
                    ORDER BY año;
            """

            sqlv = sql_ventas + """
                    GROUP BY YEAR(fecha)
                    ORDER BY año;
            """

            cursor.execute(sql, params_semanal)
            ganancias_diarass = cursor.fetchall()

            cursor.execute(sqlv, params_semanal)
            ventas_diarass = cursor.fetchall()

            lista_fecha_diaras = []
            lista_ganancias_ventas = []
            lista_ganancias_totales = []
            for fila in ganancias_diarass:
                fecha_diaria = fila["año"] 
                ganancias_ventas= fila["ganancias_ventas_anual"]
                ganancias_totales= fila["ganancias_totales_anual"]
                lista_fecha_diaras.append(fecha_diaria)
                lista_ganancias_ventas.append(ganancias_ventas)
                lista_ganancias_totales.append(ganancias_totales)
            lista_ventas=[]
            for fila in ventas_diarass:
                ventas=fila["ventas_anual"]
                lista_ventas.append(ventas)
            cursor.close()
            conexion.close()
         #   rendimiento=list(map(lambda x,y: (x-y) ))
            rendimiento=[]
            dos_decimales = Decimal("0.01")
            for i in range(0,len(lista_ventas)):
                if lista_ventas[i]!=0:
                    rend= (lista_ganancias_ventas[i]/lista_ventas[i])*Decimal("100.00")
                    rend = rend.quantize(dos_decimales, rounding=ROUND_HALF_UP)
                    rendimiento.append(rend)
                else:
                    rendimiento.append(Decimal("0.00"))

            chart = {
                'title': {
                    'text': frecuencia,
                    "formatter": '${value}'
                },
                'tooltip': {
                    'trigger': 'axis'
                },
                'legend': {
                    'data': ['Ganancia Neta', 'Ganancias Ventas',"Ventas","rendimiento"]
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
                    'name': 'Ganancia Neta',
                    'type': 'line',
                    'data': lista_ganancias_totales
                    },
                    {
                    'name': 'Ganancias Ventas',
                    'type': 'line',
                    'data': lista_ganancias_ventas
                    },
                    {
                    'name': 'Ventas',
                    'type': 'line',
                    'data': lista_ventas
                    },
                    {
                    'name': 'rendimiento',
                    'type': 'line',
                    'data': rendimiento
                    }
                        ]
                }
            # Responder en el formato esperado por DataTables
            return jsonify({
                "data": chart

                 })


    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})









