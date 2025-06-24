from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql
from datetime import datetime

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('SaldoProviders_blueprint', __name__)


@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_saldo_provedores():
    try:
        conexion = get_connection()
        cursor = conexion.cursor(pymysql.cursors.DictCursor)  # 👈 Cambia el cursor a DictCursor

                # Parámetros de paginación y filtros recibidos desde AJAX
        draw = int(request.args.get('draw', 1))  # Control de la petición
        start = int(request.args.get('start', 0))  # Índice inicial de la paginación
        length = int(request.args.get('length', 20))  # Número de registros por página
        cliente_id  = request.args.get("cliente_id")
        cliente_id = int(cliente_id) if cliente_id else ""
        
        # Construcción de la consulta base
        sql_base = """
            SELECT 
                Cli.id AS cliente_id, 
                CONCAT(Cli.nombres, ' ', Cli.apellidos) AS Provedor, 
                SUM(tr_cli.Importe) + SUM(tr_cli.deuda_anterior)- SUM(tr_cli.abono) AS Saldo 
            FROM transaccion_provedores tr_cli 
            JOIN provedores Cli ON tr_cli.provedor_id = Cli.id 
            WHERE 1 = 1
        """
        params = []

        # Aplicar filtro por cliente si está presente
        if cliente_id:
            sql_base += " AND Cli.id = %s"
            params.append(cliente_id)

        # Agrupar por cliente y ordenar por saldo descendente
        sql_base += " GROUP BY Cli.id, Provedor ORDER BY Saldo DESC"

        # Query para contar los registros filtrados
        sql_count = f"SELECT COUNT(*) AS total FROM ({sql_base}) AS subconsulta"
        cursor.execute(sql_count, params)
        total_filtered = cursor.fetchone()["total"]

        # Query para calcular el total de saldo de todas las filas filtradas
        # sql_total_saldo = f"SELECT SUM(Saldo) AS total_saldo FROM ({sql_base}) AS subconsulta"
        # cursor.execute(sql_total_saldo, params)
        # total_saldo = cursor.fetchone()["total_saldo"] or 0.00  # Si no hay resultados, devuelve 0.00

        # Aplicar paginación con LIMIT y OFFSET
        sql_paginated = sql_base + " LIMIT %s OFFSET %s"
        params.extend([length, start])

        cursor.execute(sql_paginated, params)
        saldo_provedores = cursor.fetchall()

        # Contar el total de registros sin filtros
        cursor.execute("SELECT COUNT(*) AS total FROM provedores")
        total_records = cursor.fetchone()["total"]

                # **Calcular el total_saldo solo con los clientes mostrados en la página**
        total_saldo_query = f"SELECT SUM(Saldo) AS total_saldo FROM ({sql_paginated}) AS subconsulta"
        cursor.execute(total_saldo_query, params)
        resultado_saldo = cursor.fetchone()
        total_saldo = resultado_saldo["total_saldo"] if resultado_saldo and resultado_saldo["total_saldo"] else 0.00


        cursor.close()
        conexion.close()

        # Retornar datos en formato compatible con DataTables
        return jsonify({
            "draw": draw,
            "recordsTotal": total_records,
            "recordsFiltered": total_filtered,
            "data": saldo_provedores,
            "totalSaldo": total_saldo  # Enviar el total de saldo filtrado
        })

    except Exception as e:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})



