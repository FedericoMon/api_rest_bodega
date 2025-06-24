from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback
import pymysql
from datetime import datetime

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('FilterDashboard_blueprint', __name__)


@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_saldo_clientes():
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        
        # Construcción de la consulta base
        sql_clients = """
            SELECT 
                Cli.id AS id,
                CONCAT(Cli.nombres, ' ', Cli.apellidos) AS nombre
            FROM clientes Cli
            ORDER BY nombre ASC;

        """


        # Construcción de la consulta base
        sql_providers = """
            SELECT 
                prov.id AS id,
                CONCAT(prov.nombres, ' ', prov.apellidos) AS nombre
            FROM provedores prov
            ORDER BY nombre ASC;

        """


                                # Ejecuta la primera consulta
        cursor.execute(sql_clients)
        datos_clientss = cursor.fetchall()

        cursor.execute(sql_providers)
        datos_providerss = cursor.fetchall()

        datos_clients = []
        for fila in datos_clientss:
            client = {
                "id": fila[0],
                "nombre": fila[1]
            }
            datos_clients.append(client)

        datos_providers = []
        for fila in datos_providerss:
            provider = {
                "id": fila[0],
                "nombre": fila[1]
            }
            datos_providers.append(provider)


        cursor.close()
        conexion.close()
                # Devuelve los datos en formato JSON
        return jsonify({
            "nombre_clients":datos_clients,  
            "nombre_providers":datos_providers,
            "mensaje": "Datos listados correctamente."
        })

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})
