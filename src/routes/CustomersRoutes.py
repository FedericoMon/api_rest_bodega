from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('customers_blueprint', __name__)




@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_clientes():
    try:
        conexion = get_connection()
        cursor = conexion.cursor()

        sql_clientes = """SELECT c.id, c.nombres, c.apellidos, c.nombre_negocio, c.telefono, 
                c.telefono_2, c.correo, c.direccion, c.RFC, t.nombre AS `TIPO NEGOCIO`, c.Tipo_negocio_id
                FROM clientes c 
                JOIN tipo_negocio t ON c.Tipo_negocio_id = t.id
                ORDER BY c.nombres ASC;"""

        sql_tipo_negocio = "SELECT * FROM tipo_negocio"

        sql_tipo_transaccion = "SELECT * FROM tipo_transaccion"

        sql_tipo_canal = "SELECT * FROM tipo_canal"

        sql_status_cuenta = "SELECT * FROM status_cuenta"

        sql_tipo_pago = "SELECT * FROM tipo_pago"


                # Ejecuta la primera consulta
        cursor.execute(sql_clientes)
        datos_clientes = cursor.fetchall()

        # Ejecuta la segunda consulta extraer tabla tipo negocio
        cursor.execute(sql_tipo_negocio)
        datos_tipo_negocio = cursor.fetchall()

                # Ejecuta la segunda consulta extraer tabla tipo negocio
        cursor.execute(sql_tipo_transaccion)
        datos_tipo_transaccion = cursor.fetchall()

        cursor.execute(sql_tipo_canal)
        datos_tipo_canal = cursor.fetchall()

        cursor.execute(sql_status_cuenta)
        datos_status_cuenta = cursor.fetchall()

        cursor.execute(sql_tipo_pago)
        datos_tipo_pago = cursor.fetchall()



        clientes=[]
        for fila in datos_clientes:
            cliente={"codigo":fila[0],
            "nombres":fila[1],
            "apellidos":fila[2],
            "nombre_negocio":fila[3],
            "telefono":fila[4],
            "telefono_adicional":fila[5],
            "correo":fila[6],
            "direccion":fila[7],
            "rfc":fila[8],
            "tipo_negocio":fila[9],
            "tipo_negocio_id":fila[10]  }
            clientes.append(cliente)        
        
                # Procesa los datos de la segunda consulta
        tipo_negocio = []
        for fila in datos_tipo_negocio:
            negocio = {
                "id": fila[0],
                "nombre": fila[1]
            }
            tipo_negocio.append(negocio)

                        # Procesa los datos de la segunda consulta
        tipo_transaccion = []
        for fila in datos_tipo_transaccion:
            transaccion = {
                "id": fila[0],
                "nombre": fila[1]
            }
            tipo_transaccion.append(transaccion)

                                # Procesa los datos de la segunda consulta
        tipo_canal = []
        for fila in datos_tipo_canal:
            canal = {
                "id": fila[0],
                "nombre": fila[1],
                "precio_diario": fila[2]

            }
            tipo_canal.append(canal)


        status_cuenta = []
        for fila in datos_status_cuenta:
            status = {
                "id": fila[0],
                "nombre": fila[1],

            }
            status_cuenta.append(status)

        tipo_pago = []
        for fila in datos_tipo_pago:
            pago = {
                "id": fila[0],
                "nombre": fila[1],

            }
            tipo_pago.append(pago)

        cursor.close()
        conexion.close()
                # Devuelve los datos en formato JSON
        return jsonify({
            "clientes": clientes,
            "tipo_negocio": tipo_negocio,
            "tipo_transaccion":tipo_transaccion,
            "tipo_canal":tipo_canal,
            "status_cuenta":status_cuenta,     
            "tipo_pago":tipo_pago,       
            "mensaje": "Datos listados correctamente."
        })

    
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())

        return jsonify({'message': ex, 'success': False})



@main.route("/<int:codigo>",methods=["DELETE"],strict_slashes=False)
@cross_origin()
def eliminar_cliente(codigo):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql = "DELETE FROM clientes WHERE id = %s"
        cursor.execute(sql, (codigo,))
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Cliente eliminado."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})


@main.route('', methods=['POST'],strict_slashes=False)
@cross_origin()
def createCustomers():
    try:
        # 2. Intentar obtener el JSON del cuerpo de la solicitud
        data = request.get_json(force=True)
        print("Datos recibidos:", data)

        # 3. Lógica condicional para determinar si es creación o actualización
        if 'codigo' in data:
            return updateCustomer(data)
        else:
            return createCustomer(data)
    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})



def createCustomer(data):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="INSERT INTO `clientes` (`id`, `apellidos`, `nombres`, `nombre_negocio`, `telefono`, `telefono_2`, `correo`, `direccion`, `RFC`,`Tipo_negocio_id`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        values = (
            data['apellidos'], 
            data['nombres'], 
            data['nombre_negocio'], 
            data['telefono'], 
            data['telefono_adicional'], 
            data['correo'], 
            data['direccion'], 
            data['rfc'],
            int(data['tipo_negocio_id'])         
        )
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Cliente Guardado."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})


def updateCustomer(data):

    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="UPDATE `clientes` SET `apellidos`=%s, `nombres`=%s, `nombre_negocio`=%s, `telefono`=%s, `telefono_2`=%s, `correo`=%s, `direccion`=%s, `RFC`=%s, `Tipo_negocio_id`=%s WHERE `clientes`.`id` = %s;"
        values = (
            data['apellidos'], 
            data['nombres'], 
            data['nombre_negocio'], 
            data['telefono'], 
            data['telefono_adicional'], 
            data['correo'], 
            data['direccion'], 
            data['rfc'],
            int(data['tipo_negocio_id']),
            data['codigo']
        )
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Cliente Actualizado."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'mensaje': 'Error al procesar la solicitud', 'error': str(ex), 'success': False})


