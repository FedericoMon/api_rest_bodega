from flask import Blueprint, request, jsonify
from flask_cors import CORS, cross_origin
import traceback

# Logger
from src.utils.Logger import Logger
# Database
from src.database.db_mysql import get_connection
main = Blueprint('providers_blueprint', __name__)



@main.route('', methods=['GET'],strict_slashes=False)
@cross_origin()
def listar_provedores():
    try:
        conexion = get_connection()
        cursor = conexion.cursor()

        sql_provedores = """SELECT id, nombres, apellidos, nombre_empresa, telefono, 
                telefono_2, correo, direccion, RFC
                FROM provedores
                ORDER BY nombres ASC;"""
        
        sql_tipo_transaccion_prov = "SELECT * FROM tipo_transaccion_prov"

        sql_tipo_canal = "SELECT * FROM tipo_canal"

        sql_status_cuenta = "SELECT * FROM status_cuenta"

        sql_tipo_pago = "SELECT * FROM tipo_pago"


                        # Ejecuta la primera consulta
        cursor.execute(sql_provedores)
        datos_provedores = cursor.fetchall()

        cursor.execute(sql_tipo_transaccion_prov)
        datos_tipo_transaccion_prov = cursor.fetchall()

        cursor.execute(sql_tipo_canal)
        datos_tipo_canal = cursor.fetchall()

        cursor.execute(sql_status_cuenta)
        datos_status_cuenta = cursor.fetchall()

        cursor.execute(sql_tipo_pago)
        datos_tipo_pago = cursor.fetchall()


        provedores=[]
        for fila in datos_provedores:
            cliente={"codigo":fila[0],
            "nombres":fila[1],
            "apellidos":fila[2],
            "nombre_empresa":fila[3],
            "telefono":fila[4],
            "telefono_adicional":fila[5],
            "correo":fila[6],
            "direccion":fila[7],
            "rfc":fila[8], }
            provedores.append(cliente)        
        
        tipo_transaccion_prov = []
        for fila in datos_tipo_transaccion_prov:
            transaccion = {
                "id": fila[0],
                "nombre": fila[1]
            }
            tipo_transaccion_prov.append(transaccion)



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
            "provedoress": provedores,  
            "tipo_transaccion_provv":tipo_transaccion_prov,
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
def eliminar_provedor(codigo):
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql = "DELETE FROM provedores WHERE id = %s"
        cursor.execute(sql, (codigo,))
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Provedor eliminado."})

    except Exception as ex:
        Logger.add_to_log("error", str(ex))
        Logger.add_to_log("error", traceback.format_exc())
        return jsonify({'message': ex, 'success': False})




@main.route('', methods=['POST'],strict_slashes=False)
@cross_origin()
def createProvedores():
    if 'codigo' in request.json:
        updateProvedores()
    else:
        createProvedor()
    return "ok"


def createProvedor():
    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="INSERT INTO `provedores` (`id`, `apellidos`, `nombres`, `telefono`, `telefono_2`, `correo`, `direccion`, `RFC`,`nombre_empresa`) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s, %s);"
        values = (
            request.json['apellidos'], 
            request.json['nombres'], 
            request.json['telefono'], 
            request.json['telefono_adicional'], 
            request.json['correo'], 
            request.json['direccion'], 
            request.json['rfc'],        
            request.json['nombre_empresa'] 
        )
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Provedor Guardado."})

    except Exception as ex:
        # Imprime el error en la consola para depuración
        print("Error:", ex)
        return jsonify({"mensaje":ex})


def updateProvedores():

    try:
        conexion = get_connection()
        cursor = conexion.cursor()
        sql="UPDATE `provedores` SET `apellidos`=%s, `nombres`=%s,  `telefono`=%s, `telefono_2`=%s, `correo`=%s, `direccion`=%s, `RFC`=%s, `nombre_empresa`=%s WHERE `provedores`.`id` = %s;"
        values = (
            request.json['apellidos'], 
            request.json['nombres'],  
            request.json['telefono'], 
            request.json['telefono_adicional'], 
            request.json['correo'], 
            request.json['direccion'], 
            request.json['rfc'],
            request.json['nombre_empresa'],
            request.json['codigo']
        )
        cursor.execute(sql, values)
        # Confirmar la acción de inserción
        conexion.commit()
        cursor.close()
        conexion.close()
        return jsonify({ "mensaje":"Provedor Actualizado."})

    except Exception as ex:
        # Imprime el error en la consola para depuración
        print("Error:", ex)
        return jsonify({"mensaje":ex})





