import csv
import psycopg2

# Establece los parámetros de conexión
parametros_conexion = {
    "host": "192.168.2.156",
    "database": "cp",
    "user": "cp_consulta",
    "password": "dQEzkn3JPjhsryXC"
}

# Nombre de la tabla que deseas exportar (en este caso, "Catalog")
nombre_tabla_a_exportar = "ganaderia_lista_desposte"

# Intenta establecer la conexión
try:
    # Establece la conexión
    conexion = psycopg2.connect(**parametros_conexion)
    print("Conexión exitosa.")

    # Crea un cursor para ejecutar consultas
    cursor = conexion.cursor()

    # Consulta para obtener el contenido de la tabla específica
    consulta_contenido = f"SELECT * FROM {nombre_tabla_a_exportar}"

    # Ejecuta la consulta para obtener el contenido de la tabla
    cursor.execute(consulta_contenido)

    # Obtiene los resultados de la consulta
    contenido_tabla = cursor.fetchall()

    # Define el nombre del archivo CSV
    nombre_archivo = f"{nombre_tabla_a_exportar}.csv"

    # Exporta el contenido de la tabla a un archivo CSV
    with open(nombre_archivo, "w", newline="") as archivo_csv:
        escritor_csv = csv.writer(archivo_csv)
        escritor_csv.writerows(contenido_tabla)

    print(f"Exportación de {nombre_archivo} completada.")

    # Cierra el cursor y la conexión
    cursor.close()
    conexion.close()

    print("La tabla ha sido exportada exitosamente.")

except (Exception, psycopg2.Error) as error:
    print("Error al conectar a PostgreSQL:", error)
