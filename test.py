import csv
import psycopg2

# Establece los parámetros de conexión
parametros_conexion = {
    "host": "192.168.2.156",
    "database": "cp",
    "user": "cp_consulta",
    "password": "dQEzkn3JPjhsryXC"
}

# Intenta establecer la conexión
try:
    # Establece la conexión
    conexion = psycopg2.connect(**parametros_conexion)
    print("Conexión exitosa.")

    # Crea un cursor para ejecutar consultas
    cursor = conexion.cursor()

    # Consulta para obtener las tablas
    consulta_tablas = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public'  -- Puedes cambiar 'public' si tus tablas están en otro esquema
    """

    # Ejecuta la consulta para obtener las tablas
    cursor.execute(consulta_tablas)

    # Obtiene los resultados de la consulta
    tablas = cursor.fetchall()

    # Itera sobre las tablas y exporta su contenido a archivos CSV
    for tabla in tablas:
        nombre_tabla = tabla[0]
        print("Exportando contenido de la tabla:", nombre_tabla)

        # Consulta para obtener el contenido de la tabla actual
        consulta_contenido = f"SELECT * FROM {nombre_tabla}"

        # Ejecuta la consulta para obtener el contenido de la tabla actual
        cursor.execute(consulta_contenido)

        # Obtiene los resultados de la consulta
        contenido_tabla = cursor.fetchall()

        # Define el nombre del archivo CSV
        nombre_archivo = f"{nombre_tabla}.csv"

        # Exporta el contenido de la tabla actual a un archivo CSV
        with open(nombre_archivo, "w", newline="") as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            escritor_csv.writerows(contenido_tabla)

        print(f"Exportación de {nombre_archivo} completada.\n")

    # Cierra el cursor y la conexión
    cursor.close()
    conexion.close()

    print("Todos los datos han sido exportados exitosamente.")

except (Exception, psycopg2.Error) as error:
    print("Error al conectar a PostgreSQL:", error)
