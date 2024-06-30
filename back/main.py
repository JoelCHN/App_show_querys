import os
import time
import pyodbc as sql
import pandas as pd
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
from flask import Flask, render_template, request, redirect, url_for, session
from dotenv import load_dotenv
import statistics

matplotlib.use('Agg')

# load environment variables from .env file

load_dotenv()
# configuration of Flask
app = Flask(__name__, template_folder='../templates', static_folder='../static')
app.secret_key = os.getenv('SECRET_KEY')  # Cargar la clave secreta desde la variable de entorno

# configuration of the database connection
def get_db_connection():
    if 'db_conn_details' in session:
        try:
            conn_details = session['db_conn_details']
            conn = sql.connect(conn_details)
            return conn
        except sql.Error as ex:
            print(f"Error al conectar con la base de datos: {ex}")
            return None
    return None

def close_db_connection(conn):
    if conn:
        conn.close()
        print("Conexión cerrada")
    session.pop('db_conn_details', None)

# Route of the inicial page
@app.route('/')
def login_page():
    return render_template('login.html')

# route of index
@app.route('/index')
def index_page():
    queries = [
        [1, "Número de personas que viajaron por cada estado"],
        [2, "Número de personas que viajaron por cada estado, por cada año"],
        [3, "Número de personas que viajaron de cada combinación municipios, estado"], 
        [4, "Número de vuelos por cada año"],
        [5, "Número de vuelos por cada mes año"], 
        [6, "Número de personas que viajaron de acuerdo a su categoría: Niños hasta 12 años, adolescentes de 13 a 17 años, jóvenes de 18 a 30 años, adultos de 30 a 59 años y adultos mayores de 60 en adelante"], 
        [7, "Número de vuelos por cada aeropuerto de salida en cada año, del aeropuerto se desea saber la clave internacional del aeropuerto y la clave internacional del país al que pertenece el aeropuerto"], 
        [8, "Número de vuelos por aerolínea (detalle_vuelos)"],
        [9, "Número de vuelos realizados por aerolínea, por cada año"], 
        [10, "Número de personas que viajan por cada estado, muestre los 10 estados a los que más personas viajan"], 
        [11, "Número de personas que viajan por cada año"], 
        [12, "Nombre, ciudad y país de los 10 aeropuertos de los que más personas parten hacia algún destino"], 
        [13, "Número de personas que viajan por cada mes"], 
        [14, "Nombre de los 10 municipios de los que más personas viajan, agregue el nombre del estado"], 
        [15, "Nombre de los 10 municipios de los que menos personas viajan, agregue el nombre del estado"],
        [16, "Nombre del o los aeropuertos de los que menos personas parten, muestre la ciudad y el país al que pertenece"]
    ]
    return render_template('index.html', queries=queries)

# Route to manage the login
@app.route('/login', methods=['POST'])
def login():
    windows_auth = request.form.get('checkAuth')
    database = request.form.get('Database')
    user = request.form.get('User')
    password = request.form.get('Password')
    server = request.form.get('Server')

    try:
        if windows_auth:
            connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        else:
            connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password};"
        conn = sql.connect(connection_string)
        session['db_conn_details'] = connection_string
        conn.close()
        return redirect(url_for('index_page')) 
    except sql.Error as ex:
        sqlstate = ex.args[1]
        error_message = f"Error de conexión: {sqlstate}"
        return render_template('login.html', error=error_message)

# route to destroy sesion
@app.route('/logout', methods=['POST'])
def logout():
    conn = get_db_connection()
    close_db_connection(conn)
    return redirect(url_for('login_page'))

# config before the request
@app.before_request
def before_request():
    print("Iniciando nueva solicitud")

#route to show query
@app.route('/query/<int:id>/<string:name>/', methods=['POST'])
def visualizar_consulta(id, name):
    consulta_seleccionada = id
    query = name
    db_conn = get_db_connection()
    #print("Conexión obtenida de la sesión:", db_conn)
    if db_conn:
        try:
            cursor = db_conn.cursor()
            start_time = time.time()
    
            if consulta_seleccionada == 1:
                consulta ='''
                select e.nombre as Estados, count(o.cve_clientes) as Número_de_personas_que_viajaron
                from Ocupaciones as o
                join clientes as c on o.cve_clientes = c.cve_clientes
                join estados as e on c.cve_estados = e.cve_estados
                group by e.nombre
                order by Número_de_personas_que_viajaron desc
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[1] for fila in resultados if isinstance(fila[1], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                                #create the data to create the graphs
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Estados','Número_de_personas_que_viajaron']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Estados', bar_y_col='Número_de_personas_que_viajaron', hist_col='Número_de_personas_que_viajaron', pie_index_col='Estados', pie_values_col='Número_de_personas_que_viajaron',orientation='vertical')
            elif consulta_seleccionada == 2:
                consulta = '''
                select e.nombre as Estado, year(dv.fecha_hora_salida) as año,count(distinct o.cve_clientes) as Número_personas_que_viajaron
                from ocupaciones as o
                join clientes as c on o.cve_clientes = c.cve_clientes
                join estados as e on c.cve_estados = e.cve_estados
                join detalle_vuelos as dv ON o.cve_detalle_vuelos = dv.cve_detalle_vuelos
                group by e.nombre, YEAR(dv.fecha_hora_salida)
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[2] for fila in resultados if isinstance(fila[2], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                                #create the data to create the graphs
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Estado','año','Número_de_personas_que_viajaron']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Estado', bar_y_col='Número_de_personas_que_viajaron', hist_col='Número_de_personas_que_viajaron', pie_index_col='Estado', pie_values_col='Número_de_personas_que_viajaron',orientation='vertical')
            elif consulta_seleccionada == 3:
                consulta ='''
                select e.nombre as Estado, m.nombre as Municipio, count(c.cve_clientes) as Número_de_persona
                from clientes as c
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                join estados as e on e.cve_estados = c.cve_estados
                join municipios as m on e.cve_estados = m.cve_estados and m.cve_estados = c.cve_estados
                group by e.nombre, m.nombre
                order by e.nombre
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[2] for fila in resultados if isinstance(fila[2], (int, float))]
                        
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create the data to create the graphs
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Estado','Municipio','Número_de_persona']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Estado', bar_y_col='Número_de_persona', hist_col='Número_de_persona', pie_index_col='Estado', pie_values_col='Número_de_persona',orientation='vertical')
            elif consulta_seleccionada == 4:
                consulta = '''
                select  year(dv.fecha_hora_salida) as Año,count(dv.cve_vuelos) as Número_de_vuelos
                from detalle_vuelos as dv
                group by year(dv.fecha_hora_salida)
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[1] for fila in resultados if isinstance(fila[1], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create the data to create the graphs
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Año','Número_de_vuelos']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Año', bar_y_col='Número_de_vuelos', hist_col='Número_de_vuelos', pie_index_col='Año', pie_values_col='Número_de_vuelos',orientation='horizontal')
            elif consulta_seleccionada == 5:
                consulta= '''
                select datename(month,dv.fecha_hora_salida) as Mes, year(dv.fecha_hora_salida) as Año, count(dv.cve_vuelos) as Número_de_vuelos
                from detalle_vuelos as dv
                group by datename(month,dv.fecha_hora_salida),year(dv.fecha_hora_salida)
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[2] for fila in resultados if isinstance(fila[2], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create the data to create the graphs
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Mes','Año','Número_de_vuelos']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Mes', bar_y_col='Número_de_vuelos', hist_col='Número_de_vuelos', pie_index_col='Mes', pie_values_col='Número_de_vuelos',orientation='vertical')
            elif consulta_seleccionada == 6:
                consulta = '''
                select case
                when datediff(year, c.fecha_nacimiento,getdate()) <= 12 then 'Niños hasta 12 años'
                when datediff(year, c.fecha_nacimiento, getdate()) between 13 and 17 then 'Adolescentes de 13 a 17 años'
                when datediff(year, c.fecha_nacimiento, getdate()) between 18 and 30 then 'Jóvenes de 18 a 30 años'
                when datediff(year, c.fecha_nacimiento, getdate()) between 31 and 59 then 'Adultos de 31 a 59 años'
                else 'Adultos mayores de 60 años' end as Categoría_de_edad, count(distinct c.cve_clientes) as número_de_personas
                from clientes as c
                join ocupaciones as o on c.cve_clientes = o.cve_clientes
                group by case
                when datediff(year, c.fecha_nacimiento,getdate()) <= 12 then 'Niños hasta 12 años'
                when datediff(year, c.fecha_nacimiento, getdate()) between 13 and 17 then 'Adolescentes de 13 a 17 años'
                when datediff(year, c.fecha_nacimiento, getdate()) between 18 and 30 then 'Jóvenes de 18 a 30 años'
                when datediff(year, c.fecha_nacimiento, getdate()) between 31 and 59 then 'Adultos de 31 a 59 años'
                else 'Adultos mayores de 60 años'
                end
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[1] for fila in resultados if isinstance(fila[1], (int))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Categoría_de_edad','número_de_personas']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Categoría_de_edad', bar_y_col='número_de_personas', hist_col='número_de_personas', pie_index_col='Categoría_de_edad', pie_values_col='número_de_personas',orientation='vertical')
            elif consulta_seleccionada == 7:
                consulta = '''
                select year(dv.fecha_hora_salida) as Año, a.clave_internacional as Clave_internacional, p.clave_internacional as Clave_internacional_del_pais,
                count(dv.cve_vuelos) as Número_de_vuelos
                from detalle_vuelos as dv
                join vuelos as v on dv.cve_vuelos = v.cve_vuelos
                join aeropuertos as a on v.cve_aeropuertos__origen = a.cve_aeropuertos
                join ciudades as c on a.cve_ciudades = c.cve_ciudades
                join paises as p on c.cve_paises = p.cve_paises
                group by year(dv.fecha_hora_salida), a.clave_internacional, p.clave_internacional 
                order by año,a.clave_internacional'''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[3] for fila in resultados if isinstance(fila[3], (int))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Año','Clave_internacional','Clave_internacional_del_pais','Número_de_vuelos']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Año', bar_y_col='Número_de_vuelos', hist_col='Número_de_vuelos', pie_index_col='Año', pie_values_col='Número_de_vuelos',orientation='horizontal')
    

            elif consulta_seleccionada == 8:
                consulta = '''
                select a.nombre as Aerolínea,count(dv.cve_detalle_vuelos) as Número_de_vuelos
                from detalle_vuelos as dv
                join vuelos as v on dv.cve_vuelos = v.cve_vuelos
                join aerolineas as a on v.cve_aerolineas = a.cve_aerolineas
                group by a.nombre
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[1] for fila in resultados if isinstance(fila[1], (int, float))]
                 
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Aerolínea','Número_de_vuelos']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Aerolínea', bar_y_col='Número_de_vuelos', hist_col='Número_de_vuelos', pie_index_col='Aerolínea', pie_values_col='Número_de_vuelos',orientation='horizontal')
            elif consulta_seleccionada == 9:
                consulta = '''
                select a.nombre as Aerolíneas, year(dv.fecha_hora_salida) as Año,count(dv.cve_detalle_vuelos) as Número_de_vuelos
                from detalle_vuelos as dv
                join vuelos as v on dv.cve_vuelos = v.cve_vuelos
                join aerolineas as a on v.cve_aerolineas = a.cve_aerolineas
                group by a.nombre, year(dv.fecha_hora_salida)'''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[2] for fila in resultados if isinstance(fila[2], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None

                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Aerolíneas','Año','Número_de_vuelos']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Aerolíneas', bar_y_col='Número_de_vuelos', hist_col='Número_de_vuelos', pie_index_col='Aerolíneas', pie_values_col='Número_de_vuelos',orientation='horizontal')
            elif consulta_seleccionada == 10:
                consulta = '''
                select top 10 e.nombre as Estado ,count(distinct o.cve_clientes) as Número_de_personas
                from clientes as c
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                join estados as e on e.cve_estados = c.cve_estados
                group by e.nombre
                order by Número_de_personas desc'''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[1] for fila in resultados if isinstance(fila[1], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Estado','Número_de_personas']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Estado', bar_y_col='Número_de_personas', hist_col='Número_de_personas', pie_index_col='Estado', pie_values_col='Número_de_personas',orientation='vertical')

            elif consulta_seleccionada == 11:
                consulta = '''
                select count(o.cve_clientes) as Número_de_personas, year(dv.fecha_hora_salida) as Año
                from clientes as c
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                join detalle_vuelos as dv on dv.cve_detalle_vuelos = o.cve_detalle_vuelos
                group by year(dv.fecha_hora_salida)'''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[1] for fila in resultados if isinstance(fila[1], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Número_de_personas','Año']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Año', bar_y_col='Número_de_personas', hist_col='Número_de_personas', pie_index_col='Año', pie_values_col='Número_de_personas',orientation='horizontal')

            elif consulta_seleccionada == 12:
                consulta = '''
                select top 10 a.nombre as Aeropuerto, ci.nombre as Cuidad,p.nombre as País,count(distinct c.cve_clientes) as Número_de_personas
                from clientes as c
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                join detalle_vuelos as dv on dv.cve_detalle_vuelos = o.cve_detalle_vuelos
                join vuelos as v on v.cve_vuelos = dv.cve_vuelos
                join aeropuertos as a on a.cve_aeropuertos = v.cve_aeropuertos__origen
                join ciudades as ci on ci.cve_ciudades = a.cve_ciudades
                join paises as p on p.cve_paises = ci.cve_paises
                group by a.nombre, ci.nombre, p.nombre
                '''
                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[3] for fila in resultados if isinstance(fila[3], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Aeropuerto','Cuidad','País','Número_de_personas']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Aeropuerto', bar_y_col='Número_de_personas', hist_col='Número_de_personas', pie_index_col='Aeropuerto', pie_values_col='Número_de_personas',orientation='horizontal')
                
            elif consulta_seleccionada == 13:
                consulta = '''
                select datename(month,dv.fecha_hora_salida) as Mes,count(distinct c.cve_clientes) as Número_de_personas
                from clientes as c
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                join detalle_vuelos as dv on dv.cve_detalle_vuelos = o.cve_detalle_vuelos
                group by datename(month,dv.fecha_hora_salida)'''

                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[1] for fila in resultados if isinstance(fila[1], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics    
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None) 
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Mes','Número_de_personas']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Mes', bar_y_col='Número_de_personas', hist_col='Número_de_personas', pie_index_col='Mes', pie_values_col='Número_de_personas',orientation='vertical')

            elif consulta_seleccionada == 14:
                consulta = '''
                select top 10 m.nombre as Municipio,e.nombre as Estado,count(distinct c.cve_clientes) as Número_de_personas
                from clientes as c
                join municipios as m on m.cve_municipios = c.cve_municipios
                join estados as e on e.cve_estados = m.cve_estados
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                group by m.nombre,e.nombre
                order by count(distinct c.cve_clientes) desc'''

                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[2] for fila in resultados if isinstance(fila[2], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None) 
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Municipio','Estado','Número_de_personas']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Municipio', bar_y_col='Número_de_personas', hist_col='Número_de_personas', pie_index_col='Municipio', pie_values_col='Número_de_personas',orientation='vertical')
            elif consulta_seleccionada == 15:
                consulta = '''
                select top 10 m.nombre as Municipio,e.nombre as Estado,count(distinct c.cve_clientes) as Número_de_personas
                from clientes as c
                join municipios as m on m.cve_municipios = c.cve_municipios
                join estados as e on e.cve_estados = m.cve_estados
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                group by m.nombre,e.nombre
                order by count(distinct c.cve_clientes) asc'''

                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[2] for fila in resultados if isinstance(fila[2], (int, float))]
                
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None
                #create data to graphics
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None) 
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Municipio','Estado','Número_de_personas']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Municipio', bar_y_col='Número_de_personas', hist_col='Número_de_personas', pie_index_col='Municipio', pie_values_col='Número_de_personas',orientation='vertical')

            elif consulta_seleccionada == 16:
                consulta = '''select a.nombre as Aeropuerto, ci.nombre as Cuidad, p.nombre as Pais, count(distinct c.cve_clientes) as Número_de_personas
                from clientes as c
                join ocupaciones as o on o.cve_clientes = c.cve_clientes
                join detalle_vuelos as dv on dv.cve_detalle_vuelos = o.cve_detalle_vuelos
                join vuelos as v on v.cve_vuelos = dv.cve_vuelos
                join aeropuertos a on a.cve_aeropuertos = v.cve_aeropuertos__destino
                join ciudades as ci on ci.cve_ciudades = a.cve_ciudades
                join paises as p on p.cve_paises = ci.cve_paises
                group by a.nombre,ci.nombre,p.nombre
                order by count(distinct c.cve_clientes) asc'''

                #Do query and calculate the mode,median and mean
                cursor.execute(consulta)
                resultados = cursor.fetchall()
                datos_numericos = [fila[3] for fila in resultados if isinstance(fila[3], (int, float))]
                if datos_numericos:
                    mode = statistics.mode(datos_numericos)
                    mean = statistics.mean(datos_numericos)
                    median = statistics.median(datos_numericos)
                else:
                    mode = mean = median = None

                #create the data to create graphys
                datos_adaptados = []
                for fila in resultados:
                    fila_adaptada = []
                    for valor in fila:
                        if isinstance(valor, (int, float, str)):
                            fila_adaptada.append(valor)
                        else:
                            fila_adaptada.append(None)
                    datos_adaptados.append(tuple(fila_adaptada))

                print(datos_adaptados)
                columnas = ['Aeropuerto','Cuidad','Pais','Número_de_personas']
                try:
                    df = pd.DataFrame(datos_adaptados, columns=columnas)
                except Exception as e:
                    print(f'Error al crear DataFrame: {e}')
                    return {'error': str(e)}
                graph_path_base = os.path.join('../','static/', 'graphs/', f'query_{consulta_seleccionada}')

                bar_plot, hist_plot, pie_plot = generate_graphs_and_statistics(df, graph_path_base, bar_x_col='Aeropuerto', bar_y_col='Número_de_personas', hist_col='Número_de_personas', pie_index_col='Aeropuerto', pie_values_col='Número_de_personas',orientation='horizontal')
            else:
                print("Invalide ID")

            end_time = time.time()
            #time to do query---count the data---columns to results
            columnas = [desc[0] for desc in cursor.description]
            elapsed_time = end_time - start_time
            count_data = len(resultados)

            #close connection
            cursor.close()          
            
            return render_template('query.html', id_query=consulta_seleccionada, name=query, columnas=columnas, resultados=resultados, time=elapsed_time, count=count_data,mode = mode, mean = mean, median = median, graph_dir=graph_path_base)
        except sql.Error as ex:
            sqlstate = ex.args[1]
            error_message = f"Error de conexión: {sqlstate}"
            return {'error': error_message}
    else:
        return redirect(url_for('login_page'))
#function to create graphys
def generate_bar_chart(df, x_col, y_col, graph_path,orientation):
    sns.barplot(x=x_col, y=y_col, data=df)

    plt.xticks(fontsize=8,rotation=orientation) 
    plt.title('Gráfico de Barras', fontsize=14)  
    plt.xlabel(x_col, fontsize=12)  
    plt.ylabel(y_col, fontsize=12)  
    plt.tight_layout()
    plt.savefig(graph_path)
    plt.clf() 

def generate_histogram(df, column, graph_path):
    fig, ax = plt.subplots()
    ax.hist(df[column], bins=10, linewidth=0.5, edgecolor="white")
    ax.set_title('Histograma')
    ax.set_xlabel(column, fontsize=10) 
    ax.set_ylabel('Frequency')
    plt.savefig(graph_path)
    plt.clf()

def generate_pie_chart(df, index_col, values_col, graph_path):
    df.set_index(index_col)[values_col].plot.pie()
    plt.title('Gráfico de pastel')
    plt.ylabel('')
    plt.savefig(graph_path)
    plt.clf()

def generate_graphs_and_statistics(df, graph_path_base, bar_x_col=None, bar_y_col=None, hist_col=None, pie_index_col=None, pie_values_col=None,orientation=None):

    graph_dir = os.path.dirname(graph_path_base)
    if not os.path.exists(graph_dir):
        os.makedirs(graph_dir)


    bar_plot_path = f"{graph_path_base}_bar_chart.png"
    hist_plot_path = f"{graph_path_base}_hist.png"
    pie_plot_path = f"{graph_path_base}_pie_chart.png"


    if bar_x_col and bar_y_col:
        generate_bar_chart(df, bar_x_col, bar_y_col, bar_plot_path,orientation)
    if hist_col:
        generate_histogram(df, hist_col, hist_plot_path)
    if pie_index_col and pie_values_col:
        generate_pie_chart(df, pie_index_col, pie_values_col, pie_plot_path)
        
    print(bar_plot_path,hist_plot_path,pie_plot_path)
    return bar_plot_path, hist_plot_path, pie_plot_path, 

if __name__ == '__main__':
    app.run(debug=True)
