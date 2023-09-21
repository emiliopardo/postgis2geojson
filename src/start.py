#!/usr/bin/python

from sqlalchemy import create_engine
import geopandas as gpd
import pandas as pd
import numpy as np
from configparser import ConfigParser

# funcion que parsea una archivo de configuracion
def config(filename='database.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    return db

# funcion que devuelve una conexion de sqlalchemy
def connect(parameters):
    db_conection_url = "postgresql://"+parameters['user']+":"+parameters['password']+"@"+parameters['host']+":"+str(parameters['port'])+"/"+parameters['database']
    con = create_engine(db_conection_url)
    return con

# funcion que permite conocer las tablas existentes en un schema de postgresql    
def get_table_list(connection,schema):
    #si no quiero todas las tablas puedo modificar la consulta con un where para que no incluya el nombre de tabla por ejemplo altimetria_lin
    sql = "select f_table_name from public.geometry_columns gc where f_table_schema =\'"+schema+"\' and f_table_name!='altimetria_lin' and f_table_name!='altimetria_pto' order by f_table_name ;"
    # sql = "SELECT f_table_name FROM public.geometry_columns gc WHERE f_table_schema =\'"+schema+"\' ORDER BY f_table_name ;"
    df = pd.read_sql(sql, connection)
    return df

# funcion que optien las columnas de una determianda tabla
def get_column_list(connection, schema, table_name,primary_key):
    sql = "SELECT column_name FROM information_schema.columns WHERE table_schema = '"+schema+"' AND table_name   = '"+table_name+"';"
    df = pd.read_sql(sql, connection)
    # devolvemos todos los campos menos el campo gid que es el primary_key de las tablas y no lo queremos en los geojson
    df = df.drop(df[df['column_name'] == primary_key].index)
    my_row = ''
    for index, row in df.iterrows():
        if index == 0:
            my_row = row['column_name']
        else:
            my_row = my_row+','+row['column_name']
    return(my_row)

# funcion que devuelve una tabla de postgis como dataframe de geopandas haciendo peticiones paginadas
def get_geodataframe(primary_key,connection, schema, table_name):
    my_columns = get_column_list(connection,schema,table_name,primary_key)           
    sql = "SELECT "+my_columns+" FROM \""+schema+"\"."+table_name+" ORDER BY gid ASC;"
    gdf = gpd.read_postgis(sql,connection, geom_col='the_geom', crs='25830')
    # antes de devolver los datos se tranforma al CRS 4258
    return gdf.to_crs(crs=4258) 

# funcion que devuelve una tabla de postgis como dataframe de geopandas haciendo peticiones paginadas
def get_geodataframe_split(primary_key,connection, schema, table_name, offset, limit):
    my_columns = get_column_list(connection,schema,table_name,primary_key)           
    sql = "SELECT "+my_columns+" FROM \""+schema+"\"."+table_name+" ORDER BY gid ASC LIMIT "+str(limit)+" OFFSET "+str(offset)+";"
    gdf = gpd.read_postgis(sql,connection, geom_col='the_geom', crs='25830')
    # antes de devolver los datos se tranforma al CRS 4258
    return gdf.to_crs(crs=4258) 

# funcion que cuenta el numero de registros en la tabla
def count_features(connection, schema,table_name):
    sql = "SELECT COUNT(*) AS features FROM \""+schema+"\"."+table_name+" ;"    
    df = pd.read_sql(sql, connection)
    return df['features'].iloc[0]

if __name__=='__main__':
    # numero maximo de features por cada archivo geojson, si hay mas se generan n_geojson
    max_feature_number = 100000
    parameters =config()
    connection = connect(parameters)
    primary_key = parameters['primary_key']
    table_list = get_table_list(connection, parameters['schema'])

    for index, row in table_list.iterrows():
        feature_number = count_features(connection,parameters['schema'],row['f_table_name'])
        if feature_number > 0 :
            if feature_number > max_feature_number:
                print("es necesario partir la tabla "+ row['f_table_name'] + " feature number: "+str(feature_number)+ " se crearan "+str(int(np.ceil(feature_number/max_feature_number)))+" partes")
                iteration = 1
                offset = 0
                while iteration <= int(np.ceil(feature_number/max_feature_number)):
                    print(f"iteracion numero {iteration}!")
                    data = get_geodataframe_split(primary_key,connection,parameters['schema'],row['f_table_name'], offset, max_feature_number)
                    data.to_file('./output/'+row['f_table_name']+'.geojson_'+str(iteration) ,driver='GeoJSON', encoding='utf-8')
                    iteration = iteration + 1
                    offset = (max_feature_number*(iteration-1))  
            else:
                print("no es necesario partir la tabla "+ row['f_table_name'] + " feature number: "+str(feature_number))
                data = get_geodataframe(primary_key,connection,parameters['schema'],row['f_table_name'])
                data.to_file('./output/'+row['f_table_name']+'.geojson_1',driver='GeoJSON', encoding='utf-8')
                