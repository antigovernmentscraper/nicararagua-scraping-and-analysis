"""
Created on Sun DEC  28 18:34:15 2022

Purpose: File for matching ERD data against website information for 'Proveedores'
"""

import requests
from bs4 import BeautifulSoup
import concurrent.futures
from pathlib import Path
import pandas as pd, pandas.errors
from requests.exceptions import MissingSchema, SSLError, ContentDecodingError, ConnectionError, Timeout, ChunkedEncodingError, InvalidURL, TooManyRedirects
from urllib3.exceptions import LocationParseError
import re
from datetime import datetime
import time

from http.client import HTTPConnection
HTTPConnection._http_vsn_str = "HTTP/1.0"

#--------------------------------------
def process_resultpage(resp, datos):
    flag_for_pagination = False
    try:
        #session = session1
        soup = BeautifulSoup(resp.text, 'html.parser')
        result_title = soup.find(id="mostrarResultadoView:title").string
        total_results = soup.find(id="mostrarResultadoView:listadoProvForm:Pluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id13").string
        number_of_results = re.findall(r'\d+,\d+', total_results)
        print(f"\n Total Results for {result_title}: {total_results} - Total = {number_of_results}")
        #send the flag back in return
        if int(number_of_results[0].replace(',', '')) >=101:
            flag_for_pagination = True
        i = 0
        data = {
            'ruc': None,
            'nombreRazon': None,
            'Municipio': None,
            'Sector': None,
            'Estado': None,
            'tipoId': None,
            'tipoPersonaJuridicaId': None,
            'razonSocialId': None,
            'nombreComercialId': None,
            'estadoId': None,
            'categoriaId': None,
            'clasificacionId': None,
            'actividadEconId': None,
            'cantTrabajadoresId': None,
            'sexo': None
        }
        file_path = Path(f"data/Proveedores.csv")
        table_data = soup.find(id="mostrarResultadoView:listadoProvForm:listadoProveedores:tbody_element")
        for row in table_data:
            row1 = "mostrarResultadoView:listadoProvForm:listadoProveedores_"+str(i)
            print(f"\nInside table rows----------> Row count: {row1}")

            print(f"\nAdding content")
            ruc = soup.find(id=row1+":rucColV")
            if ruc == None:
                print(f"\n Row number: {i}: End of content --> Move to next page")
                break
            data["ruc"] = ruc.string
            nombreRazon = soup.find(id=row1+":nombreRazonV").string
            data["nombreRazon"] = nombreRazon
            Municipio = soup.find(id=row1+":tipoProvV").string
            data["Municipio"] = Municipio
            print(f"\ncheck --After Municipio")
            Sector = soup.find(id=row1+":tipoProveV").string
            data["Sector"] = Sector
            print(f"\ncheck --After Sector")
            estado = soup.find(id=row1+":estadoV").string
            data["Estado"] = estado
            print(f"\ncheck --After Estado")

            print(f"\nInto Datos for {i}")

            print(f'\nOrder data: {datos[i]}\n')
            data_updated = process_datos(datos[i],data)
            print(f'Result for {i}: {data_updated}')
            final_df = pd.DataFrame([data_updated])

            if file_path.exists():
                print("APPENDING TO FILE")
                final_df.to_csv(file_path, header=False, mode='a', index=False)
            else:
                print("MAKING NEW FILE")
                final_df.to_csv(file_path, header=True, mode='w', index=False)
            i=i+1

    except Exception as e:
        print(f"Error in process_resultpage: {e}")
    return flag_for_pagination


#---------------Process Datos------------------------
def process_datos(datos,data):
    print("\nInside Process datos method....")
    new_data = None
    try:
        soup = BeautifulSoup(datos.text, 'html.parser')
        if soup.find(id="proveedorPersonaJuridicaView:title") != None:
            print("\nDatos data available....")
            new_data = process_jurisdicado_data(soup,data)
        elif soup.find(id="proveedorPersonaNaturalView:title") != None:
            print("\nDatos data available....")
            new_data = process_natural_data(soup,data)
        else:
            print("\nDatos data not available....")
            return None

    except Exception as e:
        print(f"Error in process_datos: {e}")

    print("\nEnd of Process Datos method......")
    return new_data

#-----------------------------
def if_exists(soup,soup_id):
    try:
        if soup.find(id=soup_id) != None:
            return soup.find(id=soup_id).string
        else:
            return " "
    except Exception as e:
            print(f"Error in if_exists: {e}")

#--------------------------------------
def process_resultpage_next_page(x, resp, datos):
    flag_for_pagination = False
    try:
        #session = session1
        soup = BeautifulSoup(resp.text, 'html.parser')
        result_title = soup.find(id="mostrarResultadoView:title").string
        total_results = soup.find(id="mostrarResultadoView:listadoProvForm:Pluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id13").string
        number_of_results = re.findall(r'\d+,\d+', total_results)
        print(f"\n Total Results for {result_title}: {total_results} - Total = {number_of_results}")
        #send the flag back in return
        if int(number_of_results[0].replace(',', '')) >=101:
            flag_for_pagination = True
        i = 20*(x-1)
        data = {
            'ruc': None,
            'nombreRazon': None,
            'Municipio': None,
            'Sector': None,
            'Estado': None,
            'tipoId': None,
            'tipoPersonaJuridicaId': None,
            'razonSocialId': None,
            'nombreComercialId': None,
            'estadoId': None,
            'categoriaId': None,
            'clasificacionId': None,
            'actividadEconId': None,
            'cantTrabajadoresId': None,
            'sexo': None
        }
        file_path = Path(f"data/Proveedores.csv")
        table_data = soup.find(id="mostrarResultadoView:listadoProvForm:listadoProveedores:tbody_element")
        x=0
        for row in table_data:
            row1 = "mostrarResultadoView:listadoProvForm:listadoProveedores_"+str(i)
            print(f"\nInside table rows----------> Row count: {row1}")

            print(f"\nAdding content")
            ruc = soup.find(id=row1+":rucColV")
            if ruc == None:
                print(f"\n Row number: {i}: End of content --> Move to next page")
                break
            data["ruc"] = ruc.string
            nombreRazon = soup.find(id=row1+":nombreRazonV").string
            data["nombreRazon"] = nombreRazon
            Municipio = soup.find(id=row1+":tipoProvV").string
            data["Municipio"] = Municipio
            print(f"\ncheck --After Municipio")
            Sector = soup.find(id=row1+":tipoProveV").string
            data["Sector"] = Sector
            print(f"\ncheck --After Sector")
            estado = soup.find(id=row1+":estadoV").string
            data["Estado"] = estado
            print(f"\ncheck --After Estado")

            print(f"\nInto Datos for {x}")

            print(f'\nOrder data: {datos[x]}\n')
            data_updated = process_datos(datos[x],data)
            print(f'Result for {i}: {data}')
            final_df = pd.DataFrame([data])

            if file_path.exists():
                print("APPENDING TO FILE")
                final_df.to_csv(file_path, header=False, mode='a', index=False)
            else:
                print("MAKING NEW FILE")
                final_df.to_csv(file_path, header=True, mode='w', index=False)
            i=i+1
            x=x+1

    except Exception as e:
        print(f"Error in process_resultpage_next_page: {e}")
    return flag_for_pagination

#---------------Process Datos------------------------
def process_jurisdicado_data(soup,data):
    print("\nInside process_jurisdicado_data method....")
    print(f"\nAdding tipoId")
    tipoId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:tipoId")
    data["tipoId"] = tipoId
    print(f"\nAdding sexoId")
    sexoId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:sexoId")
    data["sexoId"] = sexoId
    print(f"\nAdding tipoPersonaJuridicaId")
    tipoPersonaJuridicaId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:tipoPersonaJuridicaId")
    data["tipoPersonaJuridicaId"] = tipoPersonaJuridicaId
    print(f"\nAdding datosPersonaNaturalSubView")
    datosPersonaNaturalSubView = soup.find(id="proveedorPersonaNaturalView:datosPersonaNaturalSubView:Pluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id8")
    data["datosPersonaNaturalSubView"] = datosPersonaNaturalSubView.get_text() if datosPersonaNaturalSubView!= None else None

    print(f"\nAdding razonSocialId")
    razonSocialId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:razonSocialId")
    data["razonSocialId"] = razonSocialId
    print(f"\nAdding nombreComercialId")
    nombreComercialId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:nombreComercialId")
    data["nombreComercialId"] = nombreComercialId
    print(f"\nAdding estadoId")
    estadoId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:datosPorEstadoProveedorView:estadoId")
    data["estadoId"] = estadoId
    print(f"\nAdding categoriaId")
    categoriaId = soup.find(id="proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:datosActividadProveedorView:categoriaId")
    data["categoriaId"] = categoriaId.get_text() if categoriaId!=None else None
    print(f"\nAdding clasificacionId")
    clasificacionId = soup.find(id="proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:datosActividadProveedorView:clasificacionId")
    data["clasificacionId"] = clasificacionId.get_text() if clasificacionId!=None else None

    print(f"\nAdding actividadEconId")
    actividadEconId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:datosActividadProveedorView:actividadEconId_0:Pluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id67")
    data["actividadEconId"] = actividadEconId
    print(f"\nAdding cantTrabajadoresId")
    cantTrabajadoresId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:datosActividadProveedorView:cantTrabajadoresId")
    data["cantTrabajadoresId"] = cantTrabajadoresId
    print(f"\nAdding actisexoId")
    actisexoId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:datosActividadProveedorView:sexoId")
    data["sexo"] = actisexoId

    print(f'\n===============Jurisdicado Appended!===================')
    return data

#---------------Process Datos------------------------
def process_natural_data(soup,data):
    print("\nInside process_natural_data method....")
    print(f"\nAdding tipoId")
    tipoId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:tipoId")
    data["tipoId"] = tipoId
    print(f"\nAdding sexoId")
    sexoId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:sexoId")
    data["sexoId"] = sexoId
    print(f"\nAdding tipoPersonaJuridicaId")
    data["tipoPersonaJuridicaId"] = None
    print(f"\nAdding datosPersonaNaturalSubView")
    datosPersonaNaturalSubView = soup.find(id="proveedorPersonaNaturalView:datosPersonaNaturalSubView:Pluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id8")
    data["datosPersonaNaturalSubView"] = datosPersonaNaturalSubView.get_text() if datosPersonaNaturalSubView!=None else None

    print(f"\nAdding razonSocialId")
    razonSocialId = if_exists(soup,"proveedorPersonaJuridicaView:datosPersonaJuridicaSubView:razonSocialId")
    data["razonSocialId"] = razonSocialId
    print(f"\nAdding nombreComercialId")
    nombreComercialId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:nombreComercialId")
    data["nombreComercialId"] = nombreComercialId
    print(f"\nAdding estadoId")
    estadoId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:datosPorEstadoProveedorView:estadoId")
    data["estadoId"] = estadoId
    print(f"\nAdding categoriaId")
    categoriaId_0 = soup.find(id="proveedorPersonaNaturalView:datosPersonaNaturalSubView:datosActividadProveedorView:categoriaId")
    data["categoriaId"] = categoriaId_0.get_text() if categoriaId_0!=None else None
    print(f"\nAdding clasificacionId")
    clasificacionId = soup.find(id="proveedorPersonaNaturalView:datosPersonaNaturalSubView:datosActividadProveedorView:clasificacionId")
    data["clasificacionId"] = clasificacionId.get_text() if clasificacionId!=None else None


    print(f"\nAdding actividadEconId")
    actividadEconId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:datosActividadProveedorView:actividadEconId_0:Pluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id65")
    data["actividadEconId"] = actividadEconId
    print(f"\nAdding cantTrabajadoresId")
    cantTrabajadoresId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:datosActividadProveedorView:cantTrabajadoresId")
    data["cantTrabajadoresId"] = cantTrabajadoresId
    print(f"\nAdding actisexoId")
    actisexoId = if_exists(soup,"proveedorPersonaNaturalView:datosPersonaNaturalSubView:datosActividadProveedorView:sexoId")
    data["sexo"] = actisexoId

    print(f'\n===============Natural Appended!===================')
    return data
