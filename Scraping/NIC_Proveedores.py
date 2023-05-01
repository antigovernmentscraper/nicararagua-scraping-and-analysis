#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  24 18:34:15 2022

Purpose: File to scrape and datamatch for 'proveedores' section
"""

import requests
from bs4 import BeautifulSoup
#import concurrent.futures
import pandas as pd, pandas.errors
from requests.exceptions import MissingSchema, SSLError, ContentDecodingError, ConnectionError, Timeout, ChunkedEncodingError, InvalidURL, TooManyRedirects
from urllib3.exceptions import LocationParseError
#import search_functions
#import categorization
import re
import time
#import form_extractor
from datetime import datetime
from urllib.parse import urljoin
import proveedores_erdmatch

from http.client import HTTPConnection
HTTPConnection._http_vsn_str = "HTTP/1.0"

'''
2007=2, 2008=3, 2009=4, 2010=5
2011=6,2012=7,2013=8,2014=9,2015=10,2016=11,2017=12,2018=13,2019=14,2020=15,2021=16,2022=17
'''
year_list = ['6','7','8','9','10','11','12','13','14','15','16','17']
ministry_list = ['81','9']

trialurl = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__ac0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/busquedaView.jsp&inicioRegForm%3AnombreRazonId=&inicioRegForm%3ArucId=&inicioRegForm%3ApersonaId=-1&inicioRegForm%3AclasificacionEmpty=&inicioRegForm%3AestadoProveedorPublicaId=-1&inicioRegForm%3APluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id23=Buscar&inicioRegForm_SUBMIT=1&autoScroll=0%2C255&inicioRegForm%3A_link_hidden_='

refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda?accion=init'

todos_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda?accion=todos&usr_ua_id=todos'

next_page_url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__ac0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp&mostrarResultadoView%3AlistadoProvForm%3AordenItems=NOMBRE_RAZON&mostrarResultadoView%3AlistadoProvForm%3AascendenteId=true&mostrarResultadoView%3AlistadoProvForm_SUBMIT=1&autoScroll=0%2C928&mostrarResultadoView%3AlistadoProvForm%3Apaginador=idx'
next_page_last = '&mostrarResultadoView%3AlistadoProvForm%3A_link_hidden_=mostrarResultadoView%3AlistadoProvForm%3Apaginadoridx'
next_page_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__rp0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID/0x3fragments0x3busqueda0x3resultadoBusqueda0x2jsp?'

second_page_url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__ac0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp&mostrarResultadoView%3AlistadoProvForm%3AordenItems=NOMBRE_RAZON&mostrarResultadoView%3AlistadoProvForm%3AascendenteId=true&mostrarResultadoView%3AlistadoProvForm_SUBMIT=1&autoScroll=0%2C871&mostrarResultadoView%3AlistadoProvForm%3Apaginador=idx'
second_page_last = '&mostrarResultadoView%3AlistadoProvForm%3A_link_hidden_=mostrarResultadoView%3AlistadoProvForm%3Apaginadoridx'
second_page_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__rp0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp?'


mes_datos = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__ac0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp&mostrarResultadoView%3AlistadoProvForm%3AordenItems=NOMBRE_RAZON&mostrarResultadoView%3AlistadoProvForm%3AascendenteId=true&mostrarResultadoView%3AlistadoProvForm_SUBMIT=1&autoScroll=0%2C11&mostrarResultadoView%3AlistadoProvForm%3Apaginador=&mostrarResultadoView%3AlistadoProvForm%3A_link_hidden_=mostrarResultadoView%3AlistadoProvForm%3AlistadoProveedores_'
last_datos = '%3APluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id70'
datos_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__rp0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp?'

second_datos_url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__ac0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp&mostrarResultadoView%3AlistadoProvForm%3AordenItems=NOMBRE_RAZON&mostrarResultadoView%3AlistadoProvForm%3AascendenteId=true&mostrarResultadoView%3AlistadoProvForm_SUBMIT=1&autoScroll=0%2C11&mostrarResultadoView%3AlistadoProvForm%3Apaginador=idx'
second_datos_middle = '&mostrarResultadoView%3AlistadoProvForm%3A_link_hidden_=mostrarResultadoView%3AlistadoProvForm%3AlistadoProveedores_'
second_datos_last= '%3APluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id70'
second_datos_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/proveedores-gestion/busqueda/__rp0x3proveedores-gestion-portlet0x2busquedaProveedoresPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/proveedor/personaNaturalView.jsp?'

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'}
startTime = datetime.now()
session = requests.Session()

#-----------------Method: gets the result page with API call-------------------------------------
def get_data(url,refer):
    response = None
    try:
        #session = requests.Session()
        response = session.get(url, headers={'Referer': refer})
        print("\n\n\n=============================================================")
        soup = BeautifulSoup(response.text, 'html.parser')
        html = soup.prettify()
        text = soup.getText()
        i=0
        while soup.find(id="mostrarResultadoView:title") == None:
            print(f"\n{i} : Running loop")
            time.sleep(2)
            response = session.get(url, headers={'Referer': refer})
            soup = BeautifulSoup(response.text, 'html.parser')
            i=i+1
        result_text = soup.find(id="mostrarResultadoView:title").string
        print(f"\n Found: {result_text}")

    except Exception as e:
        print(f"Error in get_data: {e}")
    return response

#-----------------Method: gets the result page with API call-------------------------------------
def get_orders(url,refer):
    response = None
    try:
        #session = requests.Session()
        response = session.get(url, headers={'Referer': refer})
        soup = BeautifulSoup(response.text, 'html.parser')
        html = soup.prettify()
        text = soup.getText()
        i=0
        while soup.find(id="datosProcedimiento:datosOrdenesCompraView:datosOrdenesCompraId") == None:
            print(f"\n{i} : Running loop")
            time.sleep(2)
            response = session.get(url, headers={'Referer': refer})
            soup = BeautifulSoup(response.text, 'html.parser')
            i=i+1
            if i ==5:
                break
        if soup.find(id="datosProcedimiento:datosOrdenesCompraView:datosOrdenesCompraId") != None:
            result_text = soup.find(id="datosProcedimiento:datosOrdenesCompraView:datosOrdenesCompraId").string
            print(f"\nFound: {result_text}")
            return response
        else:
            print(f"\nNOTFound: ORDERS NOT AVAILABLE")

    except Exception as e:
        print(f"Error in get_data: {e}")
    return None

#-----------------Method: gets the result page with API call-------------------------------------
def get_datos(url,refer):
    response = None
    try:
        #session = requests.Session()
        response = session.get(url, headers={'Referer': refer})
        print("\n\n______________________________________________________________________________________")
        soup = BeautifulSoup(response.text, 'html.parser')
        html = soup.prettify()
        text = soup.getText()
        i=0
        while soup.find(id="proveedorPersonaJuridicaView:title") == None and soup.find(id="proveedorPersonaNaturalView:title") == None:
            print(f"\n{i} : Running loop")
            time.sleep(2)
            response = session.get(url, headers={'Referer': refer})
            soup = BeautifulSoup(response.text, 'html.parser')
            i=i+1
            if i ==5:
                break
        if soup.find(id="proveedorPersonaJuridicaView:title") != None:
            result_text = soup.find(id="proveedorPersonaJuridicaView:title").string
        else:
            result_text = soup.find(id="proveedorPersonaNaturalView:title").string
        print(f"\nFound: {result_text}")

    except Exception as e:
        print(f"Error in get_datos: {e}")
    return response

#-----------------Method: gets the scraper to process results------------------------------------
def get_scraper(results,datos):
    scraped_data = None
    try:
        #fetching scraper method from scraping.py
        scraped_data = p1.process_resultpage(results,datos)
    except Exception as e:
        print(f"Error in get_scraper: {e}")
    return scraped_data
#-----------------Method: gets the scraper to process results------------------------------------
def get_scraper_next_page(x,results,datos):
    scraped_data = None
    try:
        #fetching scraper method from scraping.py
        scraped_data = p1.process_resultpage_next_page(x,results,datos)
    except Exception as e:
        print(f"Error in get_scraper: {e}")
    return scraped_data

# ----------------Main Method: Initiates code-----------------------------------------------------

results = get_data(trialurl, refer)

next_page = 1
if results != None:
    next_page_url_1 = next_page_url+str(next_page)+next_page_last+str(next_page)
    print(f"\nNext Page URL: {next_page_url_1}")
    next_page_result_1 = get_data(next_page_url_1, refer)
    next_page = next_page+1
soup = BeautifulSoup(next_page_result_1.text, 'html.parser')
result_title = soup.find(id="mostrarResultadoView:title").string
total_results = soup.find(id="mostrarResultadoView:listadoProvForm:Pluto__proveedores_gestion_portlet_busquedaProveedoresPortlet__id13").string
number_of_results = re.findall(r'\d+,\d+', total_results)
number_of_results[0]=number_of_results[0].replace(",","")
print(f"\n Total Results for {result_title}: {total_results} - Total = {number_of_results}")
range_number = int(number_of_results[0]) if int(number_of_results[0])<=101 else 100
print(f'\nNumber of pages for Datos: {range_number}')
page_results = soup.find(id="mostrarResultadoView:listadoProvForm:listadoProveedores:tbody_element")
count = 0
for j in page_results.find_all(recursive=False):
    count = count+1
number_of_results = count
print(f"\n Total Results for {next_page} Page- Total = {number_of_results}")
datos_result = []
dato =0

for x in range(0,number_of_results):
    print(f'\n Getting datos for {x}')
    datos_url = mes_datos+str(x)+last_datos
    results = get_datos(datos_url, datos_refer)
    datos_result.append(results)
    print(f'\n=======================================\nAppended to List: {len(datos_result)}\n=======================================')

result_scraped = get_scraper(next_page_result_1, datos_result)

'''
for i in range(2,2935):
    print(f'Processing for page: {i}')
    second_page_url_updated = second_page_url+str(i)+second_page_last+str(i)
    print(f"\nNext Page URL: {second_page_url_updated}")
    second_page_result = get_data(second_page_url_updated, second_page_refer)
    soup = BeautifulSoup(second_page_result.text, 'html.parser')
    result_title = soup.find(id="mostrarResultadoView:title").string
    datos_result = []
    page_results = soup.find(id="mostrarResultadoView:listadoProvForm:listadoProveedores:tbody_element")
    count = 0
    for j in page_results.find_all(recursive=False):
        count = count+1
        number_of_results = count
    print(f"\n Total Results for {i} Page- Total = {number_of_results}")
    for x in range(0,number_of_results):
        dato_count = i*10+x
        count = dato_count
        print(f'\n Getting datos for {count}')
        time.sleep(5)
        second_page_result = get_data(second_page_url_updated, second_page_refer)
        second_page_result = get_data(second_page_url_updated, second_page_refer)
        second_datos_url_updated = second_datos_url+str(count)+second_datos_last
        print(f'\nSecond URL: {second_datos_url_updated}')
        results = get_datos(second_datos_url_updated, second_datos_refer)
        datos_result.append(results)
        count = count+1
        print(f'\n=======================================\nAppended to List: {len(datos_result)}\n=======================================')

    result_scraped = get_scraper(second_page_result, datos_result)
'''

second_page_exists = False
i = 2
if range_number< 101:
    second_page_url_updated = second_page_url+str(i)+second_page_last+str(i)
    print(f"\nNext Page URL: {second_page_url_updated}")
    second_page_result = get_data(second_page_url_updated, second_page_refer)
    soup = BeautifulSoup(second_page_result.text, 'html.parser')
    total_results = soup.find(id="mostrarResultadoView:listadoProvForm:listadoProveedores:tbody_element")
    count = 0
    for j in total_results.find_all(recursive=False):
        count = count+1
    number_of_results = count
    print(f"\n Total Results for Second Page- Total = {number_of_results}")
    second_page_exists = True

datos_result = []
if second_page_exists:
    dato =0
    for x in range(0,number_of_results):
        dato_count = i*10+x
        print(f'\n Getting datos for {x}')
        text_found = soup.find(id="mostrarResultadoView:listadoProvForm:listadoProveedores_"+str(dato_count)+":nombreRazonV").string
        print(f'\nText_found: {text_found}')
        second_datos_url_updated = second_datos_url+str(i)+second_datos_middle+str(dato_count)+second_datos_last
        print(f'\nSecond URL: {second_datos_url_updated}')
        time.sleep(2)
        results = get_datos(second_datos_url_updated, second_datos_refer)
        datos_result.insert(dato_count,results)
        print(f'\n=======================================\nAppended to List: {len(datos_result)}\n=======================================')

    result_scraped = get_scraper_next_page(i,second_page_result, datos_result)

print(f"\nResult from scraped_data method: Flag for pagination: {result_scraped}")

print(f"Scraping is done! {datetime.now() - startTime} to complete")
