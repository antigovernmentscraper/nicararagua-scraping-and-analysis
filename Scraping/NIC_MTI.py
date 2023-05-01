#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  24 18:34:15 2022

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
import scraping_mti
from urllib.parse import urljoin

from http.client import HTTPConnection
HTTPConnection._http_vsn_str = "HTTP/1.0"

'''
2007=2, 2008=3, 2009=4, 2010=5
2011=6,2012=7,2013=8,2014=9,2015=10,2016=11,2017=12,2018=13,2019=14,2020=15,2021=16,2022=17
'''
year_list = ['6','7','8','9','10','11','12','13','14','15','16','17']
ministry_list = ['81','9']

'''
url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__ac0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/busquedaSimpleView.jsp&inicioBusqForm%3AordenItems=PorFechaCierre&inicioBusqForm%3AresultadosItems=CIEN&inicioBusqForm%3AnombreAdqId=&inicioBusqForm%3AnumeroAdqId=&inicioBusqForm%3AnumeroAdqGrpId=&inicioBusqForm%3AejercicioId=16&inicioBusqForm%3AinstitucionId=9&inicioBusqForm%3AcalificacionValueId=-1&inicioBusqForm%3AprocedimientosId=-1&inicioBusqForm%3AdepartamentoId=-1&inicioBusqForm%3AestadoAdqPuId=VIGENTE&inicioBusqForm%3AestadoAdqPuId=EJECUCION&inicioBusqForm%3AestadoAdqPuId=CERRADO&inicioBusqForm%3AestadoAdqPuId=ADJUDICADO&inicioBusqForm%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id60=Buscar&inicioBusqForm_SUBMIT=1&autoScroll=0%2C431&inicioBusqForm%3A_link_hidden_='
'''

trialurl = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__ac0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/busquedaSimpleView.jsp&inicioBusqForm%3AordenItems=PorFechaCierre&inicioBusqForm%3AresultadosItems=CIEN&inicioBusqForm%3AnombreAdqId=&inicioBusqForm%3AnumeroAdqId=&inicioBusqForm%3AnumeroAdqGrpId=&inicioBusqForm%3AejercicioId=16&inicioBusqForm%3AinstitucionId=9&inicioBusqForm%3AcalificacionValueId=-1&inicioBusqForm%3AprocedimientosId=-1&inicioBusqForm%3AdepartamentoId=-1&inicioBusqForm%3AestadoAdqPuId=VIGENTE&inicioBusqForm%3AestadoAdqPuId=EJECUCION&inicioBusqForm%3AestadoAdqPuId=CERRADO&inicioBusqForm%3AestadoAdqPuId=ADJUDICADO&inicioBusqForm%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id60=Buscar&inicioBusqForm_SUBMIT=1&autoScroll=0%2C431&resultadoView%3AlistadoProcedimientosForm%3A_link_hidden_=resultadoView%3AlistadoProcedimientosForm%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id92_1%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id93'

get_url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__ac0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp'
refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda'
todos_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda?accion=todos&usr_ua_id=todos'

next_page_url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__ac0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/busquedaSimpleView.jsp&inicioBusqForm%3AordenItems=PorFechaCierre&inicioBusqForm%3AresultadosItems=CIEN&inicioBusqForm%3AnombreAdqId=&inicioBusqForm%3AnumeroAdqId=&inicioBusqForm%3AnumeroAdqGrpId=&inicioBusqForm%3AejercicioId=16&inicioBusqForm%3AinstitucionId=9&inicioBusqForm%3AcalificacionValueId=-1&inicioBusqForm%3AprocedimientosId=-1&inicioBusqForm%3AdepartamentoId=-1&inicioBusqForm%3AestadoAdqPuId=VIGENTE&inicioBusqForm%3AestadoAdqPuId=EJECUCION&inicioBusqForm%3AestadoAdqPuId=CERRADO&inicioBusqForm%3AestadoAdqPuId=ADJUDICADO&inicioBusqForm%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id60=Buscar&inicioBusqForm_SUBMIT=1&autoScroll=0%2C431&resultadoView%3AlistadoProcedimientosForm%3A_link_hidden_=resultadoView%3AlistadoProcedimientosForm%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id92_'
next_page_last = '%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id93'
next_page_refer = "https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__rp0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID/0x3fragments0x3busqueda0x3resultadoBusqueda0x2jsp?"

second_page_url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__ac0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp&resultadoView%3AlistadoProcedimientosForm%3AordenItems=PorFechaCierre&resultadoView%3AlistadoProcedimientosForm%3AresultadosItems=CIEN&resultadoView%3AlistadoProcedimientosForm_SUBMIT=1&autoScroll=0%2C10012&resultadoView%3AlistadoProcedimientosForm%3A_link_hidden_=resultadoView%3AlistadoProcedimientosForm%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id92_2%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id93'
second_page_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__rp0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp?'


mes_datos = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__ac0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp&resultadoView%3AlistadoProcedimientosForm%3AordenItems=PorFechaCierre&resultadoView%3AlistadoProcedimientosForm%3AresultadosItems=CIEN&resultadoView%3AlistadoProcedimientosForm_SUBMIT=1&autoScroll=0%2C0&resultadoView%3AlistadoProcedimientosForm%3A_link_hidden_=resultadoView%3AlistadoProcedimientosForm%3AlistadoProcedimientos_'
last_datos = '%3AverDatosLink'
datos_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__rp0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/resultadoBusqueda.jsp?'

order_url = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__ac0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet?&org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/datos/datosProcedimientoPublicoView.jsp&datosProcedimiento%3APluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id25=Orden+de+Compra&datosProcedimiento_SUBMIT=1&autoScroll=0%2C130&datosProcedimiento%3A_link_hidden_='
order_refer = 'https://www.gestion.nicaraguacompra.gob.ni/siscae/portal/adquisiciones-gestion/busqueda/__rp0x3adquisiciones-gestion-portlet0x2busquedaProcedimientoPortlet_org.apache.myfaces.portlet.MyFacesGenericPortlet.VIEW_ID=/fragments/busqueda/datos/datosProcedimientoPublicoView.jsp?'

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
        while soup.find(id="resultadoView:busqProcedimientoForm:consultaId") == None:
            print(f"\n{i} : Running loop")
            time.sleep(2)
            response = session.get(url, headers={'Referer': refer})
            soup = BeautifulSoup(response.text, 'html.parser')
            i=i+1
        result_text = soup.find(id="resultadoView:busqProcedimientoForm:consultaId").string
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
            if i ==3:
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
        while soup.find(id="Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id5") == None:
            print(f"\n{i} : Running loop")
            time.sleep(2)
            response = session.get(url, headers={'Referer': refer})
            soup = BeautifulSoup(response.text, 'html.parser')
            i=i+1
        result_text = soup.find(id="Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id5").string
        print(f"\nFound: {result_text}")
        if result_text != None:
            order_result = get_orders(order_url,order_refer)
        if order_result==None:
            return response
    except Exception as e:
        print(f"Error in get_data: {e}")
    return order_result

#-----------------Method: gets the scraper to process results------------------------------------
def get_scraper(results,datos):
    scraped_data = None
    try:
        #fetching scraper method from scraping.py
        scraped_data = scraping_mti.process_resultpage(results,datos)
    except Exception as e:
        print(f"Error in get_scraper: {e}")
    return scraped_data

# ----------------Main Method: Initiates code-----------------------------------------------------

results = get_data(trialurl, refer)

next_page = 1
if results != None:
    next_page_url_1 = next_page_url+str(next_page)+next_page_last
    print(f"\nNext Page URL: {next_page_url_1}")
    next_page_result_1 = get_data(next_page_url_1, refer)
    next_page = next_page+1
soup = BeautifulSoup(next_page_result_1.text, 'html.parser')
result_title = soup.find(id="resultadoView:busqProcedimientoForm:consultaId").string
total_results = soup.find(id="resultadoView:busqProcedimientoForm:Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id6").string
number_of_results = re.findall(r'\d+\d+', total_results)
print(f"\n Total Results for {result_title}: {total_results} - Total = {number_of_results}")
range_number = int(number_of_results[0]) if int(number_of_results[0])<=101 else 100
print(f'\nNumber of pages for Datos: {range_number}')

datos_result = []
dato =0
for x in range(0,range_number):
    print(f'\n Getting datos for {x}')
    datos_url = mes_datos+str(x)+last_datos
    results = get_datos(datos_url, datos_refer)
    datos_result.append(results)
    print(f'\n=======================================\nAppended to List: {len(datos_result)}\n=======================================')

result_scraped = get_scraper(next_page_result_1, datos_result)

second_page_exists = False
if int(number_of_results[0].replace(',', '')) >=101:
    print(f"\nSecond Page URL: {second_page_url}")
    second_page_result = get_data(second_page_url, second_page_refer)
    soup = BeautifulSoup(second_page_result.text, 'html.parser')
    total_results = soup.find(id="resultadoView:listadoProcedimientosForm:listadoProcedimientos:tbody_element")
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
        print(f'\n Getting datos for {x}')
        datos_url = mes_datos+str(x)+last_datos
        results = get_datos(datos_url, datos_refer)
        datos_result.append(results)
        print(f'\n=======================================\nAppended to List: {len(datos_result)}\n=======================================')

    result_scraped = get_scraper(second_page_result, datos_result)

print(f"\nResult from scraped_data method: Flag for pagination: {result_scraped}")

print(f"Scraping is done! {datetime.now() - startTime} to complete")
