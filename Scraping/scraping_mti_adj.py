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
        result_title = soup.find(id="resultadoView:busqProcedimientoForm:consultaId").string
        total_results = soup.find(id="resultadoView:busqProcedimientoForm:Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id6").string
        number_of_results = re.findall(r'\d+\d+', total_results)
        print(f"\n Total Results for {result_title}: {total_results} - Total = {number_of_results}")
        year = result_title.split("+")[0]
        cse = result_title.split("+")[1]
        #send the flag back in return
        if int(number_of_results[0].replace(',', '')) >=101:
            flag_for_pagination = True
        table = soup.find(id="resultadoView:listadoProcedimientosForm:listadoProcedimientos:tbody_element")
        i = 0
        data = {
            'PageNo': i,
            'P_contract': None, 
            'P_no': None,
            'M_Estado': None,
            'M_SIGAF': None,
            'M_Publicacion': None,
            'M_Cierre': None,
            'M_Ultima': None,
            'M_Body1': None,
            'M_Body2': None,
            'M_Body3': None,
            'M_Datos': None
        }
        file_path = Path(f"data/ADJ_{year}_{cse}.csv")
        for row in table:
            row1 = "resultadoView:listadoProcedimientosForm:listadoProcedimientos_"+str(i)
            print(f"\nInside table rows----------> Row count: {row1}")

            #Procedimentos
            print(f"\nAdding P_xxx content")
            p_contract = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id19")
            if p_contract == None:
                print(f"\n Row number: {i}: End of content --> Move to next page")
                break
            data["P_contract"] = p_contract.string
            p_no = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id20").string
            data["P_no"] = p_no

            #Body
            print(f"\nAdding m_xxx content")
            m_estado = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id24_0:estadoV").string
            data["M_Estado"] = m_estado
            print(f"\ncheck --After Estado")
            m_SIGAF = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id24_0:sigafV").string
            data["M_SIGAF"] = m_SIGAF
            print(f"\ncheck --After SIGAF")
            m_publicacion = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id24_0:publicacionV").string
            data["M_Publicacion"] = m_publicacion
            print(f"\ncheck --After publicacion")
            m_cierre = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id24_0:cierreVigenteV").string
            data["M_Cierre"] = m_cierre
            print(f"\ncheck --After cierre")
            m_ultima = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id24_0:ultimaActV1")
            if m_ultima == None:
                m_ultima = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id24_0:ultimaActV2")
            data["M_Ultima"] = m_ultima.string if m_ultima != None else None
            print(f"\ncheck body1")
            m_body1 = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id36").string
            data["M_Body1"] = m_body1
            print(f"\ncheck body2")
            m_body2 = soup.find(id=row1+":clasificacionesList_0:Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id37").string
            data["M_Body2"] = m_body2
            print(f"\ncheck body3")
            m_body3 = soup.find(id=row1+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id39").string
            data["M_Body3"] = m_body3

            print(f'\nOrder data: {datos[i]}\n')
            data_updated = process_datos(datos[i],data)
            print(f'Result for {i}: {data}')
            final_df = pd.DataFrame([data])

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
    try:
        soup = BeautifulSoup(datos.text, 'html.parser')
        if soup.find(id="datosProcedimiento:datosCabeceraView:entidadId") != None:
            print("\nOrder data available....")
        else:
            print("\nOrder data not available....")
        print(f"\nAdding d_entidadId")
        d_entidadId = if_exists(soup,"datosProcedimiento:datosCabeceraView:entidadId")
        data["D_EntidadId"] = d_entidadId
        print(f"\nAdding d_unidadId")
        d_unidadId = if_exists(soup,"datosProcedimiento:datosCabeceraView:unidadId")
        data["D_UnidadId"] = d_unidadId
        print(f"\nAdding d_clasificacionId")
        d_clasificacionId = if_exists(soup,"datosProcedimiento:datosCabeceraView:clasificacionId_0:Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id13")
        data["D_ClasificacionId"] = d_clasificacionId
        print(f"\nAdding d_categoriaId")
        d_categoriaId = if_exists(soup,"datosProcedimiento:datosCabeceraView:categoriaId")
        data["D_CategoriaId"] = d_categoriaId
        print(f"\nAdding d_fuenteDeFinanciamientoI")
        d_fuenteDeFinanciamientoI = if_exists(soup,"datosProcedimiento:datosCabeceraView:fuenteDeFinanciamientoId")
        data["D_FuenteDeFinanciamientoI"] = d_fuenteDeFinanciamientoI
        print(f"\nAdding d_normaAplicableId")
        d_normaAplicableId = if_exists(soup,"datosProcedimiento:datosCabeceraView:normaAplicableId")
        data["D_NormaAplicableId"] = d_normaAplicableId
        print(f"\nAdding d_tipoProcedimientoId")
        d_tipoProcedimientoId = if_exists(soup,"datosProcedimiento:datosCabeceraView:tipoProcedimientoId")
        data["D_TipoProcedimientoId"] = d_tipoProcedimientoId
        print(f"\nAdding d_modalidadId")
        d_modalidadId = if_exists(soup,"datosProcedimiento:datosCabeceraView:modalidadId")
        data["D_ModalidadId"] = d_modalidadId
        print(f"\nAdding d_estudiPBCId")
        d_estudiPBCId = if_exists(soup,"datosProcedimiento:datosCabeceraView:estudiPBCId")
        data["D_EstudiPBCId"] = d_estudiPBCId
        print(f"\nAdding d_vinculacionId")
        d_vinculacionId = if_exists(soup,"datosProcedimiento:datosCabeceraView:vinculaciongrpId")
        data["D_VinculacionId"] = d_vinculacionId
        print(f"\nAdding d_estado")
        d_estado = if_exists(soup,"datosProcedimiento:datosCabeceraView:estadoId")
        data["D_Estado"] = d_estado

        print(f'\n-----------Processing ADJ section------------')
        order_section = soup.find(id="datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones")
        if order_section != None:
            print(f'\Adj section found!')
        number_of_orders = len(order_section.find_all("tr"))
        print(f'\nNumber of orders: {number_of_orders}')
        if number_of_orders >=1:
            for x in range(0,9):
                if soup.find(id="datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorNombreColV") != None or soup.find(id="datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorRUCColV") != None:
                    d_proveedorNombre = if_exists(soup,"datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorNombreColV")
                    data["D_ADJ_ProveedorNombre_"+str(x)] = d_proveedorNombre
                    if soup.find(id="datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorRUCColV2") != None:
                        d_proveedorRUCColV = if_exists(soup,"datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorRUCColV2")
                        data["D_ADJ_ProveedorRUCColV_"+str(x)] = d_proveedorRUCColV
                    elif soup.find(id="datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorRUCColV1") != None:
                        d_proveedorRUCColV = if_exists(soup,"datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorRUCColV1")
                        data["D_ADJ_ProveedorRUCColV_"+str(x)] = d_proveedorRUCColV
                    else:
                        d_proveedorRUCColV = if_exists(soup,"datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorRUCColV")
                        data["D_ADJ_ProveedorRUCColV_"+str(x)] = d_proveedorRUCColV
                    d_monto = if_exists(soup,"datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":montoEstimadoColV")
                    data["D_ADJ_Monto_"+str(x)] = d_monto
                    d_renglonesColV = if_exists(soup,"datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":renglonesColV")
                    data["D_ADJ_RenglonesColV_"+str(x)] = d_renglonesColV
                    d_beneficiario = if_exists(soup,"datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id81_0:Pluto__adquisiciones_gestion_portlet_busquedaProcedimientoPortlet__id83")
                    data["D_ADJ_BeneficiarioFinal_"+str(x)] = d_beneficiario
                elif soup.find(id="datosProcedimiento:datosAdjudicacionesDGCEView:listadoAdjudicaciones_"+str(x)+":proveedorNombreColV") == None:
                    data["D_ADJ_ProveedorNombre_"+str(x)] = None
                    data["D_ADJ_ProveedorRUCColV_"+str(x)] = None
                    data["D_ADJ_Monto_"+str(x)] = None
                    data["D_ADJ_RenglonesColV_"+str(x)] = None
                    data["D_ADJ_BeneficiarioFinal_"+str(x)] = None
                
    except Exception as e:
        print(f"Error in process_datos: {e}")

    print("\nEnd of Process Datos method......")
    return data

#-----------------------------
def if_exists(soup,soup_id):
    try:
        if soup.find(id=soup_id) != None:
            return soup.find(id=soup_id).string
        else:
            return " "
    except Exception as e:
            print(f"Error in if_exists: {e}")
