import pandas as pd
import requests

def analizar_contratos_secop(
    departamento=None, 
    ciudad=None, 
    entidad=None, 
    busqueda=None,
    umbral_corbatas=2, 
    umbral_fraccionamiento=2, 
    umbral_valor=40000000
):
    try:
        url = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
        
        # 1. Configuración de la consulta base
        parametros = {
            "$limit": 5000,
            "$order": "fecha_de_firma DESC"
        }
        
        # Filtros dinámicos (Socrata es sensible a mayúsculas/minúsculas)
        if departamento: parametros["departamento"] = departamento.title()
        if ciudad: parametros["ciudad"] = ciudad.title() 
        if entidad: parametros["nombre_entidad"] = entidad.upper()
        if busqueda: parametros["$q"] = busqueda 

        # 2. Extracción de datos (Data Ingestion)
        respuesta = requests.get(url, params=parametros)
        
        if respuesta.status_code != 200:
            print(f"Error de API: {respuesta.status_code}")
            return []
            
        datos_json = respuesta.json()
        if not datos_json or isinstance(datos_json, dict): 
            return []
            
        df = pd.DataFrame(datos_json)

        # 3. Blindaje y Limpieza de Datos (Data Cleaning)
        columnas_requeridas = [
            'valor_del_contrato', 'modalidad_de_contratacion', 'documento_proveedor', 
            'tipo_de_contrato', 'id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 'fecha_de_firma'
        ]
        
        for col in columnas_requeridas:
            if col not in df.columns:
                df[col] = ''

        df['valor_del_contrato'] = pd.to_numeric(df['valor_del_contrato'], errors='coerce').fillna(0)
        df['documento_proveedor'] = df['documento_proveedor'].fillna('DESCONOCIDO')
        
        # Inicializamos el sistema de scoring
        df['riesgo_corrupcion'] = 0
        df['motivo_alerta'] = ''

        # =======================================================
        # 4. MOTOR DE RIESGO (Risk Engine)
        # =======================================================
        
        # A. Detección de "Corbatas" (Múltiples contratos de prestación)
        corbatas = df[df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)].groupby(
            'documento_proveedor'
        ).agg(num_contratos=('id_contrato', 'count')).reset_index()
        
        proveedores_corbatas = corbatas[corbatas['num_contratos'] >= umbral_corbatas]['documento_proveedor'].tolist()
        
        mascara_corbatas = df['documento_proveedor'].isin(proveedores_corbatas) & df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)
        df.loc[mascara_corbatas, 'riesgo_corrupcion'] += 40
        df.loc[mascara_corbatas, 'motivo_alerta'] += f'🚩 Corbata: >{umbral_corbatas - 1} Contrato(s) de Prestación. '

        # B. Detección de Fraccionamiento (Múltiples contratos directos misma entidad)
        fraccionamiento = df[df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)].groupby(
            ['nombre_entidad', 'documento_proveedor']
        ).agg(num_contratos_directos=('id_contrato', 'count')).reset_index()

        proveedores_fraccionados = fraccionamiento[fraccionamiento['num_contratos_directos'] >= umbral_fraccionamiento]['documento_proveedor'].tolist()

        mascara_fraccionamiento = df['documento_proveedor'].isin(proveedores_fraccionados) & df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)
        df.loc[mascara_fraccionamiento, 'riesgo_corrupcion'] += 40
        df.loc[mascara_fraccionamiento, 'motivo_alerta'] += f'🚩 Fraccionamiento: >{umbral_fraccionamiento - 1} Contrato(s) directos en misma entidad. '

        # C. Detección Tradicional (Contratación directa de alto valor)
        mascara_alto_valor = df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False) & (df['valor_del_contrato'] > umbral_valor)
        df.loc[mascara_alto_valor, 'riesgo_corrupcion'] += 30
        df.loc[mascara_alto_valor, 'motivo_alerta'] += f'🚩 Contrato Directo > {umbral_valor // 1000000} Millones. '

        # =======================================================
        # 5. PREPARACIÓN DE LA RESPUESTA
        # =======================================================
        # Filtramos solo los que tienen riesgo y ordenamos de mayor a menor gravedad
        df_alertas = df[df['riesgo_corrupcion'] >= 30].copy()
        df_alertas = df_alertas.sort_values(by='riesgo_corrupcion', ascending=False)

        columnas_clave = ['id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 'modalidad_de_contratacion', 'valor_del_contrato', 'riesgo_corrupcion', 'motivo_alerta']
        df_final = df_alertas[columnas_clave]
        
        # Reemplazamos los NaN de Pandas por None para que JSON los entienda como null
        df_final = df_final.where(pd.notnull(df_final), None)

        return df_final.to_dict(orient='records')
        
    except Exception as e:
        print(f"🔥 Error Crítico en Pandas salvado: {e}")
        return []