import pandas as pd
import requests

def analizar_contratos_secop(
    departamento=None, ciudad=None, entidad=None, busqueda=None,
    umbral_corbatas=2, umbral_fraccionamiento=2, umbral_valor=40000000
):
    try:
        url = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
        
        parametros = {
            "$limit": 5000,
            "$order": "fecha_de_firma DESC"
        }
        
        if departamento: parametros["departamento"] = departamento.title()
        if ciudad: parametros["ciudad"] = ciudad.title() 
        if entidad: parametros["nombre_entidad"] = entidad.upper()
        if busqueda: parametros["$q"] = busqueda 

        respuesta = requests.get(url, params=parametros)
        if respuesta.status_code != 200: return []
        datos_json = respuesta.json()
        if not datos_json or isinstance(datos_json, dict): return []
            
        df = pd.DataFrame(datos_json)

        # BLINDAJE DE COLUMNAS (Añadimos dias_adicionados para encontrar los retrasos)
        columnas_requeridas = [
            'valor_del_contrato', 'modalidad_de_contratacion', 'documento_proveedor', 
            'tipo_de_contrato', 'id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 
            'fecha_de_firma', 'dias_adicionados'
        ]
        
        for col in columnas_requeridas:
            if col not in df.columns: df[col] = ''

        df['valor_del_contrato'] = pd.to_numeric(df['valor_del_contrato'], errors='coerce').fillna(0)
        df['dias_adicionados'] = pd.to_numeric(df['dias_adicionados'], errors='coerce').fillna(0)
        df['documento_proveedor'] = df['documento_proveedor'].fillna('DESCONOCIDO')
        df['proveedor_adjudicado'] = df['proveedor_adjudicado'].fillna('')
        
        df['riesgo_corrupcion'] = 0
        df['motivo_alerta'] = ''

        # --- REGLA 1: CORBATAS ---
        corbatas = df[df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)].groupby('documento_proveedor').agg(num_contratos=('id_contrato', 'count')).reset_index()
        prov_corbatas = corbatas[corbatas['num_contratos'] >= umbral_corbatas]['documento_proveedor'].tolist()
        mask_corbatas = df['documento_proveedor'].isin(prov_corbatas) & df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)
        df.loc[mask_corbatas, 'riesgo_corrupcion'] += 40
        df.loc[mask_corbatas, 'motivo_alerta'] += f'🚩 Corbata: >{umbral_corbatas - 1} Contratos. '

        # --- REGLA 2: FRACCIONAMIENTO ---
        fracc = df[df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)].groupby(['nombre_entidad', 'documento_proveedor']).agg(num_contratos_directos=('id_contrato', 'count')).reset_index()
        prov_fracc = fracc[fracc['num_contratos_directos'] >= umbral_fraccionamiento]['documento_proveedor'].tolist()
        mask_fracc = df['documento_proveedor'].isin(prov_fracc) & df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)
        df.loc[mask_fracc, 'riesgo_corrupcion'] += 40
        df.loc[mask_fracc, 'motivo_alerta'] += f'🚩 Fraccionamiento. '

        # --- REGLA 3: DIRECTOS DE ALTO VALOR ---
        mask_valor = df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False) & (df['valor_del_contrato'] > umbral_valor)
        df.loc[mask_valor, 'riesgo_corrupcion'] += 30
        df.loc[mask_valor, 'motivo_alerta'] += f'🚩 Contrato Directo > {umbral_valor // 1000000}M. '

        # --- NUEVA REGLA 4: ADICIONES SOSPECHOSAS (El Carrusel de Tiempo) ---
        mask_retrasos = (df['dias_adicionados'] > 180) # Más de 6 meses de adición
        df.loc[mask_retrasos, 'riesgo_corrupcion'] += 50
        df.loc[mask_retrasos, 'motivo_alerta'] += '🚨 Múltiples adiciones/Retrasos críticos (>180 días). '

        # --- NUEVA REGLA 5: CONSORCIOS FANTASMAS (Especial para Licitaciones) ---
        mask_consorcio = df['proveedor_adjudicado'].str.contains('CONSORCIO|UNION TEMPORAL', case=False, na=False) & (df['valor_del_contrato'] > 500000000)
        df.loc[mask_consorcio, 'riesgo_corrupcion'] += 20
        df.loc[mask_consorcio, 'motivo_alerta'] += '⚠️ Consorcio Multimillonario adjudicado. '

        # PREPARAR RESPUESTA
        df_alertas = df[df['riesgo_corrupcion'] >= 30].copy()
        df_alertas = df_alertas.sort_values(by='riesgo_corrupcion', ascending=False)

        columnas_clave = ['id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 'modalidad_de_contratacion', 'valor_del_contrato', 'riesgo_corrupcion', 'motivo_alerta']
        df_final = df_alertas[columnas_clave]
        df_final = df_final.where(pd.notnull(df_final), None)

        return df_final.to_dict(orient='records')
        
    except Exception as e:
        print(f"🔥 Error Crítico en Pandas salvado: {e}")
        return []