import pandas as pd
import requests
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, StandardScaler

def analizar_contratos_secop(
    departamento=None, ciudad=None, entidad=None, busqueda=None,
    umbral_corbatas=2, umbral_fraccionamiento=2, umbral_valor=40000000
):
    try:
        url = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
        
        parametros = { "$limit": 5000, "$order": "fecha_de_firma DESC" }
        
        if departamento: parametros["departamento"] = departamento.title()
        if ciudad: parametros["ciudad"] = ciudad.title() 
        if entidad: parametros["nombre_entidad"] = entidad.upper()
        if busqueda: parametros["$q"] = busqueda 

        respuesta = requests.get(url, params=parametros)
        if respuesta.status_code != 200: return []
        datos_json = respuesta.json()
        if not datos_json or isinstance(datos_json, dict): return []
            
        df = pd.DataFrame(datos_json)

        # 1. BLINDAJE
        columnas_requeridas = [
            'valor_del_contrato', 'modalidad_de_contratacion', 'documento_proveedor', 
            'tipo_de_contrato', 'id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 
            'fecha_de_firma', 'dias_adicionados', 'tipo_de_proceso'
        ]
        
        for col in columnas_requeridas:
            if col not in df.columns: df[col] = ''

        df['valor_del_contrato'] = pd.to_numeric(df['valor_del_contrato'], errors='coerce').fillna(0)
        df['dias_adicionados'] = pd.to_numeric(df['dias_adicionados'], errors='coerce').fillna(0)
        df['documento_proveedor'] = df['documento_proveedor'].fillna('DESCONOCIDO')
        df['proveedor_adjudicado'] = df['proveedor_adjudicado'].fillna('DESCONOCIDO')
        
        df['score_humano'] = 0
        df['motivo_alerta'] = ''

        # 2. CAPA HEURÍSTICA (Reglas)
        # Corbatas
        corbatas = df[df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)].groupby('documento_proveedor').agg(num_contratos=('id_contrato', 'count')).reset_index()
        prov_corbatas = corbatas[corbatas['num_contratos'] >= umbral_corbatas]['documento_proveedor'].tolist()
        mask_corbatas = df['documento_proveedor'].isin(prov_corbatas) & df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)
        df.loc[mask_corbatas, 'score_humano'] += 40
        df.loc[mask_corbatas, 'motivo_alerta'] += f'🚩 Corbata: >{umbral_corbatas - 1} Contratos. '

        # Fraccionamiento
        fracc = df[df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)].groupby(['nombre_entidad', 'documento_proveedor']).agg(num_contratos_directos=('id_contrato', 'count')).reset_index()
        prov_fracc = fracc[fracc['num_contratos_directos'] >= umbral_fraccionamiento]['documento_proveedor'].tolist()
        mask_fracc = df['documento_proveedor'].isin(prov_fracc) & df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)
        df.loc[mask_fracc, 'score_humano'] += 40
        df.loc[mask_fracc, 'motivo_alerta'] += f'🚩 Fraccionamiento. '

        # Valor Directo
        mask_valor = df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False) & (df['valor_del_contrato'] > umbral_valor)
        df.loc[mask_valor, 'score_humano'] += 30
        df.loc[mask_valor, 'motivo_alerta'] += f'🚩 Contrato Directo > {umbral_valor // 1000000}M. '

        # Retrasos
        mask_retrasos = (df['dias_adicionados'] > 180)
        df.loc[mask_retrasos, 'score_humano'] += 50
        df.loc[mask_retrasos, 'motivo_alerta'] += '🚨 Retrasos críticos (>180 días). '
        
        # Consorcios
        mask_consorcio = df['proveedor_adjudicado'].str.contains('CONSORCIO|UNION TEMPORAL', case=False, na=False) & (df['valor_del_contrato'] > 500000000)
        df.loc[mask_consorcio, 'score_humano'] += 20
        df.loc[mask_consorcio, 'motivo_alerta'] += '⚠️ Consorcio Multimillonario. '

        # NUEVO: Opacidad (Nombres Ocultos multimillonarios)
        mask_opacidad = df['proveedor_adjudicado'].str.contains('XXXX|SIN DESCRIPCION', case=False, na=False) & (df['valor_del_contrato'] > 100000000)
        df.loc[mask_opacidad, 'score_humano'] += 30
        df.loc[mask_opacidad, 'motivo_alerta'] += '🕵️‍♂️ Contratista Anónimo (>100M). '

        # 3. CAPA DE INTELIGENCIA ARTIFICIAL
        if len(df) > 10:
            df_ia = df.copy()
            le = LabelEncoder()
            df_ia['modalidad_enc'] = le.fit_transform(df_ia['modalidad_de_contratacion'].astype(str))
            df_ia['tipo_enc'] = le.fit_transform(df_ia['tipo_de_contrato'].astype(str))
            
            columnas_para_ia = ['valor_del_contrato', 'dias_adicionados', 'modalidad_enc', 'tipo_enc']
            scaler = StandardScaler()
            datos_normalizados = scaler.fit_transform(df_ia[columnas_para_ia])
            
            modelo = IsolationForest(contamination=0.05, random_state=42)
            df_ia['is_anomalia'] = modelo.fit_predict(datos_normalizados) # <-- Corregido
            df_ia['score_anomalia_ia'] = modelo.decision_function(datos_normalizados)
            
            min_score = df_ia['score_anomalia_ia'].min()
            max_score = df_ia['score_anomalia_ia'].max()
            
            if max_score - min_score == 0: df['score_ia_final'] = 0
            else: df['score_ia_final'] = (1 - (df_ia['score_anomalia_ia'] - min_score) / (max_score - min_score)) * 100
            
            mask_ia = df_ia['is_anomalia'] == -1
            df.loc[mask_ia, 'motivo_alerta'] += '🧠 IA: Anomalía Matemática Detectada. '
        else:
            df['score_ia_final'] = 0

        # 4. FUSIÓN FINAL
        df['riesgo_corrupcion'] = (df['score_humano'] * 0.7) + (df['score_ia_final'] * 0.3)
        df['riesgo_corrupcion'] = df['riesgo_corrupcion'].round().astype(int)

        df_alertas = df[df['riesgo_corrupcion'] >= 20].copy() 
        df_alertas = df_alertas.sort_values(by='riesgo_corrupcion', ascending=False)

        columnas_clave = ['id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 'modalidad_de_contratacion', 'valor_del_contrato', 'riesgo_corrupcion', 'motivo_alerta']
        df_final = df_alertas[columnas_clave]
        df_final = df_final.where(pd.notnull(df_final), None)

        return df_final.to_dict(orient='records')
        
    except Exception as e:
        print(f"🔥 Error Crítico salvado: {e}")
        return []