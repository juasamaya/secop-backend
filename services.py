import requests
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, StandardScaler
from inyector_grafos import MotorGrafos

def analizar_contratos_secop(
    departamento=None, ciudad=None, entidad=None, busqueda=None, anio=None,
    umbral_corbatas=2, umbral_fraccionamiento=2, umbral_valor=40000000
):
    try:
        url = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
        
        parametros = { "$limit": 5000, "$order": "fecha_de_firma DESC" }
        
        if departamento: parametros["departamento"] = departamento.title()
        if ciudad: parametros["ciudad"] = ciudad.title() 
        
        textos_busqueda = []
        if busqueda: textos_busqueda.append(busqueda)
        if entidad: textos_busqueda.append(entidad)
        
        if textos_busqueda:
            parametros["$q"] = " ".join(textos_busqueda)

        respuesta = requests.get(url, params=parametros, timeout=15)
        if respuesta.status_code != 200: return []
        
        datos_json = respuesta.json()
        if not datos_json or isinstance(datos_json, dict): return []
            
        df = pd.DataFrame(datos_json)

        if entidad and 'nombre_entidad' in df.columns:
            df = df[df['nombre_entidad'].str.contains(entidad, case=False, na=False)]
            
        if df.empty: return []

        # LIMPIEZA Y ESTRUCTURACIÓN AMPLIADA (Nuevas columnas necesarias)
        columnas_requeridas = [
            'valor_del_contrato', 'modalidad_de_contratacion', 'documento_proveedor', 
            'tipo_de_contrato', 'id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 
            'fecha_de_firma', 'dias_adicionados', 'tipo_de_proceso', 'fecha_de_inicio_del_contrato',
            'identificaci_n_representante_legal', 'valor_facturado', 'codigo_de_categoria_principal'
        ]
        
        for col in columnas_requeridas:
            if col not in df.columns: df[col] = ''

        if 'detalle_del_objeto_a_contrat' in df.columns:
            df['descripcion_contrato'] = df['detalle_del_objeto_a_contrat'].fillna('Sin descripción detallada.')
        elif 'descripcion_del_proceso' in df.columns:
            df['descripcion_contrato'] = df['descripcion_del_proceso'].fillna('Sin descripción detallada.')
        else:
            df['descripcion_contrato'] = 'Descripción no disponible en este registro de SECOP.'

        df['valor_del_contrato'] = pd.to_numeric(df['valor_del_contrato'], errors='coerce').fillna(0)
        df['dias_adicionados'] = pd.to_numeric(df['dias_adicionados'], errors='coerce').fillna(0)
        df['valor_facturado'] = pd.to_numeric(df['valor_facturado'], errors='coerce').fillna(0)
        df['documento_proveedor'] = df['documento_proveedor'].fillna('DESCONOCIDO')
        df['proveedor_adjudicado'] = df['proveedor_adjudicado'].fillna('DESCONOCIDO')
        df['identificaci_n_representante_legal'] = df['identificaci_n_representante_legal'].fillna('')
        
        fechas_firma = pd.to_datetime(df['fecha_de_firma'], errors='coerce')
        fechas_inicio = pd.to_datetime(df['fecha_de_inicio_del_contrato'], errors='coerce')
        fechas_final = fechas_firma.fillna(fechas_inicio)
        
        df['anio_contrato'] = fechas_final.dt.year.fillna(0).astype(int)
        df['fecha_contrato_str'] = fechas_final.dt.strftime('%d/%m/%Y').fillna('No Registrada')
        
        df['score_humano'] = 0
        df['motivo_alerta'] = ''

        # =========================================================
        # NUEVOS MÓDULOS FORENSES (A, B, C, D)
        # =========================================================

        # A. CARRUSEL DE TESTAFERROS (Representante Legal con varios NITs)
        reps = df[df['identificaci_n_representante_legal'].str.len() > 3].groupby('identificaci_n_representante_legal')['documento_proveedor'].nunique()
        prov_testaferros = reps[reps > 1].index.tolist()
        mask_testaferros = df['identificaci_n_representante_legal'].isin(prov_testaferros)
        df.loc[mask_testaferros, 'score_humano'] += 60
        df.loc[mask_testaferros, 'motivo_alerta'] += '🚩 Múltiples Empresas (Mismo Rep. Legal). '

        # B. SOBRECOSTOS (Facturado vs Inicial)
        mask_sobrecosto = (df['valor_facturado'] > (df['valor_del_contrato'] * 1.2)) & (df['valor_del_contrato'] > 0)
        df.loc[mask_sobrecosto, 'score_humano'] += 40
        df.loc[mask_sobrecosto, 'motivo_alerta'] += '💰 Posible Sobrecosto (>20%). '

        # C. EMPRESAS TODOTERRENO (Muchos sectores distintos)
        df['segmento_categoria'] = df['codigo_de_categoria_principal'].astype(str).str[3:5] 
        categorias_prov = df[df['segmento_categoria'] != ''].groupby('documento_proveedor')['segmento_categoria'].nunique()
        prov_todoterreno = categorias_prov[categorias_prov >= 3].index.tolist()
        mask_todoterreno = df['documento_proveedor'].isin(prov_todoterreno) & (df['documento_proveedor'] != 'DESCONOCIDO')
        df.loc[mask_todoterreno, 'score_humano'] += 30
        df.loc[mask_todoterreno, 'motivo_alerta'] += '🛠️ Empresa Todoterreno. '

        # D. CONTRATOS RELÁMPAGO (Fechas sospechosas)
        mask_fin_anio = (fechas_final.dt.month == 12) & (fechas_final.dt.day >= 24)
        df.loc[mask_fin_anio, 'score_humano'] += 30
        df.loc[mask_fin_anio, 'motivo_alerta'] += '🎆 Raspado de Olla (Fin de año). '

        mask_fin_semana = fechas_final.dt.dayofweek >= 5
        df.loc[mask_fin_semana, 'score_humano'] += 30
        df.loc[mask_fin_semana, 'motivo_alerta'] += '📅 Firmado en Fin de Semana. '

        # =========================================================
        # REGLAS HEURÍSTICAS CLÁSICAS
        # =========================================================

        mask_sin_firma = fechas_firma.isna()
        df.loc[mask_sin_firma, 'score_humano'] += 20
        df.loc[mask_sin_firma, 'motivo_alerta'] += '🚩 Sin Fecha de Firma. '

        corbatas = df[df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)].groupby('documento_proveedor').agg(num_contratos=('id_contrato', 'count')).reset_index()
        prov_corbatas = corbatas[corbatas['num_contratos'] >= umbral_corbatas]['documento_proveedor'].tolist()
        mask_corbatas = df['documento_proveedor'].isin(prov_corbatas) & df['tipo_de_contrato'].str.contains('Prestación', case=False, na=False)
        df.loc[mask_corbatas, 'score_humano'] += 40
        df.loc[mask_corbatas, 'motivo_alerta'] += f'🚩 Corbata: >{umbral_corbatas - 1} Contratos. '

        fracc = df[df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)].groupby(['nombre_entidad', 'documento_proveedor']).agg(num_contratos_directos=('id_contrato', 'count')).reset_index()
        prov_fracc = fracc[fracc['num_contratos_directos'] >= umbral_fraccionamiento]['documento_proveedor'].tolist()
        mask_fracc = df['documento_proveedor'].isin(prov_fracc) & df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False)
        df.loc[mask_fracc, 'score_humano'] += 40
        df.loc[mask_fracc, 'motivo_alerta'] += f'🚩 Fraccionamiento. '

        mask_valor = df['modalidad_de_contratacion'].str.contains('Directa', case=False, na=False) & (df['valor_del_contrato'] > umbral_valor)
        df.loc[mask_valor, 'score_humano'] += 30
        df.loc[mask_valor, 'motivo_alerta'] += f'🚩 Contrato Directo > {umbral_valor // 1000000}M. '

        mask_retrasos = (df['dias_adicionados'] > 180)
        df.loc[mask_retrasos, 'score_humano'] += 50
        df.loc[mask_retrasos, 'motivo_alerta'] += '🚨 Retrasos críticos (>180 días). '
        
        mask_consorcio = df['proveedor_adjudicado'].str.contains('CONSORCIO|UNION TEMPORAL', case=False, na=False) & (df['valor_del_contrato'] > 500000000)
        df.loc[mask_consorcio, 'score_humano'] += 20
        df.loc[mask_consorcio, 'motivo_alerta'] += '⚠️ Consorcio Multimillonario. '

        mask_opacidad = df['proveedor_adjudicado'].str.contains('XXXX|SIN DESCRIPCION', case=False, na=False) & (df['valor_del_contrato'] > 100000000)
        df.loc[mask_opacidad, 'score_humano'] += 30
        df.loc[mask_opacidad, 'motivo_alerta'] += '🕵️‍♂️ Contratista Anónimo (>100M). '

        # INTELIGENCIA ARTIFICIAL
        if len(df) > 10:
            df_ia = df.copy()
            le = LabelEncoder()
            df_ia['modalidad_enc'] = le.fit_transform(df_ia['modalidad_de_contratacion'].astype(str))
            df_ia['tipo_enc'] = le.fit_transform(df_ia['tipo_de_contrato'].astype(str))
            
            columnas_para_ia = ['valor_del_contrato', 'dias_adicionados', 'modalidad_enc', 'tipo_enc']
            scaler = StandardScaler()
            datos_normalizados = scaler.fit_transform(df_ia[columnas_para_ia])
            
            modelo = IsolationForest(contamination=0.05, random_state=42)
            df_ia['is_anomalia'] = modelo.fit_predict(datos_normalizados)
            df_ia['score_anomalia_ia'] = modelo.decision_function(datos_normalizados)
            
            min_score = df_ia['score_anomalia_ia'].min()
            max_score = df_ia['score_anomalia_ia'].max()
            
            if max_score - min_score == 0: df['score_ia_final'] = 0
            else: df['score_ia_final'] = (1 - (df_ia['score_anomalia_ia'] - min_score) / (max_score - min_score)) * 100
            
            mask_ia = df_ia['is_anomalia'] == -1
            df.loc[mask_ia, 'motivo_alerta'] += '🧠 IA: Anomalía Matemática Detectada. '
        else:
            df['score_ia_final'] = 0

        # FUSIÓN FINAL
        df['riesgo_corrupcion'] = (df['score_humano'] * 0.7) + (df['score_ia_final'] * 0.3)
        df['riesgo_corrupcion'] = df['riesgo_corrupcion'].round().astype(int)

        if anio:
            prov_este_anio = df[df['anio_contrato'] == int(anio)]['documento_proveedor'].unique()
            prov_otros_anios = df[df['anio_contrato'] != int(anio)]['documento_proveedor'].unique()
            prov_reincidentes = set(prov_este_anio).intersection(set(prov_otros_anios))
            
            mask_reincidentes = df['documento_proveedor'].isin(prov_reincidentes)
            df.loc[mask_reincidentes, 'motivo_alerta'] += '🕰️ Contratación en otros años. '

        df_alertas = df[df['riesgo_corrupcion'] >= 20].copy() 
        df_alertas = df_alertas.sort_values(by='riesgo_corrupcion', ascending=False)

        if anio:
            df_alertas = df_alertas[df_alertas['anio_contrato'] == int(anio)]
            if df_alertas.empty: return []

        columnas_clave = [
            'id_contrato', 'nombre_entidad', 'proveedor_adjudicado', 
            'modalidad_de_contratacion', 'valor_del_contrato', 'riesgo_corrupcion', 
            'motivo_alerta', 'descripcion_contrato', 'anio_contrato', 'fecha_contrato_str'
        ]
        df_final = df_alertas[columnas_clave]
        df_final = df_final.where(pd.notnull(df_final), None)

        return df_final.to_dict(orient='records')
        
    except Exception as e:
        print(f"🔥 Error Crítico salvado: {e}")
        return []