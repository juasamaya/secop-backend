from fastapi import FastAPI, HTTPException, Header, Query, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from neo4j import GraphDatabase
import shutil
import os
import pandas as pd

# Importamos tu motor de SECOP y tu Ingestor Forense
from services import analizar_contratos_secop 
from ingestor_pep_csv import IngestorCSVForense 
# Importamos tu inyector de grafos para guardar solo la página actual
from inyector_grafos import MotorGrafos
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USUARIO = os.getenv("NEO4J_USERNAME", "neo4j")
CLAVE = os.getenv("NEO4J_PASSWORD", "#Clave1234")
API_KEY =os.getenv("API_KEY")

app = FastAPI(title="Radar SECOP API Forense")

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"]
)

@app.get("/api/alertas")
def obtener_alertas(
    ciudad: Optional[str] = None, departamento: Optional[str] = None, entidad: Optional[str] = None,
    busqueda: Optional[str] = None, anio: Optional[int] = None, umbral_corbatas: int = 2,
    umbral_fraccionamiento: int = 2, umbral_valor: int = 40000000, pagina: int = 1, limite: int = 10,
    x_api_key: Optional[str] = Header(None)
):
    try:
        if x_api_key != API_KEY: 
            raise HTTPException(status_code=401, detail="No autorizado")
        
        resultados_completos = analizar_contratos_secop(
            departamento=departamento, ciudad=ciudad, entidad=entidad,
            busqueda=busqueda, anio=anio, umbral_corbatas=umbral_corbatas,
            umbral_fraccionamiento=umbral_fraccionamiento, umbral_valor=umbral_valor
        )

        total_alertas = len(resultados_completos)
        inicio = (pagina - 1) * limite
        fin = inicio + limite
        datos_paginados = resultados_completos[inicio:fin]
        total_paginas = (total_alertas // limite) + (1 if total_alertas % limite > 0 else 0)

        # ==========================================
        # SOLUCIÓN DE RENDIMIENTO: INYECTAR SOLO 10 CONTRATOS
        # ==========================================
        if len(datos_paginados) > 0:
            try:
                motor = MotorGrafos(uri=URI, user=USUARIO, password=CLAVE)
                motor.sincronizar_dataframe(pd.DataFrame(datos_paginados))
                motor.close()
                print(f"🕷️ Inyectados {len(datos_paginados)} contratos (Página {pagina}) en Neo4j.")
            except Exception as e:
                print(f"⚠️ Aviso Neo4j: {e}")

        return {
            "datos": datos_paginados,
            "metadata": { "total_alertas": total_alertas, "pagina_actual": pagina, "total_paginas": total_paginas }
        }
    except Exception as e:
        import traceback
        error_real = traceback.format_exc()
        print(f"🔥 ERROR FATAL: {error_real}")
        # Ahora Python le enviará el error exacto a Angular
        raise HTTPException(status_code=500, detail=f"Falló Python: {str(e)}")


@app.post("/api/alertas/forense/cargar-pep")
async def cargar_archivo_pep(file: UploadFile = File(...)):
    try:
        ruta_temp = f"temp_{file.filename}"
        with open(ruta_temp, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        ingestor = IngestorCSVForense(uri=URI, user=USUARIO, password=CLAVE)
        ingestor.iniciar_ingesta_masiva(ruta_temp)
        ingestor.close()
        
        if os.path.exists(ruta_temp): os.remove(ruta_temp)
        return {"mensaje": "Ingesta forense completada."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alertas/forense/contrato/{id_contrato}")
def obtener_red_contrato(id_contrato: str):
    try:
        driver = GraphDatabase.driver(URI, auth=(USUARIO, CLAVE))
        
        # ==========================================
        # SOLUCIÓN DE SPAM: CONSULTA AGNÓSTICA (Sin nombres de flechas)
        # ==========================================
        query = """
        MATCH (c:Contrato {id_contrato: $id_contrato})
        WITH c LIMIT 1
        
        OPTIONAL MATCH p1 = (c)--(ent:EntidadPublica)
        OPTIONAL MATCH p2 = (c)--(nodo_proveedor) WHERE nodo_proveedor:Empresa OR nodo_proveedor:Persona
        OPTIONAL MATCH p3 = (c)--(emp:Empresa)--(rep:Persona)
        OPTIONAL MATCH p4 = (c)-[*1..3]-(pep:Persona {es_pep: true})
        
        RETURN p1, p2, p3, p4
        """
        
        nodos_dict = {}
        enlaces_dict = {}
        
        with driver.session() as session:
            resultados = session.run(query, id_contrato=id_contrato)
            for record in resultados:
                for key in record.keys():
                    path = record[key]
                    if path is None: continue
                    
                    for node in path.nodes:
                        node_id = str(getattr(node, 'element_id', getattr(node, 'id', '0')))
                        if node_id not in nodos_dict:
                            props = dict(node)
                            
                            # SOLUCIÓN DE INFO DE PERSONA: Resaltamos el proveedor adjudicado
                            nombre_principal = props.get('nombre', props.get('proveedor_adjudicado', props.get('id_contrato', 'Desconocido')))
                            
                            titulo_html = f"<b style='font-size:1.1em; color:#60a5fa;'>{nombre_principal}</b><br><hr style='margin:5px 0; border-color:#374151;'>"
                            for k, v in props.items():
                                if k not in ['nombre', 'parientes_raw']:
                                    titulo_html += f"<b>{k.replace('_', ' ').title()}:</b> {v}<br>"

                            etiquetas = list(node.labels)
                            tipo_nodo = etiquetas[0] if etiquetas else "Desconocido"
                            if tipo_nodo == "Persona" and node.get("es_pep") == True: tipo_nodo = "PEP"
                                
                            nodos_dict[node_id] = {
                                "id": node_id, "label": nombre_principal[:20] + "...",
                                "group": tipo_nodo, "title": titulo_html
                            }
                            
                    for rel in path.relationships:
                        rel_id = str(getattr(rel, 'element_id', getattr(rel, 'id', '0')))
                        origen = str(getattr(rel.start_node, 'element_id', getattr(rel.start_node, 'id', '0')))
                        destino = str(getattr(rel.end_node, 'element_id', getattr(rel.end_node, 'id', '0')))
                        enlaces_dict[rel_id] = {
                            "id": rel_id, "from": origen, "to": destino, "label": rel.type 
                        }
                        
        driver.close()
        return {"nodes": list(nodos_dict.values()), "edges": list(enlaces_dict.values())}
    except Exception as e:
        print(f"Error Neo4j: {e}")
        return {"nodes": [], "edges": []}