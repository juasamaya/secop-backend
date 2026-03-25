import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
USUARIO = os.getenv("NEO4J_USERNAME", "neo4j")
CLAVE = os.getenv("NEO4J_PASSWORD", "#Clave1234")

class IngestorCSVForense:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def procesar_fila_pep(self, tx, row):
        # 1. Limpiamos y unimos el nombre del Funcionario Público (PEP)
        nombres_pep = f"{row.get('PRIMER_NOMBRE_DECLARANTE_PEP', '')} {row.get('SEGUNDO_NOMBRE_DECLARANTE_PEP', '')}".strip()
        apellidos_pep = f"{row.get('PRIMER_APELLIDO_DECLARANTE_PEP', '')} {row.get('SEGUNDO_APELLIDO_DECLARANTE_PEP', '')}".strip()
        nombre_completo_pep = f"{nombres_pep} {apellidos_pep}".replace('nan', '').strip()
        
        cedula_pep = str(row.get('NUMERO_DOCUMENTO_PEP', ''))
        cargo = str(row.get('CARGO_DECLARANTE_PEP', 'FUNCIONARIO PUBLICO'))
        entidad = str(row.get('ENTIDAD_NOMBRE', 'ESTADO COLOMBIANO'))

        if not nombre_completo_pep or not cedula_pep:
            return

        # 2. Inyectamos al Político y su Entidad en Neo4j
        query_pep = """
        MERGE (entidad:EntidadPublica {nombre: $entidad})
        MERGE (politico:Persona {cedula: $cedula})
        SET politico.nombre = $nombre_completo, politico.cargo = $cargo, politico.es_pep = true
        MERGE (politico)-[:DIRIGE_O_TRABAJA_EN]->(entidad)
        """
        tx.run(query_pep, entidad=entidad, cedula=cedula_pep, nombre_completo=nombre_completo_pep, cargo=cargo)

        # 3. EXTRAEMOS AL CÓNYUGE (Si aplica)
        if str(row.get('TIENE_CONYUGE_COMPANERO_PERMANENTE', '')).strip().upper() == 'SI':
            nombres_conyuge = f"{row.get('CONYUGE_COMPAÑERO_PERMANENTE_PNOMBRE', '')} {row.get('CONYUGE_COMPAÑERO_PERMANENTE_SNOMBRE', '')}".strip()
            apellidos_conyuge = f"{row.get('CONYUGE_COMPAÑERO_PERMANENTE_PAPELLIDO', '')} {row.get('CONYUGE_COMPAÑERO_PERMANENTE_SAPELLIDO', '')}".strip()
            nombre_conyuge = f"{nombres_conyuge} {apellidos_conyuge}".replace('nan', '').strip()
            cedula_conyuge = str(row.get('CONYUGE_COMPANERO_PERMANENTE_NUM_DOC', '')).replace('.0', '')
            
            if nombre_conyuge:
                query_conyuge = """
                MATCH (politico:Persona {cedula: $cedula_pep})
                MERGE (familiar:Persona {cedula: $cedula_familiar})
                SET familiar.nombre = $nombre_familiar
                MERGE (familiar)-[:FAMILIAR_DE {tipo: 'CONYUGE'}]->(politico)
                """
                tx.run(query_conyuge, 
                       cedula_pep=cedula_pep, 
                       cedula_familiar=cedula_conyuge, 
                       nombre_familiar=nombre_conyuge)

        # ==========================================
        # 4. EXTRAEMOS AL RESTO DE PARIENTES (Modificado)
        # Rompiendo la cadena de texto para crear Nodos
        # ==========================================
        col_parientes = str(row.get('PARIENTES', ''))
        if col_parientes and col_parientes.lower() != 'nan':
            # 4.1. Separamos cada familiar (usando el símbolo |)
            lista_parientes = col_parientes.split('|')
            
            for pariente_raw in lista_parientes:
                # 4.2. Separamos los atributos del familiar (usando el símbolo ;)
                datos_p = pariente_raw.split(';')
                
                # Nos aseguramos de que el registro esté completo (esperamos unos 7 campos)
                if len(datos_p) >= 7:
                    parentesco = datos_p[0].strip().upper()
                    # datos_p[1] es el Tipo de Documento, lo saltamos por ahora
                    cedula_p = datos_p[2].strip()
                    p_nombre = datos_p[3].strip()
                    s_nombre = datos_p[4].strip()
                    p_apellido = datos_p[5].strip()
                    s_apellido = datos_p[6].strip()
                    
                    # Unimos el nombre limpiando espacios dobles si no tiene segundo nombre
                    nombre_completo_p = f"{p_nombre} {s_nombre} {p_apellido} {s_apellido}".replace('  ', ' ').strip()
                    
                    if nombre_completo_p and cedula_p:
                        # 4.3. Inyectamos a este familiar como un nodo individual
                        query_pariente = """
                        MATCH (politico:Persona {cedula: $cedula_pep})
                        MERGE (familiar:Persona {cedula: $cedula_familiar})
                        SET familiar.nombre = $nombre_familiar
                        MERGE (familiar)-[:FAMILIAR_DE {tipo: $parentesco}]->(politico)
                        """
                        tx.run(query_pariente,
                               cedula_pep=cedula_pep,
                               cedula_familiar=cedula_p,
                               nombre_familiar=nombre_completo_p,
                               parentesco=parentesco)

    def iniciar_ingesta_masiva(self, ruta_archivo):
        print(f"🚀 Iniciando lectura del archivo masivo: {ruta_archivo}")
        
        try:
            # CAMBIO CLAVE: sheet_name=0 asegura que siempre lea la primera pestaña, sin importar el nombre
            df = pd.read_excel(ruta_archivo, sheet_name=0, dtype=str, engine='openpyxl')
            total_filas = len(df)
            print(f"📊 ¡Archivo Excel cargado con {total_filas} declaraciones de funcionarios públicos!")
        except Exception as e:
            print(f"🔥 Error al leer el Excel: {e}")
            return

        print("🕸️ Inyectando la telaraña política en Neo4j... (Esto tomará un momento)")
        
        with self.driver.session() as session:
            # Procesaremos solo los primeros 10,000 para esta prueba
            for index, row in df[:10000].iterrows():
                session.execute_write(self.procesar_fila_pep, row)
                if index % 500 == 0 and index > 0:
                    print(f"   -> Procesados {index} de {total_filas} funcionarios...")
                    
        print("\n✅ ¡INGESTA FORENSE MASIVA COMPLETADA CON ÉXITO!")

if __name__ == "__main__":
    # RUTA EXACTA A TU ARCHIVO EXCEL
    ARCHIVO_EXCEL = "DECLARACION_PEP.xlsx"
    
    ingestor = IngestorCSVForense(URI, USUARIO, CLAVE)
    ingestor.iniciar_ingesta_masiva(ARCHIVO_EXCEL)