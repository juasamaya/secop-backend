from neo4j import GraphDatabase
import pandas as pd

class MotorGrafos:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="#Clave1234"):
        # Asegúrate de poner tu password real aquí
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def _inyectar_fila(self, tx, row):
        # 1. EXTRACCIÓN Y LIMPIEZA DE DATOS CRÍTICOS
        nombre_entidad = str(row.get('nombre_entidad', 'DESCONOCIDO')).upper().strip()
        id_contrato = str(row.get('id_contrato', 'DESCONOCIDO')).strip()
        valor = float(row.get('valor_del_contrato', 0))
        modalidad = str(row.get('modalidad_de_contratacion', 'DESCONOCIDA')).strip()
        fecha = str(row.get('fecha_contrato_str', '')).strip()
        
        nit_proveedor = str(row.get('documento_proveedor', '')).upper().strip()
        nombre_proveedor = str(row.get('proveedor_adjudicado', 'DESCONOCIDO')).upper().strip()
        
        id_rep_legal = str(row.get('identificaci_n_representante_legal', '')).upper().strip()
        nombre_rep_legal = str(row.get('nombre_representante_legal', 'DESCONOCIDO')).upper().strip()

        # LISTA DE PALABRAS BASURA QUE EL ESTADO O PANDAS METEN CUANDO NO HAY DATOS
        palabras_basura = ['', 'NAN', 'DESCONOCIDO', 'SIN_DATOS', '0', 'SIN DESCRIPCION']

        # =========================================================
        # PASO A: INYECTAR ENTIDAD Y CONTRATO (Siempre existen)
        # =========================================================
        query_base = """
        MERGE (entidad:EntidadPublica {nombre: $nombre_entidad})
        MERGE (contrato:Contrato {id_contrato: $id_contrato})
        SET contrato.valor = $valor, 
            contrato.modalidad = $modalidad, 
            contrato.fecha = $fecha
        MERGE (entidad)-[:ADJUDICÓ]->(contrato)
        """
        tx.run(query_base, 
               nombre_entidad=nombre_entidad, 
               id_contrato=id_contrato, 
               valor=valor, 
               modalidad=modalidad, 
               fecha=fecha)

        # =========================================================
        # PASO B: INYECTAR EMPRESA (Solo si el NIT es real)
        # =========================================================
        if nit_proveedor not in palabras_basura:
            query_empresa = """
            MATCH (contrato:Contrato {id_contrato: $id_contrato})
            MERGE (empresa:Empresa {nit: $nit_proveedor})
            SET empresa.nombre = $nombre_proveedor
            MERGE (contrato)-[:GANADO_POR]->(empresa)
            """
            tx.run(query_empresa, id_contrato=id_contrato, nit_proveedor=nit_proveedor, nombre_proveedor=nombre_proveedor)

            # =========================================================
            # PASO C: INYECTAR REPRESENTANTE LEGAL (Solo si hay empresa y hay cédula real)
            # =========================================================
            if id_rep_legal not in palabras_basura:
                query_rep = """
                MATCH (empresa:Empresa {nit: $nit_proveedor})
                MERGE (rep_legal:Persona {cedula: $id_rep_legal})
                SET rep_legal.nombre = $nombre_rep_legal
                MERGE (rep_legal)-[:ES_REPRESENTANTE_DE]->(empresa)
                """
                tx.run(query_rep, nit_proveedor=nit_proveedor, id_rep_legal=id_rep_legal, nombre_rep_legal=nombre_rep_legal)

    def sincronizar_dataframe(self, df):
        """
        Recibe el DataFrame real filtrado por el usuario en Angular y lo sube a Neo4j.
        """
        if df is None or df.empty:
            return False
            
        print(f"🕸️ Inyectando {len(df)} contratos a la telaraña de Neo4j...")
        with self.driver.session() as session:
            for index, row in df.iterrows():
                session.execute_write(self._inyectar_fila, row)
        return True