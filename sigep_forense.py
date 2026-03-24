import requests
from bs4 import BeautifulSoup
from neo4j import GraphDatabase
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class ScraperSigep:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="Clave1234"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        # Usamos una sesión para simular un navegador real y evitar bloqueos del gobierno
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

    def close(self):
        self.driver.close()

    def buscar_funcionario_real(self, primer_nombre, segundo_nombre, primer_apellido, segundo_apellido):
        nombre_completo = f"{primer_nombre} {segundo_nombre} {primer_apellido} {segundo_apellido}".strip()
        print(f"🔍 Buscando a {nombre_completo} en el portal de Ley 2013 (consultaPEP)...")
        
        url_buscador = "https://www.funcionpublica.gov.co/fdci/consultaCiudadana/consultaPEP"
        
        # EL SECRETO REVELADO POR GOOGLE ANALYTICS:
        # El gobierno no usa POST, usa GET con estos parámetros exactos
        parametros_get = {
            "tipoRegistro": "0",
            "numeroDocumento": "",
            "primerNombre": primer_nombre,
            "segundoNombre": segundo_nombre,
            "primerApellido": primer_apellido,
            "segundoApellido": segundo_apellido,
            "entidad": "",
            "dpto": "",
            "mun": "",
            "find": "Find"
        }
        
        try:
            # CAMBIAMOS POST POR GET y data por params
            respuesta = self.session.get(url_buscador, params=parametros_get, timeout=15, verify=False)
            
            if respuesta.status_code != 200:
                print("❌ El servidor del SIGEP no responde.")
                return None
                
            soup = BeautifulSoup(respuesta.text, 'lxml')
            enlace_declaracion = None
            
            # Buscamos el enlace (El gobierno suele poner un ícono de un ojo o un PDF)
            enlaces = soup.find_all('a', href=True)
            for enlace in enlaces:
                href = enlace['href'].lower()
                if 'detalle' in href or 'declaracion' in href or 'consultaciudadana/descargar' in href or 'id=' in href:
                    enlace_declaracion = enlace
                    break
            
            if not enlace_declaracion:
                print("⚠️ El servidor respondió, pero la tabla de resultados vino vacía. (Bloqueo o sin datos).")
                return None
                
            url_extraida = enlace_declaracion['href']
            
            if url_extraida.startswith("http"):
                url_detalle = url_extraida
            else:
                url_detalle = "https://www.funcionpublica.gov.co" + url_extraida
                
            return self.extraer_parientes_html(url_detalle, nombre_completo)
            
        except Exception as e:
            print(f"🔥 Error raspando el SIGEP: {e}")
            return None

    def extraer_parientes_html(self, url_detalle, nombre_completo):
        print(f"📄 Extrayendo PDF/HTML de la declaración...")
        respuesta = self.session.get(url_detalle, timeout=15, verify=False)
        soup = BeautifulSoup(respuesta.text, 'lxml')
        
        datos = {
            "funcionario": {"nombre": nombre_completo.upper(), "cargo": "ALTO FUNCIONARIO", "entidad": "ESTADO COLOMBIANO"},
            "parientes_declarados": []
        }
        
        # Raspamos la sección "1.b. De los parientes"
        # Buscamos todas las tablas y filtramos la que tiene los parentescos
        tablas = soup.find_all('table')
        for tabla in tablas:
            if 'Parentesco' in tabla.text or 'Cónyuge' in tabla.text:
                filas = tabla.find_all('tr')
                # Saltamos la cabecera
                for fila in filas[1:]:
                    columnas = fila.find_all('td')
                    if len(columnas) >= 3:
                        nombre_pariente = columnas[0].text.strip().upper()
                        identificacion = columnas[1].text.strip()
                        parentesco = columnas[2].text.strip().upper()
                        
                        if nombre_pariente:
                            datos["parientes_declarados"].append({
                                "nombre": nombre_pariente,
                                "cedula": identificacion,
                                "parentesco": parentesco
                            })
                break # Solo necesitamos la primera tabla de parientes
                
        print(f"✅ Se encontraron {len(datos['parientes_declarados'])} familiares registrados.")
        return datos

    def _inyectar_red(self, tx, datos_sigep):
        funcionario = datos_sigep["funcionario"]
        parientes = datos_sigep["parientes_declarados"]

        query_funcionario = """
        MERGE (politico:Persona {nombre: $nombre_politico})
        SET politico.cargo = $cargo, politico.es_pep = true
        """
        tx.run(query_funcionario, nombre_politico=funcionario["nombre"], cargo=funcionario["cargo"])

        for pariente in parientes:
            query_familia = """
            MATCH (politico:Persona {nombre: $nombre_politico})
            MERGE (familiar:Persona {nombre: $nombre_familiar}) // A veces ocultan la cédula, usamos el nombre
            SET familiar.cedula = $cedula_familiar
            MERGE (familiar)-[:FAMILIAR_DE {tipo: $tipo_parentesco}]->(politico)
            """
            tx.run(query_familia,
                   nombre_politico=funcionario["nombre"],
                   cedula_familiar=pariente["cedula"],
                   nombre_familiar=pariente["nombre"],
                   tipo_parentesco=pariente["parentesco"])

    def rastrear_e_inyectar(self, primer_nombre, segundo_nombre, primer_apellido, segundo_apellido):
        datos = self.buscar_funcionario_real(primer_nombre, segundo_nombre, primer_apellido, segundo_apellido)
        if datos and datos["parientes_declarados"]:
            with self.driver.session() as session:
                session.execute_write(self._inyectar_red, datos)
            print("🕸️ ¡Familiares inyectados en Neo4j con éxito!")
        else:
            print("🛑 No se inyectó nada a la base de datos.")

if __name__ == "__main__":
    # ¡Prueba con datos reales apuntando al nuevo endpoint!
    scraper = ScraperSigep("bolt://localhost:7687", "neo4j", "tu_password_secreto")
    
    # Intenta buscar a un funcionario real dividiendo sus nombres
    scraper.rastrear_e_inyectar(
        primer_nombre="GUSTAVO", 
        segundo_nombre="FRANCISCO", 
        primer_apellido="PETRO", 
        segundo_apellido="URREGO"
    )
    scraper.close()