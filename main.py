import math
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
from services import analizar_contratos_secop

app = FastAPI(
    title="Motor de Riesgo SECOP",
    description="API para detección de anomalías y banderas rojas en contratación pública colombiana.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/alertas")
async def obtener_alertas(
    departamento: Optional[str] = None,
    ciudad: Optional[str] = None,
    entidad: Optional[str] = None,
    busqueda: Optional[str] = None,
    umbral_corbatas: int = 2,
    umbral_fraccionamiento: int = 2,
    umbral_valor: int = 40000000,
    pagina: int = 1,
    limite: int = 10
):
    datos_procesados = analizar_contratos_secop(
        departamento=departamento, 
        ciudad=ciudad, 
        entidad=entidad, 
        busqueda=busqueda,
        umbral_corbatas=umbral_corbatas,
        umbral_fraccionamiento=umbral_fraccionamiento,
        umbral_valor=umbral_valor
    )
    
    total_alertas = len(datos_procesados)
    total_paginas = math.ceil(total_alertas / limite) if total_alertas > 0 else 1
    
    if pagina > total_paginas:
        pagina = total_paginas
    if pagina < 1:
        pagina = 1
        
    inicio = (pagina - 1) * limite
    fin = inicio + limite
    datos_paginados = datos_procesados[inicio:fin]
    
    return {
        "metadata": {
            "total_alertas": total_alertas,
            "pagina_actual": pagina,
            "total_paginas": total_paginas,
            "limite_por_pagina": limite
        },
        "parametros_usados": {
            "umbral_corbatas": umbral_corbatas,
            "umbral_fraccionamiento": umbral_fraccionamiento,
            "umbral_valor": umbral_valor
        },
        "datos": datos_paginados
    }