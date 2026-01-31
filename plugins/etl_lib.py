import pandas as pd
from rapidfuzz import process, fuzz, utils

def unificar_nombres_clientes(df, col_cliente='CUSTNUM', col_grupo='INV'):
    print(f"--- Iniciando unificación de clientes en '{col_cliente}' ---")
    
    clientes = df[col_cliente].dropna().astype(str).unique()
    clientes_ord = sorted(clientes, key=len, reverse=True)
    
    reemplazos = {}
    procesados = set()
    
    for nombre in clientes_ord:
        if nombre in procesados: continue
        
        matches = process.extract(
            nombre, clientes_ord, scorer=fuzz.token_sort_ratio, limit=None, score_cutoff=85
        )
        
        for match_tuple in matches:
            variante = match_tuple[0]
            if variante not in procesados:
                if variante != nombre:
                    reemplazos[variante] = nombre
                procesados.add(variante)
    
    if reemplazos:
        print(f"Unificando {len(reemplazos)} variantes de nombres.")
        df[col_cliente] = df[col_cliente].replace(reemplazos)

    grupos = df.groupby(col_grupo)[col_cliente].unique()
    inconsistentes = grupos[grupos.apply(len) > 1]
    
    reemplazos_ctx = {}
    for inv, nombres in inconsistentes.items():
        nombres_validos = [n for n in nombres if pd.notna(n)]
        if not nombres_validos: continue
        
        mejor_nombre = max(nombres_validos, key=len)
        
        for n in nombres_validos:
            if n != mejor_nombre:
                reemplazos_ctx[n] = mejor_nombre
                
    if reemplazos_ctx:
        print(f"Corrigiendo {len(reemplazos_ctx)} inconsistencias por INV.")
        df[col_cliente] = df[col_cliente].replace(reemplazos_ctx)
        
    return df