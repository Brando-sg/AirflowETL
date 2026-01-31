from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
import os
from rapidfuzz import process, fuzz, utils
from etl_lib import unificar_nombres_clientes

# --- RUTAS ---
DATA_DIR = '/opt/airflow/data'
FILES = {
    'raw_web': f'{DATA_DIR}/Web_orders.txt',
    'raw_cat': f'{DATA_DIR}/Catalog_Orders.txt',
    'raw_pro': f'{DATA_DIR}/products.txt',
    'stage_web': f'{DATA_DIR}/stage_web.csv',
    'stage_cat': f'{DATA_DIR}/stage_catalog.csv',
    'stage_pro': f'{DATA_DIR}/stage_products.csv',
    'output': f'{DATA_DIR}/all_orders_integrated.csv'
}

def eliminar_duplicados_sin_id(df, id_col='ID'):
    if id_col in df.columns:
        cols_check = [c for c in df.columns if c != id_col]
        return df.drop_duplicates(subset=cols_check, keep='first')
    return df.drop_duplicates()

def prep_web_func(**kwargs):
    if not os.path.exists(FILES['raw_web']): return
    with open(FILES['raw_web'], 'r', encoding='utf-8') as f:
        lines = f.readlines()
    if lines:
        lines[0] = lines[0].replace(',', ';')
    with open(FILES['stage_web'], 'w', encoding='utf-8') as f:
        f.writelines(lines)

def prep_cat_func(**kwargs):
    if not os.path.exists(FILES['raw_cat']): return
    with open(FILES['raw_cat'], 'r', encoding='utf-8') as f:
        lines = f.readlines()
    fixed = []
    for line in lines:
        if '," ,' in line:
            line = line.replace('," ,', ',"",')
        fixed.append(line)
    with open(FILES['stage_cat'], 'w', encoding='utf-8') as f:
        f.writelines(fixed)

def prep_pro_func(**kwargs):
    if os.path.exists(FILES['raw_pro']):
        pd.read_csv(FILES['raw_pro']).to_csv(FILES['stage_pro'], index=False)


def trans_web_func(**kwargs):
    if not os.path.exists(FILES['stage_web']): return
    print("Transformando Web...")
    df = pd.read_csv(FILES['stage_web'], sep=';')
    
    mapping = {'DATE':'PCODE', 'CATALOG':'DATE', 'PCODE':'CATALOG', 'custnum':'CUSTNUM'}
    df.rename(columns={k:v for k,v in mapping.items() if k in df.columns}, inplace=True)
    df.dropna(inplace=True)
    df['INV'] = pd.to_numeric(df['INV'], errors='coerce').astype('Int64')
    df['QTY'] = df['QTY'].astype('Int64')
    df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, errors='coerce')
    
    df = unificar_nombres_clientes(df, col_cliente='CUSTNUM', col_grupo='INV')
    
    df['CATALOG'] = df['CATALOG'].astype(str).str.upper().str.strip()
    conteo = df['CATALOG'].value_counts()
    frecuentes = conteo[conteo > 50].index.tolist()
    
    reemplazos = {}
    for valor in df['CATALOG'].unique():
        if valor in frecuentes: continue
        match = process.extractOne(valor, frecuentes, scorer=fuzz.token_sort_ratio)
        if match and match[1] >= 60:
            reemplazos[valor] = match[0]
    if reemplazos: df['CATALOG'] = df['CATALOG'].replace(reemplazos)
    
    df['PCODE'] = df['PCODE'].astype(str).str.upper().str.replace('O', '0', regex=False)
    if os.path.exists(FILES['stage_pro']):
        maestro = pd.read_csv(FILES['stage_pro'])['PCODE'].astype(str).str.upper().unique()
        df['PCODE'] = df['PCODE'].apply(lambda x: process.extractOne(x, maestro, scorer=fuzz.ratio)[0] if x not in maestro and process.extractOne(x, maestro, scorer=fuzz.ratio)[1] >= 60 else x)

    df['ORIGEN'] = 'WEB'
    df.to_csv(FILES['stage_web'], index=False)

def trans_cat_func(**kwargs):
    if not os.path.exists(FILES['stage_cat']): return
    print("Transformando Catalog...")
    df = pd.read_csv(FILES['stage_cat'], sep=',')
    if 'custnum' in df.columns: df.rename(columns={'custnum':'CUSTNUM'}, inplace=True)
    
    df['INV'] = pd.to_numeric(df['INV'], errors='coerce').astype('Int64')
    df['CUSTNUM'] = pd.to_numeric(df['CUSTNUM'], errors='coerce').astype('Int64')
    df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce')
    df['DATE'] = pd.to_datetime(df['DATE'], format='%m/%y/%d %H:%M:%S', errors='coerce')
    
    df['PCODE'] = df['PCODE'].astype(str).str.upper().str.replace('O', '0', regex=False)
    if os.path.exists(FILES['stage_pro']):
        maestro = pd.read_csv(FILES['stage_pro'])['PCODE'].astype(str).str.upper().unique()
        df['PCODE'] = df['PCODE'].apply(lambda x: process.extractOne(x, maestro, scorer=fuzz.ratio)[0] if x not in maestro and process.extractOne(x, maestro, scorer=fuzz.ratio)[1] >= 60 else x)
    mask_notna = df['CATALOG'].notna()
    df.loc[mask_notna, 'CATALOG'] = df.loc[mask_notna, 'CATALOG'].astype(str).str.upper().str.strip()
    CORRECCIONES = {'PRTS': 'PETS', 'PATS': 'PETS', 'PEST': 'PETS', 'TOSY': 'TOYS', 'GARDNING': 'GARDENING', 'SOFTWARS': 'SOFTWARE'}
    df['CATALOG'] = df['CATALOG'].replace(CORRECCIONES)

    conteo = df['CATALOG'].value_counts()
    frecuentes = conteo[conteo > 100].index.tolist()
    reemplazos = {}
    for valor in df['CATALOG'].dropna().unique():
        if valor in frecuentes: continue
        match = process.extractOne(valor, frecuentes, scorer=fuzz.token_sort_ratio)
        if match and match[1] >= 60: reemplazos[valor] = match[0]
    df['CATALOG'] = df['CATALOG'].replace(reemplazos)

    def get_mode(x): return x.mode()[0] if not x.mode().empty else None
    mapa_cat = df.dropna(subset=['CATALOG']).groupby('PCODE')['CATALOG'].agg(get_mode).to_dict()
    df['CATALOG'] = df['CATALOG'].fillna(df['PCODE'].map(mapa_cat))

    custnum_counts = df['CUSTNUM'].value_counts()
    unicos = custnum_counts[custnum_counts == 1].index
    map_data = df[~df['CUSTNUM'].isin(unicos)]
    inv_to_cust_map = map_data.groupby('INV')['CUSTNUM'].agg(lambda x: x.value_counts().idxmax() if not x.empty else None)
    
    mask_error = df['CUSTNUM'].isin(unicos)
    nuevos = df.loc[mask_error, 'INV'].map(inv_to_cust_map)
    df.loc[mask_error, 'CUSTNUM'] = nuevos.fillna(df.loc[mask_error, 'CUSTNUM'])

    df = eliminar_duplicados_sin_id(df)
    df.dropna( inplace=True)
    df['QTY'] = df['QTY'].astype('Int64')
    df['ORIGEN'] = 'CATALOG'
    
    df.to_csv(FILES['stage_cat'], index=False)

def enriquecer_catalog_func(**kwargs):
    if not os.path.exists(FILES['stage_web']) or not os.path.exists(FILES['stage_cat']): return
    print("--- Mapeando: Web(INV) -> Cat(CUSTNUM) ---")
    
    df_web = pd.read_csv(FILES['stage_web'])
    df_cat = pd.read_csv(FILES['stage_cat'])
    
    web_ref = df_web[['INV', 'CUSTNUM']].dropna().drop_duplicates(subset=['INV'])
    
    df_merged = pd.merge(
        df_cat, web_ref, 
        left_on='CUSTNUM', right_on='INV', 
        how='left', suffixes=('', '_web')
    )
    
    df_merged['CUSTNUM'] = df_merged['CUSTNUM_web'].fillna(df_merged['CUSTNUM'])
    cols_to_drop = [c for c in df_merged.columns if '_web' in c]
    df_merged.drop(columns=cols_to_drop, inplace=True)
    
    df_merged.to_csv(FILES['stage_cat'], index=False)
    print("Catalog enriquecido correctamente.")

def integrar_func(**kwargs):
    if not os.path.exists(FILES['stage_web']) or not os.path.exists(FILES['stage_cat']): return
    df_w = pd.read_csv(FILES['stage_web'])
    df_c = pd.read_csv(FILES['stage_cat'])
    
    df_final = pd.concat([df_w.drop(columns=['ID'], errors='ignore'), 
                          df_c.drop(columns=['ID'], errors='ignore')], ignore_index=True)
    
    df_final.insert(0, 'ID', range(1, len(df_final) + 1))
    df_final.to_csv(FILES['output'], index=False)
    print(f"Generado: {FILES['output']}")

def insert_on_conflict_nothing(table, conn, keys, data_iter):
    """
    Maneja conflictos SOLO para tablas con Primary Key o Unique Constraint definidos.
    """
    conflict_cols_map = {
        'dim_date': ['date_id'],
        'dim_products': ['product_id']
    }
    
    table_name = table.table.name
    index_elements = conflict_cols_map.get(table_name)
    
    data = [dict(zip(keys, row)) for row in data_iter]
    stmt = insert(table.table).values(data)
    
    if index_elements:
        stmt = stmt.on_conflict_do_nothing(index_elements=index_elements)
    else:
        pass 
        
    conn.execute(stmt)

def load_dw_func(**kwargs):
    if not os.path.exists(FILES['output']): return
    print("Iniciando Carga al DWH...")
    
    df_orders = pd.read_csv(FILES['output'])
    df_orders['DATE'] = pd.to_datetime(df_orders['DATE'])
    
    if os.path.exists(FILES['stage_pro']):
        df_meta = pd.read_csv(FILES['stage_pro'])
        df_meta.columns = [c.upper().strip() for c in df_meta.columns]
    else:
        df_meta = pd.DataFrame(columns=['PCODE', 'DESC', 'PRICE', 'COST', 'SUPPLIER'])

    pg_hook = PostgresHook(postgres_conn_id='postgres_db')
    engine = pg_hook.get_sqlalchemy_engine()

    fechas = df_orders['DATE'].dropna().unique()
    dim_date = pd.DataFrame({'full_date': fechas})
    dim_date['date_id'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.month_name()
    dim_date['month_short'] = dim_date['full_date'].dt.strftime('%b')
    dim_date['day'] = dim_date['full_date'].dt.day
    dim_date['day_of_week'] = dim_date['full_date'].dt.dayofweek + 1
    dim_date['day_name'] = dim_date['full_date'].dt.day_name()
    dim_date['quarter'] = dim_date['full_date'].dt.quarter
    dim_date['is_weekend'] = dim_date['day_of_week'].isin([6, 7])
    
    print("Cargando DIM_DATE...")
    dim_date.to_sql('dim_date', engine, if_exists='append', index=False, method=insert_on_conflict_nothing, chunksize=1000)
    
    uprod = df_orders[['PCODE', 'CATALOG']].drop_duplicates().rename(columns={'PCODE':'product_id', 'CATALOG':'category'})
    if 'PCODE' in df_meta.columns:
        dim_products = pd.merge(uprod, df_meta, left_on='product_id', right_on='PCODE', how='left')
        cols = {'DESC': 'description', 'PRICE': 'price', 'COST': 'cost', 'SUPPLIER': 'supplier'}
        dim_products.rename(columns={k:v for k,v in cols.items() if k in dim_products.columns}, inplace=True)
    else:
        dim_products = uprod
    
    for c in ['description', 'price', 'cost', 'supplier']:
        if c not in dim_products.columns: dim_products[c] = None
    dim_products = dim_products[['product_id', 'category', 'description', 'price', 'cost', 'supplier']].drop_duplicates(subset=['product_id'])
    
    print("Cargando DIM_PRODUCTS...")
    dim_products.to_sql('dim_products', engine, if_exists='append', index=False, method=insert_on_conflict_nothing, chunksize=1000)

    dim_customers = df_orders[['CUSTNUM']].drop_duplicates().dropna().rename(columns={'CUSTNUM': 'customer_name'})
    
    print("Cargando DIM_CUSTOMERS (Verificando existentes)...")
    try:
        existing_cust = pd.read_sql("SELECT customer_name FROM dim_customers", engine)
        existing_names = set(existing_cust['customer_name'])
        dim_customers = dim_customers[~dim_customers['customer_name'].isin(existing_names)]
    except Exception as e:
        print(f"Advertencia leyendo clientes (tabla vacía o error): {e}")

    if not dim_customers.empty:
        dim_customers.to_sql('dim_customers', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f" -> Insertados {len(dim_customers)} clientes nuevos.")
    else:
        print(" -> No hay clientes nuevos.")
    fact = df_orders.copy()
    fact['date_id'] = fact['DATE'].dt.strftime('%Y%m%d').astype(int)
    fact['sucursal_id'] = fact['ORIGEN'].map({'WEB': 1, 'CATALOG': 2}).fillna(0).astype(int)
    fact = fact.rename(columns={'ID': 'transaction_id', 'INV': 'invoice_num', 'PCODE': 'product_pcode', 'QTY': 'quantity','CUSTNUM':'customer_id'})
    fact = fact[['transaction_id', 'invoice_num', 'date_id', 'product_pcode', 'sucursal_id', 'quantity','customer_id']]
    
    print("Cargando FACT_ORDERS (Verificando existentes)...")
    try:
        existing_facts = pd.read_sql("SELECT transaction_id FROM fact_orders", engine)
        existing_ids = set(existing_facts['transaction_id'])
        fact = fact[~fact['transaction_id'].isin(existing_ids)]
    except Exception as e:
        print(f"Advertencia leyendo hechos: {e}")
        
    if not fact.empty:
        fact.to_sql('fact_orders', engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f" -> Insertadas {len(fact)} ordenes nuevas.")
    else:
        print(" -> No hay ordenes nuevas.")

    print("Carga DWH Finalizada.")


default_args = {'owner': 'airflow', 'retries': 1}

with DAG(
    'etl_company_order',
    default_args=default_args,
    schedule='20,50 * * * *',
    description='ETL de ordenes de la empresa para dos sucursales Web y Catalog',
    start_date=datetime(2026, 1, 30),
    catchup=False,
    tags=['ETL', 'Final', 'DWH']
) as dag:

    t_prep_web = PythonOperator(task_id='prep_web', python_callable=prep_web_func)
    t_prep_cat = PythonOperator(task_id='prep_cat', python_callable=prep_cat_func)
    t_prep_pro = PythonOperator(task_id='prep_pro', python_callable=prep_pro_func)
    t_trans_web = PythonOperator(task_id='trans_web', python_callable=trans_web_func)
    t_trans_cat = PythonOperator(task_id='trans_cat', python_callable=trans_cat_func)
    t_enriquecer = PythonOperator(task_id='enriquecer_catalog', python_callable=enriquecer_catalog_func)
    t_int = PythonOperator(task_id='integrar', python_callable=integrar_func)
    t_load = PythonOperator(task_id='load_dwh', python_callable=load_dw_func)

    [t_prep_web, t_prep_pro] >> t_trans_web
    [t_prep_cat, t_prep_pro] >> t_trans_cat
    [t_trans_web, t_trans_cat] >> t_enriquecer >> t_int >> t_load