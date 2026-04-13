"""
Pipeline ETL Batch — E-commerce com Dados Fake
Extrai CSVs, transforma e carrega em DuckDB.

Inclui:
  - Logs de execução
  - Validação de schema
  - Idempotência (DROP + CREATE)
  - Consultas analíticas
"""

import os
import logging
import duckdb
import pandas as pd
from datetime import datetime

# ──────────────────────────────────────────────
# CONFIGURAÇÃO
# ──────────────────────────────────────────────
RAW_DIR = "data/raw"
TRANSFORMED_DIR = "data/transformed"
DB_PATH = "data/ecommerce.duckdb"
LOG_DIR = "logs"

os.makedirs(TRANSFORMED_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "etl_pipeline.log")),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# SCHEMAS ESPERADOS (validação)
# ──────────────────────────────────────────────
EXPECTED_SCHEMAS = {
    "customers": ["customer_id", "customer_name", "city", "state", "signup_date"],
    "products": ["product_id", "product_name", "category", "price"],
    "orders": ["order_id", "customer_id", "order_date", "status"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
}

def validate_schema(df: pd.DataFrame, name: str) -> bool:
    """Valida se o DataFrame tem as colunas esperadas."""
    expected = EXPECTED_SCHEMAS.get(name, [])
    missing = [c for c in expected if c not in df.columns]
    if missing:
        logger.error(f"Schema inválido para {name}. Colunas faltando: {missing}")
        return False
    logger.info(f"✔ Schema validado para {name} ({len(df)} registros)")
    return True

# ══════════════════════════════════════════════
# ETAPA 1 — EXTRAÇÃO
# ══════════════════════════════════════════════
def extract():
    """Lê os CSVs da pasta data/raw/ e retorna DataFrames."""
    logger.info("─" * 50)
    logger.info("ETAPA 1: EXTRAÇÃO")
    logger.info("─" * 50)

    dfs = {}
    for name in EXPECTED_SCHEMAS:
        filepath = os.path.join(RAW_DIR, f"{name}.csv")
        if not os.path.exists(filepath):
            logger.error(f"Arquivo não encontrado: {filepath}")
            raise FileNotFoundError(filepath)
        df = pd.read_csv(filepath)
        validate_schema(df, name)
        dfs[name] = df
        logger.info(f"  Extraído: {name}.csv → {len(df)} linhas")

    return dfs

# ══════════════════════════════════════════════
# ETAPA 2 — TRANSFORMAÇÃO
# ══════════════════════════════════════════════
def transform(dfs: dict) -> dict:
    """Aplica transformações nos DataFrames."""
    logger.info("─" * 50)
    logger.info("ETAPA 2: TRANSFORMAÇÃO")
    logger.info("─" * 50)

    customers = dfs["customers"].copy()
    products = dfs["products"].copy()
    orders = dfs["orders"].copy()
    order_items = dfs["order_items"].copy()

    # --- Ajuste de tipos ---
    customers["customer_id"] = customers["customer_id"].astype(int)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"])
    customers["customer_name"] = customers["customer_name"].str.strip()
    customers["city"] = customers["city"].str.strip()
    customers["state"] = customers["state"].str.strip().str.upper()
    logger.info("  ✔ Tipos ajustados: customers")

    products["product_id"] = products["product_id"].astype(int)
    products["price"] = products["price"].astype(float)
    products["product_name"] = products["product_name"].str.strip()
    products["category"] = products["category"].str.strip()
    logger.info("  ✔ Tipos ajustados: products")

    orders["order_id"] = orders["order_id"].astype(int)
    orders["customer_id"] = orders["customer_id"].astype(int)
    orders["order_date"] = pd.to_datetime(orders["order_date"])
    orders["status"] = orders["status"].str.strip().str.lower()
    logger.info("  ✔ Tipos ajustados: orders")

    order_items["order_item_id"] = order_items["order_item_id"].astype(int)
    order_items["order_id"] = order_items["order_id"].astype(int)
    order_items["product_id"] = order_items["product_id"].astype(int)
    order_items["quantity"] = order_items["quantity"].astype(int)
    order_items["unit_price"] = order_items["unit_price"].astype(float)
    logger.info("  ✔ Tipos ajustados: order_items")

    # --- Remoção / tratamento de nulos ---
    for name, df in [("customers", customers), ("products", products),
                     ("orders", orders), ("order_items", order_items)]:
        nulls = df.isnull().sum().sum()
        if nulls > 0:
            logger.warning(f"  {name}: {nulls} valores nulos encontrados — removendo linhas")
            df.dropna(inplace=True)
        else:
            logger.info(f"  ✔ {name}: sem nulos")

    # --- Cálculo de valor total por item ---
    order_items["total_price"] = (order_items["quantity"] * order_items["unit_price"]).round(2)
    logger.info("  ✔ Coluna total_price calculada em order_items")

    # --- Junção: base consolidada para análise ---
    consolidated = (
        order_items
        .merge(orders, on="order_id", how="left")
        .merge(customers, on="customer_id", how="left")
        .merge(products, on="product_id", how="left", suffixes=("", "_product"))
    )

    # Colunas extras para análise
    consolidated["order_month"] = consolidated["order_date"].dt.to_period("M").astype(str)
    consolidated["order_year"] = consolidated["order_date"].dt.year
    logger.info(f"  ✔ Base consolidada criada: {len(consolidated)} linhas, {len(consolidated.columns)} colunas")

    # --- Salvar CSVs transformados ---
    customers.to_csv(os.path.join(TRANSFORMED_DIR, "customers_clean.csv"), index=False)
    products.to_csv(os.path.join(TRANSFORMED_DIR, "products_clean.csv"), index=False)
    orders.to_csv(os.path.join(TRANSFORMED_DIR, "orders_clean.csv"), index=False)
    order_items.to_csv(os.path.join(TRANSFORMED_DIR, "order_items_clean.csv"), index=False)
    consolidated.to_csv(os.path.join(TRANSFORMED_DIR, "consolidated.csv"), index=False)
    logger.info("  ✔ CSVs transformados salvos em data/transformed/")

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "consolidated": consolidated,
    }

# ══════════════════════════════════════════════
# ETAPA 3 — CARGA (DuckDB)
# ══════════════════════════════════════════════
def load(dfs: dict):
    """Carrega DataFrames no DuckDB (idempotente: DROP IF EXISTS + CREATE)."""
    logger.info("─" * 50)
    logger.info("ETAPA 3: CARGA (DuckDB)")
    logger.info("─" * 50)

    con = duckdb.connect(DB_PATH)

    # Criar schemas
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS treated")
    con.execute("CREATE SCHEMA IF NOT EXISTS analytics")

    # ── Tabelas BRUTAS (raw) ──
    for name in ["customers", "products", "orders", "order_items"]:
        table = f"raw.{name}"
        df_temp = dfs[name]
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_temp")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"  ✔ {table}: {count} registros")

    # ── Tabelas TRATADAS (treated) ──
    for name in ["customers", "products", "orders", "order_items"]:
        table = f"treated.{name}"
        df_temp = dfs[name]
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df_temp")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"  ✔ {table}: {count} registros")

    # ── Tabela ANALÍTICA FINAL (analytics) ──
    df_consolidated = dfs["consolidated"]
    con.execute("DROP TABLE IF EXISTS analytics.consolidated")
    con.execute("CREATE TABLE analytics.consolidated AS SELECT * FROM df_consolidated")
    count = con.execute("SELECT COUNT(*) FROM analytics.consolidated").fetchone()[0]
    logger.info(f"  ✔ analytics.consolidated: {count} registros")

    con.close()
    logger.info(f"  ✔ Banco DuckDB salvo em {DB_PATH}")

# ══════════════════════════════════════════════
# ETAPA 4 — CONSULTAS ANALÍTICAS
# ══════════════════════════════════════════════
def run_analytics():
    """Executa as 5 consultas analíticas obrigatórias."""
    logger.info("─" * 50)
    logger.info("ETAPA 4: CONSULTAS ANALÍTICAS")
    logger.info("─" * 50)

    con = duckdb.connect(DB_PATH, read_only=True)

    queries = {
        "1. Faturamento total por mês": """
            SELECT order_month,
                   ROUND(SUM(total_price), 2) AS faturamento
            FROM analytics.consolidated
            WHERE status != 'cancelado'
            GROUP BY order_month
            ORDER BY order_month
        """,
        "2. Faturamento por categoria": """
            SELECT category,
                   ROUND(SUM(total_price), 2) AS faturamento
            FROM analytics.consolidated
            WHERE status != 'cancelado'
            GROUP BY category
            ORDER BY faturamento DESC
        """,
        "3. Quantidade de pedidos por estado": """
            SELECT state,
                   COUNT(DISTINCT order_id) AS total_pedidos
            FROM analytics.consolidated
            GROUP BY state
            ORDER BY total_pedidos DESC
        """,
        "4. Ticket médio por cliente": """
            SELECT customer_id,
                   customer_name,
                   ROUND(AVG(total_price), 2) AS ticket_medio
            FROM analytics.consolidated
            WHERE status != 'cancelado'
            GROUP BY customer_id, customer_name
            ORDER BY ticket_medio DESC
            LIMIT 20
        """,
        "5. Top 10 produtos mais vendidos": """
            SELECT product_id,
                   product_name,
                   SUM(quantity) AS total_vendido,
                   ROUND(SUM(total_price), 2) AS receita_total
            FROM analytics.consolidated
            WHERE status != 'cancelado'
            GROUP BY product_id, product_name
            ORDER BY total_vendido DESC
            LIMIT 10
        """,
    }

    results = {}
    for title, sql in queries.items():
        logger.info(f"\n{'='*50}")
        logger.info(f"  {title}")
        logger.info(f"{'='*50}")
        df = con.execute(sql).fetchdf()
        print(df.to_string(index=False))
        print()
        results[title] = df

    con.close()
    return results

# ══════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════
if __name__ == "__main__":
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("  PIPELINE ETL BATCH — E-COMMERCE (DADOS FAKE)")
    logger.info(f"  Início: {start.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    try:
        raw_dfs = extract()
        transformed_dfs = transform(raw_dfs)
        load(transformed_dfs)
        run_analytics()

        elapsed = (datetime.now() - start).total_seconds()
        logger.info("=" * 60)
        logger.info(f"  PIPELINE CONCLUÍDO COM SUCESSO em {elapsed:.2f}s")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"ERRO NO PIPELINE: {e}", exc_info=True)
        raise
