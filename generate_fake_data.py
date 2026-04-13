"""
Script de Geração de Dados Fake para Pipeline ETL
Gera CSVs sintéticos simulando um e-commerce.
"""

import csv
import random
import os
import logging
from datetime import datetime, timedelta
from faker import Faker

# Configuração de logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/generate_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ──────────────────────────────────────────────
# 1. CUSTOMERS - 3.000 registros
# ──────────────────────────────────────────────
def generate_customers(n=3000):
    logger.info(f"Gerando {n} clientes...")
    filepath = os.path.join(OUTPUT_DIR, "customers.csv")

    states = [
        "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
        "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
        "RS","RO","RR","SC","SP","SE","TO"
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "customer_name", "city", "state", "signup_date"])
        for i in range(1, n + 1):
            writer.writerow([
                i,
                fake.name(),
                fake.city(),
                random.choice(states),
                fake.date_between(start_date="-3y", end_date="today").isoformat()
            ])
    logger.info(f"✔ {filepath} gerado com {n} registros.")

# ──────────────────────────────────────────────
# 2. PRODUCTS - 300 registros
# ──────────────────────────────────────────────
def generate_products(n=300):
    logger.info(f"Gerando {n} produtos...")
    filepath = os.path.join(OUTPUT_DIR, "products.csv")

    categories = [
        "Eletrônicos", "Roupas", "Calçados", "Alimentos",
        "Livros", "Esportes", "Casa e Jardim", "Beleza",
        "Brinquedos", "Informática"
    ]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "price"])
        for i in range(1, n + 1):
            writer.writerow([
                i,
                fake.catch_phrase(),
                random.choice(categories),
                round(random.uniform(9.90, 2999.99), 2)
            ])
    logger.info(f"✔ {filepath} gerado com {n} registros.")

# ──────────────────────────────────────────────
# 3. ORDERS - 10.000 registros
# ──────────────────────────────────────────────
def generate_orders(n=10000, num_customers=3000):
    logger.info(f"Gerando {n} pedidos...")
    filepath = os.path.join(OUTPUT_DIR, "orders.csv")

    statuses = ["concluido", "pendente", "cancelado", "enviado"]

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "order_date", "status"])
        for i in range(1, n + 1):
            writer.writerow([
                i,
                random.randint(1, num_customers),
                fake.date_between(start_date="-2y", end_date="today").isoformat(),
                random.choice(statuses)
            ])
    logger.info(f"✔ {filepath} gerado com {n} registros.")

# ──────────────────────────────────────────────
# 4. ORDER_ITEMS - 20.000 registros
# ──────────────────────────────────────────────
def generate_order_items(n=20000, num_orders=10000, num_products=300):
    logger.info(f"Gerando {n} itens de pedido...")
    filepath = os.path.join(OUTPUT_DIR, "order_items.csv")

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_item_id", "order_id", "product_id", "quantity", "unit_price"])
        for i in range(1, n + 1):
            writer.writerow([
                i,
                random.randint(1, num_orders),
                random.randint(1, num_products),
                random.randint(1, 10),
                round(random.uniform(9.90, 2999.99), 2)
            ])
    logger.info(f"✔ {filepath} gerado com {n} registros.")

# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
if __name__ == "__main__":
    start = datetime.now()
    logger.info("=" * 50)
    logger.info("INÍCIO DA GERAÇÃO DE DADOS FAKE")
    logger.info("=" * 50)

    generate_customers()
    generate_products()
    generate_orders()
    generate_order_items()

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"Geração concluída em {elapsed:.2f}s")
    logger.info("=" * 50)
