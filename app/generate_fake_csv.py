from faker import Faker
import csv
import random
from pathlib import Path

fake = Faker("pt_BR")

path = Path("data/raw/orders.csv")
path.parent.mkdir(parents=True, exist_ok=True)

with open(path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "nome", "email", "valor"])

    for i in range(10000):
        writer.writerow([
            i,
            fake.name(),
            fake.email(),
            round(random.uniform(10, 500), 2)
        ])

print("CSV gerado")