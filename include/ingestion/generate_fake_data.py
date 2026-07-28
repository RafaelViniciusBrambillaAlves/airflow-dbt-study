"""
include/ingestion/generate_fake_data.py
----------------------------------------
Gerador de dados fake para o cenário "large" do benchmark (~N linhas em
raw_orders, default 10.000.000), usado no lugar dos seeds fixos quando a
Airflow Variable `ecommerce_dataset_size` está setada como "large".
 
POR QUE ISSO NÃO É UM SEED?
A documentação oficial do dbt (https://docs.getdbt.com/docs/build/seeds) é
explícita: seeds não devem ser usados para carregar dados brutos em volume
(ex: exports grandes de um banco de produção) - eles existem para dados de
referência pequenos, estáveis e versionados. Este módulo gera os dados e os
entrega como DataFrames para o `load_raw_data.py` carregar DIRETO nas
tabelas raw_* do warehouse, sem passar pelo `dbt seed` - simulando o papel
de uma ferramenta de ingestão (EL) real. O dbt nunca vê este script: ele só
enxerga as tabelas raw_* já povoadas, através do `source()` já declarado em
models/staging/_staging__sources.yml. O dataset pequeno original continua
sendo carregado via `dbt seed`, sem nenhuma alteração.
 
REPRODUTIBILIDADE
Faker.seed() + random.seed() fixos garantem que gerar o dataset duas vezes
com o mesmo n_orders produz exatamente o mesmo resultado - importante para
comparações de benchmark justas entre execuções diferentes.
 
NOTA SOBRE order_status
O schema.yml de stg_orders (models/staging/_staging__models.yml) só aceita
['completed', 'pending', 'cancelled'] em accepted_values. O seed pequeno
original (raw_orders.csv) tem uma linha com status 'returned', que na
verdade já viola esse teste - provavelmente um bug pré-existente no
dataset de exemplo, não corrigido aqui por não fazer parte do escopo deste
prompt. Para não introduzir falhas de teste no dataset grande, este gerador
usa apenas os 3 status aceitos pelo teste.
"""

import argparse
import random
from datetime import timedelta, datetime
from pathlib import Path

import pandas as pd
from faker import Faker

RANDOW_SEED = 42

# Categorias de produtos
CATEGORY_PRODUCT_TEMPLATES = {
    "Electronics": [
        "Wireless Mouse", "Mechanical Keyboard", "USB-C Cable", "Bluetooth Speaker",
        "Webcam HD", "Monitor 24pol", "Laptop Stand", "Power Bank 10000mAh",
        "Wireless Charger", "Headset com Cancelamento de Ruido",
    ],
    "Office": [
        "Notebook Stand", "Desk Lamp", "Cadeira Ergonomica", "Quadro Branco",
        "Grampeador", "Organizador de Mesa", "Pacote de Papel A4",
        "Tapete para Cadeira de Escritorio", "Suporte para Monitor",
        "Arquivo de Aco",
    ],
    "Lifestyle": [
        "Water Bottle", "Yoga Mat", "Mochila de Viagem", "Caneca Termica",
        "Vela Aromatica", "Oculos de Sol", "Tenis de Corrida",
        "Organizador de Mochila", "Manta para Piquenique", "Sacola Ecologica",
    ],
}

LEN_CATEGORY_PRODUCT_TEMPLATES = len(CATEGORY_PRODUCT_TEMPLATES)

# Faixa de preco (min, max) por categoria
CATEGORY_PRICE_RANGE = {
    "Electronics": (19.90, 249.90),
    "Office": (14.90, 129.90),
    "Lifestyle": (9.90, 89.90)
}

STATUS_WEIGHTS = {"completed": 0.82, "pending": 0.10, "cancelled": 0.08}
QUANTITY_WEIGHTS = {1: 0.45, 2: 0.25, 3: 0.15, 4: 0.10, 5: 0.05}
DISCOUNT_WEIGHTS = {0.00: 0.50, 0.05: 0.20, 0.10: 0.15, 0.15: 0.10, 0.20: 0.05}


def _weighted_choice(rng: random.Random, weights: dict):
    options = list(weights.keys())
    probs = list(weights.values())
    return rng.choices(options, weights = probs, k = 1)[0]


def _generate_products(rng: random.Random, n_products: int) -> pd.DataFrame:
    rows = []
    categories = list(CATEGORY_PRODUCT_TEMPLATES.keys())

    for product_id in range(1, n_products + 1):

        category = categories[(product_id - 1) % LEN_CATEGORY_PRODUCT_TEMPLATES]

        templates = CATEGORY_PRODUCT_TEMPLATES[category]
        base_name = templates[(product_id - 1) % len(templates)]
        variant = (product_id  - 1) // len(templates)
        name = base_name if variant == 0 else f"{base_name} - Modelo {variant + 1}"

        low, high = CATEGORY_PRICE_RANGE[category]
        price = round(rng.uniform(low, high), 2)

        rows.append(
            {
                "product_id": product_id,
                "product_name": name,
                "category": category,
                "unit_price": price,
            }
        )

    return pd.DataFrame(rows)


def _generate_customers(fake: Faker, rng: random.Random, n_customers: int) -> pd.DataFrame:
    rows = []
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2026, 6, 30)
    seen_emails = set()

    for customer_id in range(1, n_customers + 1):

        first_name = fake.first_name()
        last_name = fake.last_name()

        email = f"{first_name}.{last_name}@example.com".lower()
        suffix = 1
        base_email = email 
        while email in seen_emails:
            email = base_email.replace("@", f"{suffix}@")
            suffix += 1
        seen_emails.add(email)

        signup_date = start_date + timedelta(
            days = rng.randint(0, (end_date - start_date).days)
        )

        rows.append(
            {
                "customer_id": customer_id,
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "signup_date": signup_date.date().isoformat(),
            }
        )

    return pd.DataFrame(rows)


def _generate_orders(
    rng: random.Random,
    n_orders: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    n_customers = len(customers_df)

    # Nem todo cliente compra: ~75% do total de clientes concentra os
    # pedidos, com peso decrescente (poucos clientes muito ativos, muitos
    # com 1-2 pedidos) - distribuicao tipo "power law" simplificada
    active_customers = customers_df.sample(
        frac = 0.75, random_state = RANDOW_SEED
    )["customer_id"].to_list()
    customer_weights = [rng.uniform(0.1, 1.0) ** 2 for _ in active_customers]

    # Alguns produtos vendem muito mais que outros
    products_ids = products_df["product_id"].to_list()
    products_weights = [rng.uniform(0.1, 1.0) ** 2 for _ in products_ids]

    signup_lookup = dict(
        zip(
            customers_df["customer_id"],
            pd.to_datetime(customers_df["signup_date"]),
        )
    )

    max_order_date = pd.Timestamp("2026-07-01")

    rows = []

    for order_id in range(1, n_orders + 1):
        customer_id = rng.choices(active_customers, weights = customer_weights, k = 1)[0]
        product_id = rng.choices(products_ids, weights = products_weights, k = 1)[0]

        signup_date = signup_lookup[customer_id]

        earliest = signup_date + timedelta(days = 1)

        if earliest >= max_order_date:
            earliest = max_order_date - timedelta(days = 1)

        span_days = max((max_order_date - earliest).days, 1)

        order_date = earliest + timedelta(days = rng.randint(0, span_days))

        rows.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "product_id": product_id,
                "quantity": _weighted_choice(rng, QUANTITY_WEIGHTS),
                "discount_pct": _weighted_choice(rng, DISCOUNT_WEIGHTS),
                "status": _weighted_choice(rng, STATUS_WEIGHTS),
                "order_date": order_date.date().isoformat(),
            }
        )

    return pd.DataFrame(rows)


def generate_dataset(
    n_orders: int = 10_000_000,
    n_products: int = 60,
    n_customers: int | None = None,
    seed: int = RANDOW_SEED
) -> dict[str, pd.DataFrame]:
    """
    Gera o dataset fake completo (customers, products, orders) com
    integridade referencial garantida (todo customer_id/product_id em
    raw_orders existe em raw_customers/raw_products).
 
    Retorna um dict {"raw_customers": df, "raw_products": df, "raw_orders": df}
    pronto para ser carregado pelo load_raw_data.py.
    """

    Faker.seed(seed)
    random.seed(seed)
    rng = random.Random(seed)
    fake = Faker("pt_br")

    if n_customers is None:
        n_customers = max(50, n_orders // 15)

    products_df = _generate_products(rng, n_products)
    customers_df = _generate_customers(fake, rng, n_customers)
    orders_df = _generate_orders(rng, n_orders, customers_df, products_df)

    return {
        "raw_customers": customers_df,
        "raw_products": products_df,
        "raw_orders": orders_df
    }

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description = __doc__)
    parser.add_argument("--n-orders", type = int, default = 10_000_000)
    parser.add_argument("--n-products", type = int, default = 60)
    parser.add_argument("--output-dir", type = str, default = "/tmp/ecommerce_fake_data")
    args = parser.parse_args()

    dataset = generate_dataset(n_orders = args.n_orders, n_products = args.n_products)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents = True, exist_ok = True)
    for table_name, df in dataset.items():
        df.to_csv(out_dir / f"{table_name}.csv", index = False)
        print(f"{table_name}: {len(df)} linhas -> {out_dir / f'{table_name}.csv'}")
