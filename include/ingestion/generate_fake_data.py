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
import numpy as np
from faker import Faker

RANDOM_SEED = 42

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
    n_orders: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    seed: int = RANDOM_SEED
) -> pd.DataFrame:
    """
    Gera pedidos usando operações VETORIZADAS com NumPy.
    Isso reduz o tempo de geração de 10 milhões de linhas de ~horas para poucos segundos.
    """

    np_rng = np.random.default_rng(seed)

    # 1. Selecionar 75% dos clientes ativos e produtos 
    active_customers = customers_df.sample(
        frac = 0.75, random_state = seed
    )["customer_id"].to_numpy()

    customer_weights = np_rng.uniform(0.1, 1.0, size = len(active_customers)) ** 2
    customer_weights /= customer_weights.sum()

    products_ids = products_df["product_id"].to_numpy()
    
    products_weights = np_rng.uniform(0.1, 1.0, size = len(products_ids)) ** 2
    products_weights /= products_weights.sum()

    # 2. Geracao vetorizada de Ids
    customers_ids = np_rng.choice(active_customers, size = n_orders, p = customer_weights)
    product_ids = np_rng.choice(products_ids, size = n_orders, p = products_weights)

    # 3. Geracao Vetorizada de Datas
    customer_ids_all = customers_df["customer_id"].to_numpy()
    signup_dates_all = customers_df["signup_date"].to_numpy()

    max_customer_id = int(customer_ids_all.max())
    signup_lookup = np.empty(max_customer_id + 1, dtype = signup_dates_all.dtype)
    signup_lookup[customer_ids_all] = signup_dates_all

    signup_np = pd.to_datetime(signup_lookup[customers_ids]).to_numpy()

    max_order_date_np = np.datetime64("2026-07-01")
    earliest_np = signup_np + np.timedelta64(1, "D")

    limit_date_np = max_order_date_np - np.timedelta64(1, "D")
    earliest_np = np.where(earliest_np > limit_date_np, limit_date_np, earliest_np)

    span_days_np = ((max_order_date_np - earliest_np) / np.timedelta64(1, "D")).astype(int)
    span_days_np = np.maximum(span_days_np, 1)

    added_days_np = (np_rng.random(size = n_orders) * (span_days_np + 1)).astype(int)
    order_dates_np = earliest_np + (added_days_np * np.timedelta64(1, "D"))


    # 4. Geracao Vetorizada de Quantidade, Desconto e Status
    quantities = np_rng.choice(
        list(QUANTITY_WEIGHTS.keys()),
        size = n_orders,
        p = list(QUANTITY_WEIGHTS.values())
    )

    discounts = np_rng.choice(
        list(DISCOUNT_WEIGHTS.keys()),
        size = n_orders,
        p = list(DISCOUNT_WEIGHTS.values())
    )

    statuses = np_rng.choice(
        list(STATUS_WEIGHTS.keys()),
        size = n_orders,
        p = list(STATUS_WEIGHTS.values())
    )

    # `astype(str)` direto no numpy datetime64 evita instanciar um
    # DatetimeIndex do pandas só para formatar - mais rápido para 10M linhas
    # e produz o mesmo formato YYYY-MM-DD que `.strftime('%Y-%m-%d')`
    order_dates_str = order_dates_np.astype("datetime64[D]").astype(str)

    # 5. Monta o DataFrame de uma só vez (sem append em loops)
    
    columns = {
        "order_id": np.arange(1, n_orders + 1),
        "customer_id": customers_ids,
        "product_id": product_ids,
        "quantity": quantities,
        "discount_pct": discounts,
        "status": statuses,
        "order_date": order_dates_str,
    }

    lengths = {name: len(arr) for name, arr in columns.items()}
    
    if len(set(lengths.values())) != 1:
        raise ValueError(
            f"Shape mismatch ao montar raw_orders (esperado {n_orders} em "
            f"todas as colunas): {lengths}"
        )
    
    return pd.DataFrame(columns)




def generate_dataset(
    n_orders: int = 10_000_000,
    n_products: int = 60,
    n_customers: int | None = None,
    seed: int = RANDOM_SEED,
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
    orders_df = _generate_orders(n_orders, customers_df, products_df, seed)

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
