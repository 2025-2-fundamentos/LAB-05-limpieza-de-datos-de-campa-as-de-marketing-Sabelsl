"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import os
import glob
import zipfile
import pandas as pd


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """
    
    ruta_entrada = "files/input/"
    ruta_salida = "files/output/"
    os.makedirs(ruta_salida, exist_ok=True)

    # --- Cargar todos los CSV comprimidos ---
    archivos_zip = glob.glob(os.path.join(ruta_entrada, "*.csv.zip"))
    lista_df = []

    for ruta_zip in archivos_zip:
        with zipfile.ZipFile(ruta_zip, "r") as archivo_zip:
            nombres_csv = [nombre for nombre in archivo_zip.namelist() if nombre.endswith(".csv")]
            for nombre_csv in nombres_csv:
                with archivo_zip.open(nombre_csv) as archivo_csv:
                    lista_df.append(pd.read_csv(archivo_csv))

    # Unir todos los archivos en un solo DataFrame
    datos = pd.concat(lista_df, ignore_index=True)

    # 1. Construcción del archivo client.csv
    df_clientes = datos[[
        "client_id", "age", "job", "marital", "education",
        "credit_default", "mortgage"
    ]].copy()

    # Limpieza de columnas categóricas
    df_clientes["job"] = (
        df_clientes["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    df_clientes["education"] = (
        df_clientes["education"]
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )

    # Conversión yes/no -> 1/0
    df_clientes["credit_default"] = df_clientes["credit_default"].apply(
        lambda valor: 1 if str(valor).lower() == "yes" else 0
    )
    df_clientes["mortgage"] = df_clientes["mortgage"].apply(
        lambda valor: 1 if str(valor).lower() == "yes" else 0
    )

    df_clientes.to_csv(os.path.join(ruta_salida, "client.csv"), index=False)

    # 2. Construcción del archivo campaign.csv
    df_campania = datos[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "day", "month"
    ]].copy()

    # Codificación de outcomes
    df_campania["previous_outcome"] = df_campania["previous_outcome"].apply(
        lambda valor: 1 if str(valor).lower() == "success" else 0
    )
    df_campania["campaign_outcome"] = df_campania["campaign_outcome"].apply(
        lambda valor: 1 if str(valor).lower() == "yes" else 0
    )

    # Conversión day + month → fecha YYYY-MM-DD
    mapa_meses = {
        "jan": "01", "feb": "02", "mar": "03", "apr": "04",
        "may": "05", "jun": "06", "jul": "07", "aug": "08",
        "sep": "09", "oct": "10", "nov": "11", "dec": "12"
    }

    df_campania["month"] = df_campania["month"].str.lower().map(mapa_meses)
    df_campania["day"] = df_campania["day"].astype(int).astype(str).str.zfill(2)

    df_campania["last_contact_date"] = (
        "2022-" + df_campania["month"] + "-" + df_campania["day"]
    )

    df_campania.drop(columns=["day", "month"], inplace=True)
    df_campania.to_csv(os.path.join(ruta_salida, "campaign.csv"), index=False)

    # 3. Construcción del archivo economics.csv
    df_economia = datos[[
        "client_id", "cons_price_idx", "euribor_three_months"
    ]].copy()

    df_economia.to_csv(os.path.join(ruta_salida, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()