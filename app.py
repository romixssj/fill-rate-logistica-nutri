"""
Dashboard de Control Logístico - Lácteos San Antonio C.A.
Calcula el Fill Rate cruzando Órdenes de Compra con Facturación.

Fuentes de datos soportadas:
    - Facturación: XLSX o CSV con columnas 'Cantidad facturada' y referencia
                                 (preferido: 'Referencia del cliente'; fallback: 'Material')
  - Orden de Compra: HTML (El Rosado, tabla #AutoNumber2) o
                     XLS/XLSX (formato SMX con filas tipo '1','2','3')
"""

import io
import re
import unicodedata
import streamlit as st
import pandas as pd
from bs4 import BeautifulSoup
import requests

# ──────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Fill Rate Dashboard · Lácteos San Antonio",
    page_icon="🥛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Paleta de colores corporativa
st.markdown(
    """
    <style>
      [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 700; }
      .stDataFrame { border-radius: 8px; overflow: hidden; }
      .block-container { padding-top: 1.5rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def normalize_code(code: str) -> str:
    """
    Normaliza un código de material eliminando ceros a la izquierda
    y espacios, para que '000000000060000310' y '60000310' sean iguales.
    """
    return str(code).strip().lstrip("0") or "0"


def to_float_safe(value, default: float = 0.0) -> float:
    """Convierte un valor a float sin lanzar excepción."""
    try:
        # Las celdas SMX usan coma como separador decimal en algunas versiones
        return float(str(value).replace(",", ".").strip())
    except (ValueError, TypeError):
        return default


def normalize_text(value: str) -> str:
    """Normaliza texto para comparaciones flexibles de encabezados."""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", "", text.lower())


def detect_column(columns, candidates):
    """Encuentra una columna usando nombres equivalentes normalizados."""
    normalized_map = {normalize_text(col): col for col in columns}
    for candidate in candidates:
        match = normalized_map.get(normalize_text(candidate))
        if match:
            return match
    return None


def normalize_order_number(value: str) -> str:
    """Normaliza el número de orden para comparar archivos distintos."""
    text = str(value).strip()
    if text.lower() in ("", "nan", "none"):
        return ""
    text = text.upper()
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^A-Z0-9-]", "", text)
    return text


def normalize_reference(value: str) -> str:
    """Normaliza referencias de cliente/material para el cruce."""
    text = str(value).strip().upper()
    if text.lower() in ("", "nan", "none"):
        return ""
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"[^A-Z0-9-]", "", text)
    if text.isdigit():
        return text.lstrip("0") or "0"
    return text


def extract_order_number(text: str = "", filename: str = "") -> str:
    """Busca un número de orden en texto libre o en el nombre del archivo."""
    search_targets = [text or "", filename or ""]
    patterns = [
        r"(?:orden(?:decompra)?|ordendecompra|pedido|oc|nro|numero)\D{0,20}([A-Z0-9-]{4,})",
        r"\b([0-9]{5,})\b",
    ]

    for target in search_targets:
        compact_target = normalize_text(target)
        for pattern in patterns:
            match = re.search(pattern, compact_target, re.IGNORECASE)
            if match:
                return normalize_order_number(match.group(1))

        raw_matches = re.findall(r"[A-Z0-9-]{5,}", str(target).upper())
        if raw_matches:
            return normalize_order_number(raw_matches[0])

    return ""


def aggregate_order_lines(df: pd.DataFrame, quantity_column: str) -> pd.DataFrame:
    """Consolida líneas repetidas por orden y material."""
    if df.empty:
        return df

    agg_map = {quantity_column: "sum"}
    if "Referencia_Cruce" in df.columns:
        agg_map["Referencia_Cruce"] = "first"
    if "Material_Ref" in df.columns:
        agg_map["Material_Ref"] = "first"
    if "Descripcion_OC" in df.columns:
        agg_map["Descripcion_OC"] = "first"
    if "Descripcion_Fact" in df.columns:
        agg_map["Descripcion_Fact"] = "first"
    if "Cajas_Solicitadas" in df.columns:
        agg_map["Cajas_Solicitadas"] = "sum"
    if "UXC" in df.columns:
        agg_map["UXC"] = "max"
    if "Unidades_Solicitadas" in df.columns:
        agg_map["Unidades_Solicitadas"] = "sum"

    return (
        df.groupby(["Numero_Orden", "Referencia_Cruce_Norm"], as_index=False)
        .agg(agg_map)
    )


def validate_order_sets(
    df_oc: pd.DataFrame,
    df_fact: pd.DataFrame,
    manual_order_number: str,
    strict_mode: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Valida órdenes y alinea ambos lados al conjunto común cuando hay ruido."""
    manual_order_number = normalize_order_number(manual_order_number)

    if manual_order_number:
        df_oc["Numero_Orden"] = df_oc["Numero_Orden"].replace("", manual_order_number)
        df_fact["Numero_Orden"] = df_fact["Numero_Orden"].replace("", manual_order_number)

    missing_oc = sorted({value for value in df_oc["Numero_Orden"].astype(str) if not value.strip()})
    missing_fact = sorted({value for value in df_fact["Numero_Orden"].astype(str) if not value.strip()})

    if missing_oc or missing_fact:
        raise ValueError(
            "❌ No se pudo identificar el número de orden en todos los archivos. "
            "Agrega una columna de orden en facturación, usa nombres de archivo con la orden o completa el campo manual en la barra lateral."
        )

    missing_ref_oc = (df_oc["Referencia_Cruce_Norm"].astype(str).str.strip() == "").any()
    missing_ref_fact = (df_fact["Referencia_Cruce_Norm"].astype(str).str.strip() == "").any()
    if missing_ref_oc or missing_ref_fact:
        raise ValueError(
            "❌ Hay referencias vacías en los archivos. El cruce exige número de orden + referencia del cliente/material. "
            "Revisa la columna de referencia en facturación y las referencias en la orden de compra."
        )

    oc_orders = set(df_oc["Numero_Orden"].astype(str))
    fact_orders = set(df_fact["Numero_Orden"].astype(str))

    common_orders = sorted(oc_orders & fact_orders)
    if not common_orders:
        raise ValueError(
            "❌ No hay números de orden en común entre OC y facturación. "
            f"OC detectadas: {sorted(oc_orders)} · Facturación detectadas: {sorted(fact_orders)}"
        )

    warning_msg = ""

    if oc_orders != fact_orders:
        only_oc = sorted(oc_orders - fact_orders)
        only_fact = sorted(fact_orders - oc_orders)
        if strict_mode:
            raise ValueError(
                "❌ Modo estricto activo: los números de orden no coinciden entre OC y facturación. "
                f"Solo en OC: {only_oc or 'ninguno'} · Solo en Facturación: {only_fact or 'ninguno'}"
            )
        warning_msg = (
            "⚠️ Se detectaron órdenes extra y se filtraron automáticamente al cruce común. "
            f"Solo en OC: {only_oc or 'ninguno'} · Solo en Facturación: {only_fact or 'ninguno'}"
        )

    df_oc = df_oc[df_oc["Numero_Orden"].isin(common_orders)].copy()
    df_fact = df_fact[df_fact["Numero_Orden"].isin(common_orders)].copy()

    return df_oc, df_fact, warning_msg


# ──────────────────────────────────────────────────────────────────────────────
# ETL – ORDEN DE COMPRA HTML (Corporación El Rosado)
# ──────────────────────────────────────────────────────────────────────────────

def parse_html_order(raw_bytes: bytes, filename: str = "") -> pd.DataFrame:
    """
    Lee el HTML de Corporación El Rosado y extrae los ítems de la tabla
    con id='AutoNumber2'.

    Columnas relevantes (índice 0-based dentro de la fila <tr>).
    NOTA: BeautifulSoup NO expande colspan, por lo que cada celda con
    colspan="3" sigue contando como índice 1. El orden real observado:
      Col 0  → ITEN  (número de ítem, dígito para filtrar filas reales)
      Col 1  → ARTICULO (código EAN largo del cliente)
      Col 2  → DESCRIPCION (con colspan=3 en el HTML, pero índice único)
      Col 3  → REFERENCIA (código del proveedor = Material SAP)
      Col 4  → TAMAÑO
      Col 5  → UXC  (unidades por caja)
      Col 6  → CANTIDAD (cajas pedidas)
      Col 7  → COSTO
      Col 8  → DESCTO 1
      Col 9  → DESCTO 2
    """
    soup = BeautifulSoup(raw_bytes, "html.parser")
    tabla = soup.find("table", {"id": "AutoNumber2"})
    order_number = ""

    # Extrae explícitamente la fila de "NUMERO DE ORDEN" cuando existe.
    for fila in soup.find_all("tr"):
        cells_text = [cell.get_text(" ", strip=True).upper() for cell in fila.find_all(["td", "th"])]
        if any("NUMERO DE ORDEN" in text for text in cells_text):
            full_row_text = " ".join(cells_text)
            match = re.search(r"NUMERO\s*DE\s*ORDEN\D*([A-Z0-9-]{5,})", full_row_text)
            if match:
                order_number = normalize_order_number(match.group(1))
                break

    if not order_number:
        order_number = extract_order_number(soup.get_text(" ", strip=True), filename)

    if tabla is None:
        raise ValueError(
            "❌ No se encontró la tabla con id='AutoNumber2' en el HTML. "
            "Verifica que estás subiendo la orden de Corporación El Rosado."
        )

    items = []
    for fila in tabla.find_all("tr"):
        celdas = fila.find_all(["td", "th"])

        # Ignorar filas con menos columnas de las esperadas
        if len(celdas) < 7:
            continue

        iten_text = celdas[0].get_text(strip=True).replace("\xa0", "")

        # Solo procesar filas donde ITEN sea un número (ej. 10, 20, 30…)
        if not iten_text.isdigit():
            continue

        try:
            referencia    = celdas[3].get_text(strip=True).replace("\xa0", "")
            descripcion   = celdas[2].get_text(strip=True)
            uxc           = to_float_safe(celdas[5].get_text(strip=True).replace("\xa0", ""))
            cant_cajas    = to_float_safe(celdas[6].get_text(strip=True).replace("\xa0", ""))

            if uxc <= 0 or cant_cajas <= 0 or not referencia:
                continue

            items.append(
                {
                    "Numero_Orden":         order_number,
                    "Referencia_Cruce":     referencia,
                    "Referencia_Cruce_Norm": normalize_reference(referencia),
                    "Material_Ref":          referencia,
                    "Material_Norm":         normalize_code(referencia),
                    "Descripcion_OC":        descripcion,
                    "Cajas_Solicitadas":     cant_cajas,
                    "UXC":                   uxc,
                    "Unidades_Solicitadas":  uxc * cant_cajas,
                }
            )
        except Exception:
            continue  # Fila mal formada, se omite

    if not items:
        raise ValueError(
            "❌ Se encontró la tabla, pero no contiene ítems válidos. "
            "Revisa el formato del HTML."
        )

    return pd.DataFrame(items)


# ──────────────────────────────────────────────────────────────────────────────
# ETL – ORDEN DE COMPRA XLS/XLSX (Formato SMX)
# ──────────────────────────────────────────────────────────────────────────────

def parse_smx_order(raw_bytes: bytes, filename: str = "") -> pd.DataFrame:
    """
    Lee el archivo XLS/XLSX en formato SMX (Supermaxi/El Rosado).

    Estructura de filas (columna 0 = tipo de registro):
      'R' → encabezado de sección
      '1' → cabecera del pedido (proveedor, fecha, etc.)
      '2' → línea de ítem del pedido  ← las que nos interesan
      '3' → totales del pedido

    Columnas en filas tipo '2' (índice 0-based):
      Col  2 → DESCRIPCION ARTICULO
      Col  3 → CODIGO REFER  (referencia del proveedor = Material SAP)
      Col  8 → UNMA          (unidades por caja / UXC)
      Col 18 → CANTID        (cantidad de cajas pedidas)
    """
    try:
        df_raw = pd.read_excel(io.BytesIO(raw_bytes), header=None, dtype=str)
    except Exception as e:
        raise ValueError(f"❌ No se pudo leer el archivo Excel: {e}")

    preview_text = " ".join(df_raw.head(20).fillna("").astype(str).values.flatten())
    fallback_order_number = extract_order_number(preview_text, filename)

    # Filtrar filas de ítem (tipo '2')
    mask_items = df_raw.iloc[:, 0].astype(str).str.strip() == "2"
    item_rows = df_raw[mask_items]

    if item_rows.empty:
        raise ValueError(
            "❌ No se encontraron filas de tipo '2' en el archivo SMX. "
            "Verifica que estás subiendo la orden de compra correcta."
        )

    items = []
    for _, fila in item_rows.iterrows():
        try:
            referencia   = str(fila.iloc[3]).strip()
            descripcion  = str(fila.iloc[2]).strip()
            order_number = normalize_order_number(str(fila.iloc[21])) if len(fila) > 21 else ""
            order_number = order_number or fallback_order_number
            # UNMA viene como '0012' → 12 ; '0008' → 8
            uxc          = to_float_safe(str(fila.iloc[8]).lstrip("0") or "1")
            cant_cajas   = to_float_safe(str(fila.iloc[18]).lstrip("0") or "0")

            if not referencia or referencia in ("nan", "None"):
                continue
            if uxc <= 0:
                uxc = 1  # Salvaguarda

            items.append(
                {
                    "Numero_Orden":         order_number,
                    "Referencia_Cruce":     referencia,
                    "Referencia_Cruce_Norm": normalize_reference(referencia),
                    "Material_Ref":          referencia,
                    "Material_Norm":         normalize_code(referencia),
                    "Descripcion_OC":        descripcion,
                    "Cajas_Solicitadas":     cant_cajas,
                    "UXC":                   uxc,
                    "Unidades_Solicitadas":  uxc * cant_cajas,
                }
            )
        except Exception:
            continue

    if not items:
        raise ValueError("❌ No se pudieron extraer ítems válidos del archivo SMX.")

    return pd.DataFrame(items)


# ──────────────────────────────────────────────────────────────────────────────
# ETL – FACTURACIÓN (XLSX / CSV)
# ──────────────────────────────────────────────────────────────────────────────

def parse_billing(raw_bytes: bytes, filename: str, fallback_order_number: str = "") -> pd.DataFrame:
    """
    Carga el reporte de facturación y lo agrupa por código de material.

    Columnas requeridas: 'Cantidad facturada' y referencia
    (preferido 'Referencia del cliente', fallback 'Material')
    Columna opcional:    'Descripcion'
    """
    try:
        if filename.lower().endswith(".csv"):
            # Intentar UTF-8, luego latin-1 como fallback
            try:
                df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str)
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str, encoding="latin-1")
        else:
            df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
    except Exception as e:
        raise ValueError(f"❌ No se pudo leer el archivo de facturación: {e}")

    # Verificar columnas requeridas
    required = {"Cantidad facturada"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"❌ El archivo de facturación no contiene las columnas: {missing}. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    ref_column = detect_column(
        df.columns,
        [
            "Referencia del cliente",
            "Referencia cliente",
            "Ref cliente",
            "Cod cliente",
            "Codigo cliente",
            "Referencia",
            "Material",
        ],
    )
    if not ref_column:
        raise ValueError(
            "❌ El archivo de facturación debe incluir una columna de referencia "
            "(ideal: 'Referencia del cliente'; también se acepta 'Material')."
        )

    # Convertir cantidad a numérico
    df["Cantidad facturada"] = df["Cantidad facturada"].apply(
        lambda x: to_float_safe(str(x).replace(",", "."))
    )

    # Referencia de cruce (prioridad a referencia del cliente)
    df["Referencia_Cruce"] = df[ref_column].astype(str).str.strip()
    df["Referencia_Cruce_Norm"] = df["Referencia_Cruce"].apply(normalize_reference)

    # Detectar número de orden
    order_column = detect_column(
        df.columns,
        [
            "Orden de compra",
            "Orden",
            "OC",
            "Pedido",
            "Numero de orden",
            "Nro Orden",
            "Documento",
            "Referencia de cliente",
            "Referencia cliente",
        ],
    )
    file_order_number = extract_order_number(filename=filename)
    df["Numero_Orden"] = ""
    if order_column:
        df["Numero_Orden"] = df[order_column].apply(normalize_order_number)

    df["Numero_Orden"] = df["Numero_Orden"].replace("", file_order_number)
    df["Numero_Orden"] = df["Numero_Orden"].replace("", normalize_order_number(fallback_order_number))

    # Limpieza mínima de datos sucios: ignora filas sin cantidad/referencia.
    df = df[df["Cantidad facturada"] > 0].copy()
    df = df[df["Referencia_Cruce_Norm"].astype(str).str.strip() != ""].copy()

    # Agregar descripción si existe
    agg_dict = {"Cantidad facturada": "sum"}
    if "Descripcion" in df.columns:
        agg_dict["Descripcion"] = "first"

    agg_dict["Referencia_Cruce"] = "first"
    grouped = df.groupby(["Numero_Orden", "Referencia_Cruce_Norm"]).agg(agg_dict).reset_index()
    grouped.rename(columns={"Cantidad facturada": "Unidades_Facturadas"}, inplace=True)

    if "Descripcion" in grouped.columns:
        grouped.rename(columns={"Descripcion": "Descripcion_Fact"}, inplace=True)

    return grouped


# ──────────────────────────────────────────────────────────────────────────────
# CRUCE Y CÁLCULO DE FILL RATE
# ──────────────────────────────────────────────────────────────────────────────

MOTIVOS = [
    "",
    "Quiebre de Stock",
    "Producción Insuficiente",
    "Avería en Transporte",
    "Error en Picking",
    "Rechazo de Calidad",
    "Problema de Temperatura",
    "Retraso del Proveedor",
    "Diferencia de Precio / Condición",
    "Pedido Fuera de Ventana Horaria",
    "Otro",
]


def compute_fill_rate(df_oc: pd.DataFrame, df_fact: pd.DataFrame) -> pd.DataFrame:
    """
    Une la orden de compra con la facturación y calcula:
      - Unidades_Facturadas  (0 si no hay match)
      - Fill_Rate_%          (facturado / solicitado × 100, tope 100 %)
      - Brecha_Unidades      (solicitado – facturado, mínimo 0)
    """
    merged = df_oc.merge(
        df_fact,
        on=["Numero_Orden", "Referencia_Cruce_Norm"],
        how="left",
        suffixes=("_OC", "_FACT"),
    )
    merged["Unidades_Facturadas"] = merged["Unidades_Facturadas"].fillna(0)

    if "Referencia_Cruce_OC" in merged.columns and "Referencia_Cruce_FACT" in merged.columns:
        merged["Referencia_Cruce"] = merged["Referencia_Cruce_OC"].fillna(merged["Referencia_Cruce_FACT"])
    elif "Referencia_Cruce_OC" in merged.columns:
        merged["Referencia_Cruce"] = merged["Referencia_Cruce_OC"]
    elif "Referencia_Cruce_FACT" in merged.columns:
        merged["Referencia_Cruce"] = merged["Referencia_Cruce_FACT"]

    merged["Fill_Rate_%"] = (
        merged["Unidades_Facturadas"] / merged["Unidades_Solicitadas"] * 100
    ).round(2).clip(upper=100.0)

    merged["Brecha_Unidades"] = (
        merged["Unidades_Solicitadas"] - merged["Unidades_Facturadas"]
    ).clip(lower=0)

    # Cumplimiento a nivel de orden de compra (cajas)
    merged["Cajas_Facturadas_Estimadas"] = (
        merged["Unidades_Facturadas"] / merged["UXC"].replace(0, pd.NA)
    ).fillna(0)
    merged["Cumplimiento_OC_%"] = (
        merged["Cajas_Facturadas_Estimadas"] / merged["Cajas_Solicitadas"].replace(0, pd.NA) * 100
    ).round(2).fillna(0).clip(upper=100.0)

    # Columnas editables vacías
    merged["Motivo de Falta"]    = ""
    merged["Acción Correctiva"]  = ""

    return merged


def highlight_fill_rate(val):
    """Color para la columna Fill Rate en la tabla de resumen."""
    if pd.isna(val):
        return ""
    if val < 70:
        return "color: #c0392b; font-weight: bold"
    if val < 95:
        return "color: #e67e22; font-weight: bold"
    return "color: #27ae60; font-weight: bold"


def upload_to_apps_script(
    df_export: pd.DataFrame,
    webapp_url: str,
    token: str,
    worksheet_name: str,
) -> None:
    """Sube el DataFrame a Google Sheets via Apps Script Web App."""
    rows = [df_export.columns.tolist()] + df_export.fillna("").astype(str).values.tolist()

    payload = {
        "token": token,
        "sheet_name": worksheet_name,
        "rows": rows,
    }

    response = requests.post(webapp_url, json=payload, timeout=60)
    response.raise_for_status()

    try:
        data = response.json()
    except ValueError:
        raise ValueError(f"Respuesta no valida del Apps Script: {response.text}")

    if not data.get("ok"):
        raise ValueError(data.get("error", "Error desconocido al subir a Apps Script."))


def get_apps_script_config() -> tuple[str, str, str]:
    """Obtiene la configuracion del webhook desde Streamlit secrets."""
    webapp_url = st.secrets.get("APPS_SCRIPT_URL", "")
    webhook_token = st.secrets.get("APPS_SCRIPT_TOKEN", "")
    worksheet_name = st.secrets.get("APPS_SCRIPT_SHEET_NAME", "Reporte Fill Rate")
    return webapp_url, webhook_token, worksheet_name


def make_export_dataframe(df_result: pd.DataFrame) -> pd.DataFrame:
    """Prepara el DataFrame final que se enviara a Google Sheets."""
    export_cols = [
        "Numero_Orden",
        "Referencia_Cruce",
        "Material_Ref",
        "Descripcion_OC",
        "Cajas_Solicitadas",
        "UXC",
        "Unidades_Solicitadas",
        "Unidades_Facturadas",
        "Fill_Rate_%",
        "Cumplimiento_OC_%",
        "Brecha_Unidades",
        "Motivo de Falta",
        "Acción Correctiva",
    ]
    df_export = df_result[export_cols].copy()
    df_export.rename(
        columns={
            "Numero_Orden": "Número Orden",
            "Referencia_Cruce": "Referencia Cliente",
            "Material_Ref": "Código Material",
            "Descripcion_OC": "Descripción",
            "Cajas_Solicitadas": "Cajas OC",
            "Unidades_Solicitadas": "Unidades Solicitadas",
            "Unidades_Facturadas": "Unidades Facturadas",
            "Fill_Rate_%": "Fill Rate %",
            "Cumplimiento_OC_%": "% Cumplimiento OC",
            "Brecha_Unidades": "Brecha (uds)",
        },
        inplace=True,
    )
    return df_export


# ──────────────────────────────────────────────────────────────────────────────
# INTERFAZ STREAMLIT
# ──────────────────────────────────────────────────────────────────────────────

def main():
    # ── Encabezado ──────────────────────────────────────────────────────────
    st.title("🥛 Dashboard de Control Logístico")
    st.markdown(
        "**Lácteos San Antonio C.A.** · Análisis de Nivel de Servicio (Fill Rate)"
    )
    st.divider()

    # ── Sidebar – carga de archivos ─────────────────────────────────────────
    with st.sidebar:
        st.header("📂 Cargar Archivos")
        st.caption("Puedes subir uno o varios archivos por cada fuente.")

        match_mode = st.selectbox(
            "Modo de validación de órdenes",
            ["Flexible (recomendado)", "Estricto"],
            help=(
                "Flexible: filtra automáticamente al conjunto común de órdenes. "
                "Estricto: detiene el análisis si no coinciden exactamente."
            ),
        )
        strict_mode = match_mode == "Estricto"

        manual_order_number = st.text_input(
            "Número de orden manual (opcional)",
            help="Úsalo si algún archivo no trae el número de orden o quieres forzar una validación única.",
        )

        archivos_fact = st.file_uploader(
            "1️⃣  Reporte de Facturación",
            type=["xlsx", "xls", "csv"],
            accept_multiple_files=True,
            help=(
                "Debe contener 'Cantidad facturada' y una referencia para cruce "
                "(ideal: 'Referencia del cliente'; fallback: 'Material')."
            ),
        )

        archivos_oc = st.file_uploader(
            "2️⃣  Orden de Compra",
            type=["html", "htm", "xlsx", "xls"],
            accept_multiple_files=True,
            help=(
                "Acepta:\n"
                "• HTML de Corporación El Rosado (tabla #AutoNumber2)\n"
                "• Excel SMX (filas tipo 1 / 2 / 3)"
            ),
        )

        st.divider()
        st.markdown(
            "**Semáforo Fill Rate**\n\n"
            "🟢 ≥ 95 %  Óptimo\n\n"
            "🟡 70–94 %  Alerta\n\n"
            "🔴 < 70 %  Crítico"
        )

    # ── Procesamiento ────────────────────────────────────────────────────────
    if not archivos_fact or not archivos_oc:
        st.info(
            "👈 Por favor, sube uno o varios archivos de **Reporte de Facturación** y "
            "**Orden de Compra** en el panel lateral para comenzar."
        )
        st.stop()

    # Leer y parsear facturación
    with st.spinner("Procesando facturación…"):
        try:
            df_fact = pd.concat(
                [
                    parse_billing(archivo.read(), archivo.name, manual_order_number)
                    for archivo in archivos_fact
                ],
                ignore_index=True,
            )
            df_fact = aggregate_order_lines(df_fact, "Unidades_Facturadas")
        except ValueError as e:
            st.error(str(e))
            st.stop()

    # Leer y parsear orden de compra
    with st.spinner("Procesando orden de compra…"):
        try:
            df_oc = pd.concat(
                [
                    parse_html_order(archivo.read(), archivo.name)
                    if archivo.name.lower().endswith((".html", ".htm"))
                    else parse_smx_order(archivo.read(), archivo.name)
                    for archivo in archivos_oc
                ],
                ignore_index=True,
            )
            df_oc = aggregate_order_lines(df_oc, "Unidades_Solicitadas")
        except ValueError as e:
            st.error(str(e))
            st.stop()

    try:
        df_oc, df_fact, order_warning_msg = validate_order_sets(
            df_oc,
            df_fact,
            manual_order_number,
            strict_mode,
        )
    except ValueError as e:
        st.error(str(e))
        st.stop()

    if order_warning_msg:
        st.warning(order_warning_msg)

    # Cruce y Fill Rate
    df_result = compute_fill_rate(df_oc, df_fact)

    st.subheader("🧾 Validación de Órdenes")
    st.caption("El análisis se cruza por número de orden y referencia del cliente/material. Solo se aceptan órdenes presentes en ambos lados.")
    st.dataframe(
        pd.DataFrame(
            {
                "Número Orden": sorted(df_result["Numero_Orden"].unique()),
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    order_options = ["Todas"] + sorted(df_result["Numero_Orden"].unique())
    selected_order = st.selectbox("Filtrar análisis por número de orden", order_options)
    df_view = df_result if selected_order == "Todas" else df_result[df_result["Numero_Orden"] == selected_order].copy()

    summary_by_order = (
        df_result.groupby("Numero_Orden", as_index=False)
        .agg(
            Unidades_Solicitadas=("Unidades_Solicitadas", "sum"),
            Unidades_Facturadas=("Unidades_Facturadas", "sum"),
            Cajas_Solicitadas=("Cajas_Solicitadas", "sum"),
            Cajas_Facturadas_Estimadas=("Cajas_Facturadas_Estimadas", "sum"),
            Ítems=("Referencia_Cruce_Norm", "count"),
        )
    )
    summary_by_order["Fill Rate %"] = (
        summary_by_order["Unidades_Facturadas"] / summary_by_order["Unidades_Solicitadas"] * 100
    ).round(2).fillna(0).clip(upper=100.0)
    summary_by_order["% Cumplimiento OC"] = (
        summary_by_order["Cajas_Facturadas_Estimadas"] / summary_by_order["Cajas_Solicitadas"] * 100
    ).round(2).fillna(0).clip(upper=100.0)

    st.dataframe(
        summary_by_order[["Numero_Orden", "Ítems", "Fill Rate %", "% Cumplimiento OC"]],
        use_container_width=True,
        hide_index=True,
    )

    # ── KPIs globales ────────────────────────────────────────────────────────
    total_solicitado  = df_view["Unidades_Solicitadas"].sum()
    total_facturado   = df_view["Unidades_Facturadas"].sum()
    fill_global       = (total_facturado / total_solicitado * 100) if total_solicitado > 0 else 0
    total_cajas_oc    = df_view["Cajas_Solicitadas"].sum()
    total_cajas_fact  = df_view["Cajas_Facturadas_Estimadas"].sum()
    fill_global_oc    = (total_cajas_fact / total_cajas_oc * 100) if total_cajas_oc > 0 else 0
    items_con_brecha  = (df_view["Brecha_Unidades"] > 0).sum()
    items_totales     = len(df_view)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Fill Rate Global", f"{fill_global:.1f} %")
    col2.metric("Cumplimiento OC (Cajas)", f"{fill_global_oc:.1f} %")
    col3.metric("Unidades Solicitadas", f"{int(total_solicitado):,}")
    col4.metric("Unidades Facturadas", f"{int(total_facturado):,}")

    col5, _ = st.columns([1, 3])
    col5.metric(
        "Ítems con Brecha",
        f"{items_con_brecha} / {items_totales}",
        delta=f"-{int(total_solicitado - total_facturado):,} uds" if items_con_brecha else None,
        delta_color="inverse",
    )

    st.divider()

    # ── Tabla interactiva ────────────────────────────────────────────────────
    st.subheader("📋 Análisis Detallado por Ítem")
    st.caption(
        "Completa las columnas **Motivo de Falta** y **Acción Correctiva** "
        "para los ítems con brecha. Los cambios se incluyen en el reporte enviado a Google Sheets."
    )

    # Columnas visibles en el editor (reordenadas para legibilidad)
    cols_display = [
        "Numero_Orden",
        "Referencia_Cruce",
        "Material_Ref",
        "Descripcion_OC",
        "Cajas_Solicitadas",
        "UXC",
        "Unidades_Solicitadas",
        "Unidades_Facturadas",
        "Fill_Rate_%",
        "Cumplimiento_OC_%",
        "Brecha_Unidades",
        "Motivo de Falta",
        "Acción Correctiva",
    ]

    # Configuración de columnas del editor
    column_config = {
        "Numero_Orden": st.column_config.TextColumn(
            "Número Orden", width="medium"
        ),
        "Referencia_Cruce": st.column_config.TextColumn(
            "Referencia Cliente", width="medium"
        ),
        "Material_Ref": st.column_config.TextColumn(
            "Código Material", help="Referencia del proveedor", width="medium"
        ),
        "Descripcion_OC": st.column_config.TextColumn(
            "Descripción", width="large"
        ),
        "Cajas_Solicitadas": st.column_config.NumberColumn(
            "Cajas OC", format="%.0f"
        ),
        "UXC": st.column_config.NumberColumn(
            "UXC", format="%.0f"
        ),
        "Unidades_Solicitadas": st.column_config.NumberColumn(
            "Uds. Solicitadas", format="%.0f"
        ),
        "Unidades_Facturadas": st.column_config.NumberColumn(
            "Uds. Facturadas", format="%.0f"
        ),
        "Fill_Rate_%": st.column_config.ProgressColumn(
            "Fill Rate %",
            help="Porcentaje de cumplimiento",
            min_value=0,
            max_value=100,
            format="%.1f %%",
        ),
        "Cumplimiento_OC_%": st.column_config.ProgressColumn(
            "% Cumplimiento OC",
            help="Cumplimiento por línea de orden de compra (en cajas)",
            min_value=0,
            max_value=100,
            format="%.1f %%",
        ),
        "Brecha_Unidades": st.column_config.NumberColumn(
            "Brecha (uds)", format="%.0f"
        ),
        "Motivo de Falta": st.column_config.SelectboxColumn(
            "Motivo de Falta",
            options=MOTIVOS,
            help="Selecciona la causa de la brecha",
            required=False,
            width="medium",
        ),
        "Acción Correctiva": st.column_config.TextColumn(
            "Acción Correctiva",
            help="Detalla la acción para corregir la brecha",
            width="large",
        ),
    }

    df_edited = st.data_editor(
        df_view[cols_display],
        column_config=column_config,
        disabled=[c for c in cols_display if c not in ("Motivo de Falta", "Acción Correctiva")],
        use_container_width=True,
        hide_index=True,
        key="editor_principal",
    )

    # Sincronizar ediciones al DataFrame completo
    edit_index = df_edited.index
    df_view.loc[edit_index, "Motivo de Falta"] = df_edited["Motivo de Falta"].values
    df_view.loc[edit_index, "Acción Correctiva"] = df_edited["Acción Correctiva"].values
    df_result.loc[df_view.index, "Motivo de Falta"] = df_view["Motivo de Falta"]
    df_result.loc[df_view.index, "Acción Correctiva"] = df_view["Acción Correctiva"]

    # ── Gráfico de barras Fill Rate por ítem ─────────────────────────────────
    st.divider()
    st.subheader("📊 Fill Rate por Producto")

    chart_df = df_view[["Descripcion_OC", "Fill_Rate_%"]].copy()
    chart_df = chart_df.sort_values("Fill_Rate_%", ascending=True)
    chart_df = chart_df.rename(columns={"Descripcion_OC": "Producto", "Fill_Rate_%": "Fill Rate %"})
    st.bar_chart(chart_df.set_index("Producto"), color="#1e88e5", height=350)

    # ── Ítems sin cruce en facturación ───────────────────────────────────────
    sin_factura = df_view[df_view["Unidades_Facturadas"] == 0]
    if not sin_factura.empty:
        with st.expander(
            f"⚠️ {len(sin_factura)} ítem(s) sin registro en facturación", expanded=False
        ):
            st.dataframe(
                sin_factura[["Material_Ref", "Descripcion_OC", "Unidades_Solicitadas"]],
                use_container_width=True,
                hide_index=True,
            )

    # ── Exportación ──────────────────────────────────────────────────────────
    st.divider()
    st.subheader("☁️ Subir Reporte a Google Sheets (Gratis)")
    st.caption(
        "La subida se hace solo cuando presionas el botón. "
        "No se enviará nada automáticamente."
    )

    secret_webapp_url, secret_webhook_token, secret_worksheet_name = get_apps_script_config()
    config_ready = bool(secret_webapp_url and secret_webhook_token)

    if config_ready:
        st.success("Google Sheets configurado desde secrets.")
        worksheet_name = secret_worksheet_name
        st.text_input(
            "Nombre de la pestaña",
            value=secret_worksheet_name,
            disabled=True,
            help="Definido desde st.secrets['APPS_SCRIPT_SHEET_NAME'].",
        )
    else:
        st.warning(
            "Falta configurar APPS_SCRIPT_URL y APPS_SCRIPT_TOKEN en st.secrets. "
            "Mientras no existan, la app no podrá subir a Google Sheets."
        )
        webapp_url = st.text_input(
            "URL del Web App (Apps Script)",
            help="Debe terminar en /exec.",
        )
        webhook_token = st.text_input(
            "Token del Webhook",
            type="password",
            help="Debe ser igual al token configurado en tu Apps Script.",
        )
        worksheet_name = st.text_input("Nombre de la pestaña", value="Reporte Fill Rate")

    st.caption(
        "Tip: en tu Apps Script, implementa doPost(e) y publica como Aplicacion web "
        "con acceso 'Cualquiera con el enlace'."
    )

    st.markdown(
        "**Sheet destino sugerido:** "
        "https://docs.google.com/spreadsheets/d/1Pob-5u0VDlWQgj3PQEDXcaGB-aylsbcWMfUNEozWlOw/edit"
    )

    st.text_input(
        "ID del Google Sheet (referencia)",
        value="1Pob-5u0VDlWQgj3PQEDXcaGB-aylsbcWMfUNEozWlOw",
        disabled=True,
        help=(
            "No se usa en este modo. El destino real lo define la URL del Web App, "
            "que debe estar creado dentro de esta hoja de calculo."
        ),
    )

    df_export = make_export_dataframe(df_result)

    if st.button("Subir reporte a Google Sheets", type="primary"):
        webapp_url = secret_webapp_url if config_ready else webapp_url
        webhook_token = secret_webhook_token if config_ready else webhook_token

        if not webapp_url.strip():
            st.error("Debes ingresar la URL del Web App de Apps Script.")
        elif not webapp_url.strip().endswith("/exec"):
            st.error("La URL parece invalida. Debe terminar en '/exec'.")
        elif not webhook_token.strip():
            st.error("Debes ingresar el token del webhook.")
        else:
            try:
                upload_to_apps_script(
                    df_export=df_export,
                    webapp_url=webapp_url.strip(),
                    token=webhook_token.strip(),
                    worksheet_name=worksheet_name.strip() or "Reporte Fill Rate",
                )
                st.success("Reporte subido correctamente a Google Sheets.")
            except Exception as e:
                st.error(f"No se pudo subir el reporte: {e}")


# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
