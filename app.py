"""
Dashboard de Control Logístico - Lácteos San Antonio C.A.
Calcula el Fill Rate cruzando Órdenes de Compra con Facturación.

Fuentes de datos soportadas:
  - Facturación: XLSX o CSV con columnas 'Material' y 'Cantidad facturada'
  - Orden de Compra: HTML (El Rosado, tabla #AutoNumber2) o
                     XLS/XLSX (formato SMX con filas tipo '1','2','3')
"""

import io
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


# ──────────────────────────────────────────────────────────────────────────────
# ETL – ORDEN DE COMPRA HTML (Corporación El Rosado)
# ──────────────────────────────────────────────────────────────────────────────

def parse_html_order(raw_bytes: bytes) -> pd.DataFrame:
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

def parse_smx_order(raw_bytes: bytes) -> pd.DataFrame:
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
            # UNMA viene como '0012' → 12 ; '0008' → 8
            uxc          = to_float_safe(str(fila.iloc[8]).lstrip("0") or "1")
            cant_cajas   = to_float_safe(str(fila.iloc[18]).lstrip("0") or "0")

            if not referencia or referencia in ("nan", "None"):
                continue
            if uxc <= 0:
                uxc = 1  # Salvaguarda

            items.append(
                {
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

def parse_billing(raw_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Carga el reporte de facturación y lo agrupa por código de material.

    Columnas requeridas: 'Material', 'Cantidad facturada'
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
    required = {"Material", "Cantidad facturada"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(
            f"❌ El archivo de facturación no contiene las columnas: {missing}. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    # Convertir cantidad a numérico
    df["Cantidad facturada"] = df["Cantidad facturada"].apply(
        lambda x: to_float_safe(str(x).replace(",", "."))
    )

    # Normalizar código de material
    df["Material_Norm"] = df["Material"].apply(normalize_code)

    # Agregar descripción si existe
    agg_dict = {"Cantidad facturada": "sum"}
    if "Descripcion" in df.columns:
        agg_dict["Descripcion"] = "first"

    grouped = df.groupby("Material_Norm").agg(agg_dict).reset_index()
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
    merged = df_oc.merge(df_fact, on="Material_Norm", how="left")
    merged["Unidades_Facturadas"] = merged["Unidades_Facturadas"].fillna(0)

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
        st.caption("Sube los dos archivos para activar el análisis.")

        archivo_fact = st.file_uploader(
            "1️⃣  Reporte de Facturación",
            type=["xlsx", "xls", "csv"],
            help="Debe contener las columnas 'Material' y 'Cantidad facturada'.",
        )

        archivo_oc = st.file_uploader(
            "2️⃣  Orden de Compra",
            type=["html", "htm", "xlsx", "xls"],
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
    if archivo_fact is None or archivo_oc is None:
        st.info(
            "👈 Por favor, sube el **Reporte de Facturación** y la "
            "**Orden de Compra** en el panel lateral para comenzar."
        )
        st.stop()

    # Leer y parsear facturación
    with st.spinner("Procesando facturación…"):
        try:
            df_fact = parse_billing(archivo_fact.read(), archivo_fact.name)
        except ValueError as e:
            st.error(str(e))
            st.stop()

    # Leer y parsear orden de compra
    with st.spinner("Procesando orden de compra…"):
        raw_oc = archivo_oc.read()
        try:
            if archivo_oc.name.lower().endswith((".html", ".htm")):
                df_oc = parse_html_order(raw_oc)
            else:
                df_oc = parse_smx_order(raw_oc)
        except ValueError as e:
            st.error(str(e))
            st.stop()

    # Cruce y Fill Rate
    df_result = compute_fill_rate(df_oc, df_fact)

    # ── KPIs globales ────────────────────────────────────────────────────────
    total_solicitado  = df_result["Unidades_Solicitadas"].sum()
    total_facturado   = df_result["Unidades_Facturadas"].sum()
    fill_global       = (total_facturado / total_solicitado * 100) if total_solicitado > 0 else 0
    total_cajas_oc    = df_result["Cajas_Solicitadas"].sum()
    total_cajas_fact  = df_result["Cajas_Facturadas_Estimadas"].sum()
    fill_global_oc    = (total_cajas_fact / total_cajas_oc * 100) if total_cajas_oc > 0 else 0
    items_con_brecha  = (df_result["Brecha_Unidades"] > 0).sum()
    items_totales     = len(df_result)

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
        df_result[cols_display],
        column_config=column_config,
        disabled=[c for c in cols_display if c not in ("Motivo de Falta", "Acción Correctiva")],
        use_container_width=True,
        hide_index=True,
        key="editor_principal",
    )

    # Sincronizar ediciones al DataFrame completo
    df_result["Motivo de Falta"]   = df_edited["Motivo de Falta"]
    df_result["Acción Correctiva"] = df_edited["Acción Correctiva"]

    # ── Gráfico de barras Fill Rate por ítem ─────────────────────────────────
    st.divider()
    st.subheader("📊 Fill Rate por Producto")

    chart_df = df_result[["Descripcion_OC", "Fill_Rate_%"]].copy()
    chart_df = chart_df.sort_values("Fill_Rate_%", ascending=True)
    chart_df = chart_df.rename(columns={"Descripcion_OC": "Producto", "Fill_Rate_%": "Fill Rate %"})
    st.bar_chart(chart_df.set_index("Producto"), color="#1e88e5", height=350)

    # ── Ítems sin cruce en facturación ───────────────────────────────────────
    sin_factura = df_result[df_result["Unidades_Facturadas"] == 0]
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
        "Carga el reporte final usando Apps Script Web App. "
        "No requiere Google Cloud Console ni cuenta de servicio."
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

    export_cols = [
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
            "Material_Ref":         "Código Material",
            "Descripcion_OC":       "Descripción",
            "Cajas_Solicitadas":    "Cajas OC",
            "Unidades_Solicitadas": "Unidades Solicitadas",
            "Unidades_Facturadas":  "Unidades Facturadas",
            "Fill_Rate_%":          "Fill Rate %",
            "Cumplimiento_OC_%":    "% Cumplimiento OC",
            "Brecha_Unidades":      "Brecha (uds)",
        },
        inplace=True,
    )

    if st.button("Subir reporte a Google Sheets", type="primary"):
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
