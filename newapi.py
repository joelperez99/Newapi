# -*- coding: utf-8 -*-
# 🎾 Tennis → Snowflake (BetsAPI Events)
# - Sustituye api-tennis.com por BetsAPI (Events API)
# - Llama /v3/events/upcoming o /v3/events/ended día por día
# - Normaliza a las columnas:
#   event_key, event_date, event_time, first_player, second_player,
#   tournament_name, event_type_type, event_status
# - Guarda en Snowflake y permite copiar match_keys / descargar CSV

import os
import json
import requests
import datetime as dt
import pandas as pd
import streamlit as st
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Para manejo de zonas horarias (si está disponible)
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

st.set_page_config(page_title="🎾 Tennis → Snowflake (BetsAPI)", layout="wide")
st.title("🎾 Cargar Match Keys (BetsAPI) → Snowflake")

# -----------------------------
# Helpers credenciales
# -----------------------------
def _get_secret(name, default=""):
    # Usa secrets de Streamlit Cloud primero, luego variables de entorno
    try:
        return st.secrets[name]
    except Exception:
        return os.getenv(name, default)

# Lee credenciales Snowflake (desde Secrets o ENV)
SF_ACCOUNT   = _get_secret("SF_ACCOUNT")
SF_USER      = _get_secret("SF_USER")
SF_PASSWORD  = _get_secret("SF_PASSWORD")
SF_ROLE      = _get_secret("SF_ROLE", "ACCOUNTADMIN")
SF_WAREHOUSE = _get_secret("SF_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE  = _get_secret("SF_DATABASE", "TENNIS_DB")
SF_SCHEMA    = _get_secret("SF_SCHEMA", "RAW")
SF_TABLE     = _get_secret("SF_TABLE", "RAW_TENNIS_MATCH_KEYS")

# -----------------------------
# Conexión Snowflake
# -----------------------------
@st.cache_resource(show_spinner=False)
def get_sf_conn():
    if not (SF_ACCOUNT and SF_USER and SF_PASSWORD):
        raise RuntimeError("Faltan credenciales SF_ACCOUNT/SF_USER/SF_PASSWORD en Secrets.")
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        role=SF_ROLE,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
    )

def sf_exec(cnx, sql):
    cur = cnx.cursor()
    try:
        cur.execute(sql)
        try:
            return cur.fetchall()
        except Exception:
            return []
    finally:
        cur.close()

def ensure_objects(cnx):
    sf_exec(cnx, f"create database if not exists { SF_DATABASE }")
    sf_exec(cnx, f"create schema if not exists { SF_DATABASE }.{ SF_SCHEMA }")
    sf_exec(cnx, f"use database { SF_DATABASE }")
    sf_exec(cnx, f"use schema { SF_SCHEMA }")
    sf_exec(cnx, f"""
        create table if not exists {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
          event_key string,
          event_date string,
          event_time string,
          first_player string,
          second_player string,
          tournament_name string,
          event_type_type string,
          event_status string,
          source_date date,
          timezone_used string,
          _ingested_at timestamp_ntz default current_timestamp()
        )
    """)

def delete_partition_range(cnx, start_str, stop_str, timezone):
    """
    Borra todas las filas del rango [start_str, stop_str]
    para el timezone indicado.
    """
    sf_exec(cnx, f"""
        delete from {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
        where source_date between to_date('{start_str}') and to_date('{stop_str}')
          and timezone_used = '{timezone}'
    """)

def insert_df(cnx, df):
    write_pandas(
        conn=cnx,
        df=df,
        table_name=SF_TABLE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )

# -----------------------------
# BetsAPI (Events API)
# -----------------------------
BETSAPI_BASE_URL = "https://api.b365api.com"
MAX_PAGES_PER_DAY = 20  # seguridad para no paginar infinito

def _extract_name(side):
    """
    Extrae el nombre de home/away desde la estructura de BetsAPI.
    Puede venir como string o dict con distintos campos.
    """
    if side is None:
        return ""
    if isinstance(side, dict):
        return (
            side.get("name")
            or side.get("name_en")
            or side.get("name_full")
            or side.get("name_short")
            or ""
        )
    return str(side)

def _convert_epoch_to_date_time(epoch_value, tz_name: str):
    """
    Convierte epoch (UTC) a strings de fecha y hora,
    usando la zona horaria indicada si es posible.
    """
    if not epoch_value:
        return "", ""
    try:
        ts = int(epoch_value)
    except Exception:
        return "", ""

    try:
        if ZoneInfo is not None and tz_name:
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                tz = dt.timezone.utc
        else:
            tz = dt.timezone.utc

        dt_utc = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        dt_local = dt_utc.astimezone(tz)
        return dt_local.strftime("%Y-%m-%d"), dt_local.strftime("%H:%M:%S")
    except Exception:
        # fallback simple
        try:
            d = dt.datetime.utcfromtimestamp(ts)
            return d.strftime("%Y-%m-%d"), d.strftime("%H:%M:%S")
        except Exception:
            return "", ""

def normalize_result(result_list, tz_name: str):
    """
    Normaliza la lista de eventos de BetsAPI (Events API)
    a las columnas que necesitamos.
    """
    rows = []
    for it in (result_list or []):
        # ID del evento (Events API normalmente usa 'id')
        event_id = (
            it.get("id")
            or it.get("event_id")
            or it.get("FI")
            or it.get("match_id")
            or it.get("event_key")
        )
        event_key = str(event_id) if event_id is not None else ""

        # Tiempo del evento (epoch UTC en campo 'time')
        time_epoch = it.get("time") or it.get("start_time") or it.get("kickoff")
        event_date, event_time = _convert_epoch_to_date_time(time_epoch, tz_name)

        # Liga / torneo
        league = it.get("league") or {}
        if isinstance(league, dict):
            tournament_name = (
                league.get("name")
                or league.get("name_en")
                or league.get("cc")
                or ""
            )
        else:
            tournament_name = str(league) if league is not None else ""

        # Home / Away (para tenis, equivalen a jugador 1 / jugador 2)
        home = it.get("home") or it.get("home_team") or it.get("home_player")
        away = it.get("away") or it.get("away_team") or it.get("away_player")

        first_player = _extract_name(home)
        second_player = _extract_name(away)

        # sport_id como tipo de evento (no es exactamente lo mismo que en api-tennis,
        # pero nos sirve para clasificar).
        event_type_type = str(it.get("sport_id") or "")

        # Estado: usamos time_status o status crudo
        event_status = str(it.get("time_status") or it.get("status") or "")

        rows.append({
            "event_key":       event_key,
            "event_date":      event_date,
            "event_time":      event_time,
            "first_player":    first_player,
            "second_player":   second_player,
            "tournament_name": tournament_name,
            "event_type_type": event_type_type,
            "event_status":    event_status,
        })

    return pd.DataFrame(rows)

def fetch_api_day(token: str, day_str: str, sport_id: int, scope: str, page: int = 1) -> dict:
    """
    Llama a BetsAPI Events API para un día concreto y una página concreta.

    scope = "upcoming" -> /v3/events/upcoming
    scope = "ended"    -> /v3/events/ended
    """
    endpoint = "/v3/events/upcoming" if scope == "upcoming" else "/v3/events/ended"

    r = requests.get(
        BETSAPI_BASE_URL + endpoint,
        params={
            "token": token,
            "sport_id": sport_id,
            "day": day_str,   # formato YYYYMMDD
            "page": page,
        },
        timeout=40
    )
    r.raise_for_status()
    return r.json()

# -----------------------------
# UI
# -----------------------------
with st.sidebar:
    st.header("🌍 BetsAPI – Events API")

    betsapi_token = st.text_input("Token de BetsAPI", type="password", help="Parámetro token= de BetsAPI")
    # Sport ID configurable (por defecto 13 para Tennis si usas ese ID en tu cuenta)
    sport_id = st.number_input(
        "Sport ID (Tennis)",
        min_value=1,
        max_value=999,
        value=13,
        step=1,
        help="sport_id de BetsAPI para Tennis (revísalo en tu cuenta/documentación)."
    )
    scope = st.selectbox(
        "Tipo de eventos",
        options=["Próximos (upcoming)", "Finalizados (ended)"],
        index=0,
    )
    scope_key = "upcoming" if "Próximos" in scope else "ended"

    fecha_desde = st.date_input("Fecha desde", value=dt.date.today(), format="YYYY-MM-DD")
    fecha_hasta = st.date_input("Fecha hasta", value=dt.date.today(), format="YYYY-MM-DD")
    timezone = st.text_input(
        "Timezone para mostrar hora",
        value="America/Monterrey",
        help="Sólo afecta cómo se formatean event_date/event_time."
    )

# Strings de fechas
start_str = fecha_desde.strftime("%Y-%m-%d")
stop_str  = fecha_hasta.strftime("%Y-%m-%d")

# Validación rápida del rango
if fecha_hasta < fecha_desde:
    st.sidebar.error("⚠️ La 'Fecha hasta' no puede ser menor que la 'Fecha desde'.")

col1, col2, col3 = st.columns([1.2, 1.2, 2])
with col1:
    do_fetch = st.button("📡 Traer desde BetsAPI")
with col2:
    do_save = st.button("💾 Guardar en Snowflake")

st.markdown("#### 📄 Plan B: subir JSON de BetsAPI (payload crudo)")
upl = st.file_uploader("Archivo .json", type=["json"], help="Debe contener success/result(s) como en la API de BetsAPI.")

# buffer de datos
if "df_buf" not in st.session_state:
    st.session_state.df_buf = pd.DataFrame()

# -----------------------------
# Acciones
# -----------------------------
if do_fetch:
    if not betsapi_token.strip():
        st.warning("Ingresa tu Token de BetsAPI.")
    elif fecha_hasta < fecha_desde:
        st.error("Rango de fechas inválido. Corrige 'Fecha desde' y 'Fecha hasta'.")
    else:
        try:
            total_dias = (fecha_hasta - fecha_desde).days + 1
            barra = st.progress(0.0, text="Consultando BetsAPI día por día...")
            dfs = []
            errores = []

            for i in range(total_dias):
                dia = fecha_desde + dt.timedelta(days=i)
                dia_str_display = dia.strftime("%Y-%m-%d")
                day_param = dia.strftime("%Y%m%d")  # BetsAPI usa YYYYMMDD

                acumulados_dia = []
                page = 1

                while True:
                    try:
                        payload = fetch_api_day(
                            token=betsapi_token.strip(),
                            day_str=day_param,
                            sport_id=int(sport_id),
                            scope=scope_key,
                            page=page
                        )
                    except Exception as e:
                        errores.append(f"{dia_str_display} (página {page}): error HTTP {e}")
                        break

                    success = payload.get("success")
                    if success != 1:
                        errores.append(
                            f"{dia_str_display} (página {page}): success != 1 ({payload.get('error', payload)})"
                        )
                        break

                    # BetsAPI usa normalmente 'results' (plural)
                    results = payload.get("results")
                    if results is None:
                        # fallback por si viniera como 'result'
                        results = payload.get("result")

                    if not results:
                        # sin resultados -> fin de páginas para ese día
                        break

                    acumulados_dia.extend(results)

                    # Seguridad: si ya no hay más páginas, paramos;
                    # BetsAPI suele devolver menos resultados en la última página.
                    page += 1
                    if page > MAX_PAGES_PER_DAY:
                        errores.append(
                            f"{dia_str_display}: se alcanzó MAX_PAGES_PER_DAY={MAX_PAGES_PER_DAY}, podrían faltar eventos."
                        )
                        break

                if acumulados_dia:
                    df_dia = normalize_result(acumulados_dia, timezone.strip())
                    if not df_dia.empty:
                        dfs.append(df_dia)

                barra.progress(
                    (i + 1) / total_dias,
                    text=f"Consultando {dia_str_display} ({i+1}/{total_dias}) – {scope_key}"
                )

            if not dfs:
                st.error("No se obtuvieron partidos en el rango seleccionado (o todos los días dieron error).")
                if errores:
                    st.warning("Detalle de errores:\n" + "\n".join(errores))
            else:
                df_all = pd.concat(dfs, ignore_index=True)

                # Opcional: eliminar duplicados por event_key
                if "event_key" in df_all.columns:
                    df_all = df_all.drop_duplicates(subset=["event_key"])

                st.session_state.df_buf = df_all

                msg = f"OK. {len(st.session_state.df_buf)} partidos entre {start_str} y {stop_str}, consultando día por día vía BetsAPI."
                if errores:
                    msg += f" Se encontraron algunos errores ({len(errores)}); revisa los detalles abajo."
                st.success(msg)

                if errores:
                    with st.expander("Ver detalles de errores por día/página"):
                        for e in errores:
                            st.text(e)

        except Exception as e:
            st.error(f"Error general llamando BetsAPI: {e}")

if upl is not None:
    try:
        data = json.load(upl)

        success = data.get("success")
        if success is not None and success != 1:
            st.error(f"JSON no contiene success=1 (success={success})")
        else:
            # Puede venir como results/result o directamente como lista
            result_list = (
                data.get("results")
                or data.get("result")
                or (data if isinstance(data, list) else [])
            )
            df_json = normalize_result(result_list, timezone.strip())
            if df_json.empty:
                st.warning("JSON cargado pero no se pudieron normalizar eventos.")
            else:
                st.session_state.df_buf = df_json
                st.success(f"JSON cargado. {len(st.session_state.df_buf)} partidos.")
    except Exception as e:
        st.error(f"JSON inválido: {e}")

st.markdown("---")
st.subheader("📊 Vista previa")
df = st.session_state.df_buf

if df.empty:
    st.info("Sin datos aún. Usa 'Traer desde BetsAPI' o sube un JSON.")
else:
    st.dataframe(df, use_container_width=True, height=420)

    # ================================
    # 🔵 Botón: Copiar Match Keys
    # ================================
    if "event_key" in df.columns:
        matchkeys_str = "\n".join(df["event_key"].astype(str).tolist())
    else:
        matchkeys_str = ""

    matchkeys_json = json.dumps(matchkeys_str)

    st.markdown(
        f"""
        <button
            style="
                margin-top: 0.5rem;
                padding: 0.4rem 0.8rem;
                border-radius: 0.3rem;
                border: 1px solid #ccc;
                cursor: pointer;
                background-color: #f5f5f5;
            "
            onclick='navigator.clipboard.writeText({matchkeys_json}); alert("Match Keys copiados al portapapeles");'>
            📋 Copiar Match Keys
        </button>
        """,
        unsafe_allow_html=True,
    )

    st.download_button(
        "⬇️ Descargar CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"match_keys_{start_str}_a_{stop_str}.csv",
        mime="text/csv",
        use_container_width=True
    )

# -----------------------------
# Guardar en Snowflake
# -----------------------------
if do_save:
    if df.empty:
        st.warning("No hay datos para guardar.")
    elif fecha_hasta < fecha_desde:
        st.error("Rango de fechas inválido. Corrige 'Fecha desde' y 'Fecha hasta'.")
    else:
        cnx = None
        try:
            cnx = get_sf_conn()
            ensure_objects(cnx)

            # Borra partición del rango
            delete_partition_range(cnx, start_str, stop_str, timezone.strip())

            # Prepara DF para Snowflake
            df2 = df.copy()
            # Usa event_date como source_date; si falla, cae en start_str
            try:
                df2["source_date"] = pd.to_datetime(df2["event_date"], errors="coerce").dt.date
                default_date = pd.to_datetime(start_str).date()
                df2["source_date"] = df2["source_date"].fillna(default_date)
            except Exception:
                df2["source_date"] = pd.to_datetime(start_str).date()

            df2["timezone_used"] = timezone.strip()

            insert_df(cnx, df2)
            st.success(f"Guardado en {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (rango {start_str} a {stop_str}).")
        except Exception as e:
            st.error(f"Error guardando en Snowflake: {e}")
        finally:
            try:
                if cnx is not None:
                    cnx.close()
            except Exception:
                pass

st.markdown("---")
st.subheader("🔎 Consulta rápida en Snowflake")

lim = st.number_input("Límite", 1, 10000, 200, 50)

q = f"""
select event_key,event_date,event_time,first_player,second_player,
       tournament_name,event_type_type,event_status,
       source_date,timezone_used,_ingested_at
from {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
where source_date between to_date('{start_str}') and to_date('{stop_str}')
  and timezone_used = '{timezone}'
order by tournament_name, event_time, event_key
limit {int(lim)}
"""
st.code(q, language="sql")

try:
    cnx2 = get_sf_conn()
    df_db = pd.read_sql(q, cnx2)
    cnx2.close()
    st.dataframe(df_db, use_container_width=True, height=360)
except Exception as e:
    st.info(f"No se pudo consultar (¿tabla aún vacía?). Detalle: {e}")
