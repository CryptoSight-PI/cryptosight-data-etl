"""
Upload CSVs for existing machines of a company to RAW/BRONZE S3 bucket.

Usage:
  python upload_csvs_company.py --empresa-id 2 --files-per-machine 2 --rows-per-file 6

This script ONLY uploads CSVs to the configured AWS_BUCKET_BRONZE and does NOT
run any ETL. It finds machines in the database for the given empresa_id and
creates CSVs with recent timestamps so the downstream ETL will consider them
online.
"""

import os
import io
import argparse
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
import boto3
import mysql.connector


ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / '.env')

DB = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT') or 3306),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_DATABASE') or os.getenv('DB_NAME'),
}

BUCKET = os.getenv('AWS_BUCKET_BRONZE') or os.getenv('AWS_BUCKET_RAW')
REGION = os.getenv('AWS_DEFAULT_REGION') or None

if not BUCKET:
    raise SystemExit('AWS_BUCKET_BRONZE / AWS_BUCKET_RAW not configured in .env')

CSV_COLUMNS = [
    'user','cpu_percent','cpu_frequency','ram_percent','swap_memory_total','swap_memory_used','swap_memory_percent',
    'upload_speed','download_speed','temperature','fans_speed','disk','gpu_usage','gpu_energy','gpu_temp','gpu_fan_speed','timestamp'
]

# map components to columns (should mirror TRUSTED/COMPONENTE_COLUNAS)
COMPONENTE_COLUNAS = {
    'cpu': ['cpu_percent', 'cpu_frequency'],
    'ram': ['ram_percent'],
    'swap': ['swap_memory_total', 'swap_memory_used', 'swap_memory_percent'],
    'rede': ['upload_speed', 'download_speed'],
    'temperatura': ['temperature'],
    'ventoinha': ['fans_speed'],
    'disco': ['disk'],
    'gpu': ['gpu_usage', 'gpu_energy', 'gpu_temp', 'gpu_fan_speed'],
}

NUMERICAS = [
    'cpu_percent','cpu_frequency','ram_percent','swap_memory_total','swap_memory_used','swap_memory_percent',
    'upload_speed','download_speed','temperature','fans_speed','disk','gpu_usage','gpu_energy','gpu_temp','gpu_fan_speed'
]

COLUNAS_VETOR = ['gpu_usage','gpu_energy','gpu_temp','gpu_fan_speed']

NAO_ZERO = ['cpu_frequency']

# primary metric per component (same logic as CURATED MAPA_COMPONENTES)
MAPA_PRIMARIA = {
    'cpu': 'cpu_percent',
    'ram': 'ram_percent',
    'swap': 'swap_memory_percent',
    'rede': 'download_speed',
    'temperatura': 'temperature',
    'ventoinha': 'fans_speed',
    'disco': 'disk',
    'gpu': 'gpu_usage',
}

MARGEM_ATENCAO = 0.10

# ---------------------------------------------------------------------------
# Limites padrao (min, max) por componente. Usados para preencher a
# maquina_componente quando o limite estiver nulo, e como fallback na geracao.
# ---------------------------------------------------------------------------
LIMITES_PADRAO = {
    'cpu': (10, 85),
    'ram': (10, 90),
    'swap': (0, 90),
    'temperatura': (30, 80),
    'gpu': (0, 90),
    'disco': (0, 700),
    'ventoinha': (600, 4500),
    'rede': (0, 100),
}

# colunas primarias que o script controla por estado (coluna -> (min, max) de fallback)
FALLBACK_LIMITES = {
    'cpu_percent': LIMITES_PADRAO['cpu'],
    'ram_percent': LIMITES_PADRAO['ram'],
    'swap_memory_percent': LIMITES_PADRAO['swap'],
    'temperature': LIMITES_PADRAO['temperatura'],
    'gpu_usage': LIMITES_PADRAO['gpu'],
    'disk': LIMITES_PADRAO['disco'],
    'fans_speed': LIMITES_PADRAO['ventoinha'],
}

PERCENT = {'cpu_percent', 'ram_percent', 'swap_memory_percent', 'gpu_usage'}
COL_PARA_COMPONENTE = {v: k for k, v in MAPA_PRIMARIA.items()}


def connect_db():
    return mysql.connector.connect(**DB)


def list_machines(cursor, empresa_id):
    cursor.execute(
        """
        SELECT f.id, s.id, m.id, m.mac_address
        FROM empresa e
        JOIN farm f ON f.id_empresa = e.id
        JOIN sessao s ON s.id_farm = f.id
        JOIN maquina m ON m.id_sessao = s.id
        WHERE e.id = %s
        """,
        (empresa_id,)
    )
    return cursor.fetchall()


def list_sessions(cursor, empresa_id):
    cursor.execute(
        """
        SELECT f.id, s.id
        FROM farm f
        JOIN sessao s ON s.id_farm = f.id
        WHERE f.id_empresa = %s
        ORDER BY f.id, s.id
        """,
        (empresa_id,)
    )
    return [(r[0], r[1]) for r in cursor.fetchall()]


def ensure_row(cursor, conexao, table, required_values):
    # create a dedicated cursor for metadata queries to avoid unread-result issues
    # Use a temporary separate connection for metadata/insert operations to
    # avoid interfering with the main cursor's unread results in the caller.
    temp_conn = mysql.connector.connect(**DB)
    cur = temp_conn.cursor(buffered=True)
    # try to find by mac_address or nome_fantasia
    if 'mac_address' in required_values:
        cur.execute(f"SELECT id FROM {table} WHERE mac_address = %s", (required_values['mac_address'],))
        r = cur.fetchone()
        if r:
            cur.close()
            return r[0]
    if table == 'empresa' and 'nome_fantasia' in required_values:
        cur.execute("SELECT id FROM empresa WHERE nome_fantasia = %s", (required_values['nome_fantasia'],))
        r = cur.fetchone()
        if r:
            cur.close()
            return r[0]

    try:
        cur.execute(f"SHOW COLUMNS FROM {table}")
        cols = cur.fetchall()
    except Exception:
        cols = []

    insert_cols = list(required_values.keys())
    insert_vals = [required_values[c] for c in insert_cols]

    # detect unique columns
    unique_cols = set()
    try:
        cur.execute(f"SHOW INDEX FROM {table}")
        for idx in cur.fetchall():
            if len(idx) >= 5:
                non_unique = idx[1]
                key_name = idx[2]
                column_name = idx[4]
                if non_unique == 0 and key_name != 'PRIMARY':
                    unique_cols.add(column_name)
    except Exception:
        pass

    for col in cols:
        field, coltype, null_ok, key, default, extra = col
        if field in insert_cols:
            continue
        if default is not None or null_ok == 'YES':
            continue
        if field in unique_cols:
            if 'int' in coltype:
                insert_cols.append(field); insert_vals.append(random.randint(1, 10_000_000)); continue
            if 'char' in coltype or 'text' in coltype or 'varchar' in coltype:
                insert_cols.append(field); insert_vals.append(uuid.uuid4().hex); continue
        if any(t in coltype for t in ('int','decimal','float','double')):
            insert_cols.append(field); insert_vals.append(0)
        else:
            insert_cols.append(field); insert_vals.append('')

    cols_sql = ', '.join(insert_cols)
    placeholders = ', '.join(['%s'] * len(insert_vals))
    sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
    cur.execute(sql, tuple(insert_vals))
    temp_conn.commit()
    lastid = cur.lastrowid
    cur.close()
    temp_conn.close()
    return lastid


def ensure_farm(cursor, conexao, id_empresa, nome=None):
    vals = {'id_empresa': id_empresa, 'nome': nome or f'farm_{uuid.uuid4().hex[:6]}'}
    return ensure_row(cursor, conexao, 'farm', vals)


def ensure_sessao(cursor, conexao, id_farm, nome=None):
    vals = {'id_farm': id_farm, 'nome': nome or f'sessao_{uuid.uuid4().hex[:6]}'}
    return ensure_row(cursor, conexao, 'sessao', vals)


def ensure_maquina(cursor, conexao, id_sessao, mac):
    # normalize mac to canonical form used by ETL
    mac_norm = str(mac).strip().lower().replace('-', ':')
    mid = ensure_row(cursor, conexao, 'maquina', {'id_sessao': id_sessao, 'mac_address': mac_norm})
    # ensure the mac_address is stored in normalized form
    try:
        cursor.execute("UPDATE maquina SET mac_address = %s WHERE id = %s", (mac_norm, mid))
        conexao.commit()
    except Exception:
        pass
    # ensure the machine is assigned to the correct session (overwrite if necessary)
    try:
        cursor.execute("UPDATE maquina SET id_sessao = %s WHERE id = %s", (id_sessao, mid))
        conexao.commit()
    except Exception:
        pass
    return mid


def ensure_componentes(cursor, conexao, nomes):
    ids = {}
    for n in nomes:
        cursor.execute("SELECT id FROM componente WHERE nome = %s", (n,))
        r = cursor.fetchone()
        if r:
            ids[n] = r[0]; continue
        cursor.execute("INSERT INTO componente (nome) VALUES (%s)", (n,))
        conexao.commit(); ids[n] = cursor.lastrowid
    return ids


def ensure_maquina_componente(cursor, conexao, id_maquina, id_componente, nome=None):
    # agora tambem preenche limite_min / limite_max (so onde estiver nulo)
    lim_min, lim_max = LIMITES_PADRAO.get((nome or '').strip().lower(), (None, None))
    cursor.execute("SELECT id FROM maquina_componente WHERE id_maquina = %s AND id_componente = %s", (id_maquina, id_componente))
    r = cursor.fetchone()
    if r:
        cursor.execute(
            "UPDATE maquina_componente SET monitorado = 1, "
            "limite_min = COALESCE(limite_min, %s), limite_max = COALESCE(limite_max, %s) WHERE id = %s",
            (lim_min, lim_max, r[0])
        )
        conexao.commit()
        return r[0]
    cursor.execute(
        "INSERT INTO maquina_componente (id_maquina, id_componente, monitorado, limite_min, limite_max) VALUES (%s, %s, 1, %s, %s)",
        (id_maquina, id_componente, lim_min, lim_max)
    )
    conexao.commit()
    return cursor.lastrowid


def garantir_limites(cursor, conexao, empresa_id):
    """Preenche limite_min/limite_max nulos das maquinas ja existentes da empresa."""
    total = 0
    for nome, (lim_min, lim_max) in LIMITES_PADRAO.items():
        cursor.execute(
            """
            UPDATE maquina_componente mc
            JOIN componente c ON c.id = mc.id_componente
            JOIN maquina m ON m.id = mc.id_maquina
            JOIN sessao s ON s.id = m.id_sessao
            JOIN farm f ON f.id = s.id_farm
            SET mc.limite_min = COALESCE(mc.limite_min, %s),
                mc.limite_max = COALESCE(mc.limite_max, %s)
            WHERE f.id_empresa = %s AND LOWER(c.nome) = %s
            """,
            (lim_min, lim_max, empresa_id, nome)
        )
        total += cursor.rowcount
    conexao.commit()
    print(f'Limites preenchidos/verificados ({total} linhas afetadas)')


def get_monitored_components(cursor, maquina_id):
    cursor.execute(
        """
        SELECT c.nome
        FROM maquina_componente mc
        JOIN componente c ON c.id = mc.id_componente
        WHERE mc.id_maquina = %s AND mc.monitorado = 1
        """,
        (maquina_id,)
    )
    return {r[0].strip().lower() for r in cursor.fetchall()}


def get_limits_for_machine(cursor, maquina_id):
    cursor.execute(
        """
        SELECT c.nome, mc.limite_min, mc.limite_max
        FROM maquina_componente mc
        JOIN componente c ON c.id = mc.id_componente
        WHERE mc.id_maquina = %s
        """,
        (maquina_id,)
    )
    limits = {}
    for nome, lim_min, lim_max in cursor.fetchall():
        key = nome.strip().lower()
        col = MAPA_PRIMARIA.get(key)
        if col:
            limits[col] = (lim_min, lim_max)
    return limits


def valor_para_estado(col, lim, estado):
    """Gera um valor para a coluna de acordo com o estado desejado e os limites da maquina."""
    fb_lo, fb_hi = FALLBACK_LIMITES[col]
    lo = float(lim[0]) if lim and lim[0] is not None else float(fb_lo)
    hi = float(lim[1]) if lim and lim[1] is not None else float(fb_hi)
    teto = 100.0 if col in PERCENT else hi * 1.15

    if estado == 'critical':
        # acima do limite maximo
        v = random.uniform(min(hi * 1.03, teto), teto)
    elif estado == 'attention':
        # perto do limite maximo (margem de atencao), ainda abaixo dele
        v = random.uniform(hi * (1 - MARGEM_ATENCAO), hi * 0.99)
    else:
        # faixa confortavel (dentro da meta)
        baixo = max(lo * 1.3, hi * 0.3)
        alto = hi * 0.8
        if baixo >= alto:
            baixo = hi * 0.5
        v = random.uniform(baixo, alto)

    if col == 'fans_speed':
        return str(int(v))
    return str(round(v, 2))


def generate_csv_content(mac, rows, monitored_components, limits=None, desired_state='normal'):
    now = datetime.now()
    limits = limits or {}
    lines = []
    header = ";".join(CSV_COLUMNS)
    lines.append(header)

    # escolhe 1 ou 2 componentes que vao ficar no estado desejado (critical/attention)
    alvos = set()
    if desired_state in ('critical', 'attention'):
        candidatos = [
            c for c in FALLBACK_LIMITES
            if not monitored_components or COL_PARA_COMPONENTE[c] in monitored_components
        ]
        if not candidatos:
            candidatos = list(FALLBACK_LIMITES)
        alvos = set(random.sample(candidatos, min(len(candidatos), random.randint(1, 2))))

    for i in range(rows):
        ts = (now - timedelta(seconds=random.randint(0, 300))).strftime('%Y-%m-%d %H:%M:%S')
        values = {}

        # fill all numeric columns with plausible values
        for col in NUMERICAS:
            if col in ('cpu_percent','ram_percent','swap_memory_percent','gpu_usage'):
                values[col] = str(round(random.uniform(0, 100), 2))
            elif col == 'cpu_frequency':
                values[col] = f"{round(random.uniform(1.0,4.0),2)}"
            elif col in ('upload_speed','download_speed'):
                values[col] = str(round(random.uniform(0.1,100.0), 2))
            elif col == 'temperature':
                values[col] = str(round(random.uniform(30.0, 85.0), 2))
            elif col == 'fans_speed':
                values[col] = str(random.randint(500, 4000))
            elif col == 'disk':
                values[col] = str(round(random.uniform(5.0, 1000.0), 2))
            elif col == 'swap_memory_total':
                values[col] = str(round(random.uniform(0.1,16.0),2))
            elif col == 'swap_memory_used':
                values[col] = str(round(random.uniform(0.0,8.0),2))
            elif col in COLUNAS_VETOR:
                # vectors: produce at least one value
                if col == 'gpu_usage' or col == 'gpu_fan_speed':
                    vec = [random.randint(0, 100)]
                elif col == 'gpu_energy':
                    vec = [round(random.uniform(10.0,200.0),2)]
                else:
                    vec = [round(random.uniform(30.0,90.0),2)]
                values[col] = str(vec)
            else:
                values[col] = ''

        # sobrescreve as metricas primarias de acordo com o estado desejado:
        # componentes-alvo ficam em critical/attention, o resto fica normal
        for col in FALLBACK_LIMITES:
            estado_col = desired_state if col in alvos else 'normal'
            values[col] = valor_para_estado(col, limits.get(col), estado_col)

        # build row according to CSV_COLUMNS; unmonitored numeric columns must be empty
        row = []
        for col in CSV_COLUMNS:
            if col == 'user':
                row.append(mac)
            elif col == 'timestamp':
                row.append(ts)
            else:
                row.append(values.get(col, ''))

        lines.append(";".join(row))

    return "\n".join(lines).encode('utf-8')


def upload_bytes_to_s3(s3, content_bytes, empresa_id, mac, when):
    # when is a datetime; key format metrics/YYYY/MM/DD/<empresa>-<timestamp>-<mac>.csv
    key = f"metrics/{when:%Y}/{when:%m}/{when:%d}/{empresa_id}-{when:%Y-%m-%d_%H-%M-%S}-{mac.replace(':','-')}.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=content_bytes, ContentType='text/csv')
    return key


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--empresa-id', '-e', type=int, default=2)
    p.add_argument('--files-per-machine', '-f', type=int, default=1)
    p.add_argument('--rows-per-file', '-r', type=int, default=6)
    p.add_argument('--create-farms', type=int, default=0, help='Number of farms to create under the empresa')
    p.add_argument('--sessions-per-farm', type=int, default=0, help='If creating farms, number of sessions per farm')
    p.add_argument('--machines-per-session', type=int, default=0, help='If creating sessions, number of machines per session')
    p.add_argument('--mac-prefix', type=str, default=None, help='Optional MAC prefix to generate deterministic MACs (eg "02:00:00")')
    p.add_argument('--historic-days', type=int, default=0, help='Generate data for N past days (0 = only today)')
    args = p.parse_args()

    conexao = connect_db()
    cursor = conexao.cursor()

    # Optionally create farms/sessions/machines under empresa
    if args.create_farms > 0 and args.sessions_per_farm > 0 and args.machines_per_session > 0:
        print(f"Creating {args.create_farms} farms, {args.sessions_per_farm} sessions per farm, {args.machines_per_session} machines per session for empresa {args.empresa_id}")
        # ensure components exist
        comp_ids = ensure_componentes(cursor, conexao, list(COMPONENTE_COLUNAS.keys()))
        created = []
        for fi in range(args.create_farms):
            farm_name = f"demo_farm_{fi+1}"
            farm_id = ensure_farm(cursor, conexao, args.empresa_id, farm_name)
            for si in range(args.sessions_per_farm):
                sess_name = f"demo_sessao_{fi+1}_{si+1}"
                sess_id = ensure_sessao(cursor, conexao, farm_id, sess_name)
                for mi in range(args.machines_per_session):
                    # generate deterministic mac if prefix provided
                    if args.mac_prefix:
                        parts = args.mac_prefix.split(':')
                        suffix = [f"{(fi+1)&0xff:02x}", f"{(si+1)&0xff:02x}", f"{(mi+1)&0xff:02x}"]
                        mac = ':'.join((parts + suffix)[:6])
                    else:
                        mac = ':'.join(f"{random.randint(0,255):02x}" for _ in range(6))
                    mac = mac.lower()
                    mid = ensure_maquina(cursor, conexao, sess_id, mac)
                    for cname, cid in ensure_componentes(cursor, conexao, list(COMPONENTE_COLUNAS.keys())).items():
                        ensure_maquina_componente(cursor, conexao, mid, cid, cname)
                    created.append((farm_id, sess_id, mid, mac))
        print(f"Created {len(created)} machines")

    # preenche limites nulos das maquinas da empresa (inclui as que ja existiam)
    garantir_limites(cursor, conexao, args.empresa_id)

    machines = list_machines(cursor, args.empresa_id)
    if not machines:
        print('No machines found for empresa', args.empresa_id)
        return

    s3 = boto3.client('s3', region_name=REGION)

    uploaded = []
    # determine sessions available for distribution
    sessions_all = list_sessions(cursor, args.empresa_id)
    if not sessions_all:
        print('No sessions found for empresa, nothing to upload')
        return

    machines_list = machines
    n_machines = len(machines_list)

    # historic generation: for each day, pick random proportions and assign severities per machine
    days = args.historic_days if args.historic_days and args.historic_days > 0 else 1
    for day_offset in range(days):
        # random proportions for this day (mais criticos e atencao que antes)
        p_critical = random.uniform(0.20, 0.35)
        p_attention = random.uniform(0.25, 0.40)
        # cap sum < 0.9
        if p_critical + p_attention > 0.9:
            scale = 0.9 / (p_critical + p_attention)
            p_critical *= scale
            p_attention *= scale

        # prepare assignment for this day
        n_critical = max(1, int(n_machines * p_critical))
        n_attention = max(1, int(n_machines * p_attention))
        assignment = ['normal'] * n_machines
        for idx in range(n_critical):
            assignment[idx] = 'critical'
        for idx in range(n_critical, n_critical + n_attention):
            if idx < n_machines:
                assignment[idx] = 'attention'
        random.shuffle(assignment)

        # for each machine generate file for this day
        for idx, (f_id, s_id, m_id, mac) in enumerate(machines_list):
            mac_clean = str(mac).strip().lower().replace('-', ':')
            monitored = get_monitored_components(cursor, m_id)
            limits = get_limits_for_machine(cursor, m_id)
            severity = assignment[idx]

            for i in range(args.files_per_machine):
                when = datetime.now() - timedelta(days=day_offset, minutes=random.randint(0, 120))
                content = generate_csv_content(mac_clean, args.rows_per_file, monitored, limits=limits, desired_state=severity)
                key = upload_bytes_to_s3(s3, content, args.empresa_id, mac_clean, when)
                uploaded.append(key)
                print('Uploaded', key, f'(day={day_offset} severity={severity})')

            # Verify DB entries for this machine so ETL can match the MAC
            try:
                cursor.execute("SELECT id, mac_address, id_sessao FROM maquina WHERE mac_address = %s", (mac_clean,))
                row = cursor.fetchone()
                if row:
                    print(f"DB verify: maquina id={row[0]} mac={row[1]} sessao={row[2]}")
                else:
                    print(f"DB verify: maquina with mac {mac_clean} NOT found (this is unexpected)")
            except Exception as e:
                print('DB verify error for mac', mac_clean, e)

    print('Done. Uploaded', len(uploaded), 'files to', BUCKET)


if __name__ == '__main__':
    main()