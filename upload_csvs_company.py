"""
Mock de CSVs do coletor -> bucket BRONZE (nao roda ETL).

Estados por maquina (hoje):
  normal    : valores estaveis na faixa confortavel (sem risco previsto)
  attention : perto do limite, subindo devagar (vira critico em algumas horas)
  critical  : estourou o limite ha algumas horas e continua alto
  rising    : AGORA esta normal, mas vem subindo e vai entrar em atencao
              em ~1.5 a 6 horas (e em critico depois) -> gera "risco previsto"

Hoje (day_offset 0) cada maquina recebe 1 arquivo por hora das ultimas
--trend-hours horas (precisa de >= 6 horas para a regressao do Gold prever).
Dias anteriores (--historic-days) recebem --files-per-machine arquivos,
com timestamps coerentes com o dia (antes todos caiam nos ultimos 5 min).

Uso:
  python upload_csvs_company.py --empresa-id 2 --historic-days 5 --trend-hours 10
"""

import os
import argparse
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
import boto3
import mysql.connector

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


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

FUSO_COLETOR = 'America/Sao_Paulo'

CSV_COLUMNS = [
    'user', 'cpu_percent', 'cpu_frequency', 'ram_percent', 'swap_memory_total', 'swap_memory_used', 'swap_memory_percent',
    'upload_speed', 'download_speed', 'temperature', 'fans_speed', 'disk', 'gpu_usage', 'gpu_energy', 'gpu_temp', 'gpu_fan_speed', 'timestamp'
]

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
    'cpu_percent', 'cpu_frequency', 'ram_percent', 'swap_memory_total', 'swap_memory_used', 'swap_memory_percent',
    'upload_speed', 'download_speed', 'temperature', 'fans_speed', 'disk', 'gpu_usage', 'gpu_energy', 'gpu_temp', 'gpu_fan_speed'
]

COLUNAS_VETOR = ['gpu_usage', 'gpu_energy', 'gpu_temp', 'gpu_fan_speed']

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

HORAS_ATE_ATENCAO = [1.5, 2, 2, 3, 4, 6]

PESOS_HOJE = {'rising': 0.5, 'critical': 0.15, 'attention': 0.2}
PESOS_PASSADO = {'critical': 0.25, 'attention': 0.30}

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


def agora_local():
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo(FUSO_COLETOR)).replace(tzinfo=None)
        except Exception:
            pass
    return datetime.now()


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
    temp_conn = mysql.connector.connect(**DB)
    cur = temp_conn.cursor(buffered=True)
    if 'mac_address' in required_values:
        cur.execute(f"SELECT id FROM {table} WHERE mac_address = %s", (required_values['mac_address'],))
        r = cur.fetchone()
        if r:
            cur.close()
            temp_conn.close()
            return r[0]
    if table == 'empresa' and 'nome_fantasia' in required_values:
        cur.execute("SELECT id FROM empresa WHERE nome_fantasia = %s", (required_values['nome_fantasia'],))
        r = cur.fetchone()
        if r:
            cur.close()
            temp_conn.close()
            return r[0]

    try:
        cur.execute(f"SHOW COLUMNS FROM {table}")
        cols = cur.fetchall()
    except Exception:
        cols = []

    insert_cols = list(required_values.keys())
    insert_vals = [required_values[c] for c in insert_cols]

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
                insert_cols.append(field)
                insert_vals.append(random.randint(1, 10_000_000))
                continue
            if 'char' in coltype or 'text' in coltype or 'varchar' in coltype:
                insert_cols.append(field)
                insert_vals.append(uuid.uuid4().hex)
                continue
        if any(t in coltype for t in ('int', 'decimal', 'float', 'double')):
            insert_cols.append(field)
            insert_vals.append(0)
        else:
            insert_cols.append(field)
            insert_vals.append('')

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
    mac_norm = str(mac).strip().lower().replace('-', ':')
    mid = ensure_row(cursor, conexao, 'maquina', {'id_sessao': id_sessao, 'mac_address': mac_norm})
    try:
        cursor.execute("UPDATE maquina SET mac_address = %s WHERE id = %s", (mac_norm, mid))
        conexao.commit()
    except Exception:
        pass
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
            ids[n] = r[0]
            continue
        cursor.execute("INSERT INTO componente (nome) VALUES (%s)", (n,))
        conexao.commit()
        ids[n] = cursor.lastrowid
    return ids


def ensure_maquina_componente(cursor, conexao, id_maquina, id_componente, nome=None):
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


def limites_da_coluna(col, lim):
    fb_lo, fb_hi = FALLBACK_LIMITES[col]
    lo = float(lim[0]) if lim and lim[0] is not None else float(fb_lo)
    hi = float(lim[1]) if lim and lim[1] is not None else float(fb_hi)
    teto = 100.0 if col in PERCENT else hi * 1.15
    piso = max(lo * 1.2, hi * 0.15)
    return lo, hi, teto, piso


def plano_coluna(col, lim, estado, horas_hist):
    """
    Define a trajetoria de uma coluna: valor(idade_em_horas) = v_agora - inclinacao * idade.
    v_agora = valor mais recente; inclinacao > 0 = vem subindo.
    """
    lo, hi, teto, piso = limites_da_coluna(col, lim)
    inclinacao = 0.0

    if estado == 'critical':
        v_agora = min(random.uniform(hi * 1.03, hi * 1.12), teto)
        inclinacao = hi * random.uniform(0.02, 0.04)
        inclinacao = min(inclinacao, (v_agora - piso) / max(horas_hist - 1, 1))

    elif estado == 'attention':
        v_agora = random.uniform(hi * (1 - MARGEM_ATENCAO) * 1.01, hi * 0.97)
        inclinacao = hi * random.uniform(0.008, 0.02)

    elif estado == 'rising':
        t = random.choice(HORAS_ATE_ATENCAO)
        inclinacao = hi * random.uniform(0.03, 0.05)
        inclinacao = min(inclinacao, (hi * (1 - MARGEM_ATENCAO) - piso) / (t + horas_hist - 1))
        v_agora = hi * (1 - MARGEM_ATENCAO) - inclinacao * t

    else:
        baixo = max(lo * 1.3, hi * 0.3)
        alto = hi * 0.8
        if baixo >= alto:
            baixo = hi * 0.5
        v_agora = random.uniform(baixo, alto)

    return {'v_agora': v_agora, 'inclinacao': inclinacao, 'lo': lo, 'hi': hi, 'teto': teto, 'piso': piso}


def valor_no_tempo(col, plano, idade_h):
    v = plano['v_agora'] - plano['inclinacao'] * idade_h
    v = v + random.gauss(0, plano['hi'] * 0.008)
    v = max(plano['piso'] * 0.9, min(v, plano['teto']))
    if col == 'fans_speed':
        return str(int(v))
    return str(round(v, 2))


def escolher_alvos(estado, monitored):
    if estado == 'normal':
        return set()
    candidatos = [
        c for c in FALLBACK_LIMITES
        if not monitored or COL_PARA_COMPONENTE[c] in monitored
    ]
    if not candidatos:
        candidatos = list(FALLBACK_LIMITES)
    return set(random.sample(candidatos, min(len(candidatos), random.randint(1, 2))))


def montar_planos(limits, estado, alvos, horas_hist):
    planos = {}
    for col in FALLBACK_LIMITES:
        estado_col = estado if col in alvos else 'normal'
        planos[col] = plano_coluna(col, limits.get(col), estado_col, horas_hist)
    return planos


def gerar_csv(mac, base_time, idades_s, planos):
    """Uma linha por idade (em segundos antes de base_time)."""
    linhas = [";".join(CSV_COLUMNS)]

    for idade in idades_s:
        ts = (base_time - timedelta(seconds=idade)).strftime('%Y-%m-%d %H:%M:%S')
        idade_h = idade / 3600.0
        values = {}

        for col in NUMERICAS:
            if col in ('cpu_percent', 'ram_percent', 'swap_memory_percent', 'gpu_usage'):
                values[col] = str(round(random.uniform(0, 100), 2))
            elif col == 'cpu_frequency':
                values[col] = f"{round(random.uniform(1.0, 4.0), 2)}"
            elif col in ('upload_speed', 'download_speed'):
                values[col] = str(round(random.uniform(0.1, 100.0), 2))
            elif col == 'temperature':
                values[col] = str(round(random.uniform(30.0, 85.0), 2))
            elif col == 'fans_speed':
                values[col] = str(random.randint(500, 4000))
            elif col == 'disk':
                values[col] = str(round(random.uniform(5.0, 1000.0), 2))
            elif col == 'swap_memory_total':
                values[col] = str(round(random.uniform(0.1, 16.0), 2))
            elif col == 'swap_memory_used':
                values[col] = str(round(random.uniform(0.0, 8.0), 2))
            elif col in COLUNAS_VETOR:
                if col == 'gpu_usage' or col == 'gpu_fan_speed':
                    vec = [random.randint(0, 100)]
                elif col == 'gpu_energy':
                    vec = [round(random.uniform(10.0, 200.0), 2)]
                else:
                    vec = [round(random.uniform(30.0, 90.0), 2)]
                values[col] = str(vec)
            else:
                values[col] = ''

        for col in FALLBACK_LIMITES:
            values[col] = valor_no_tempo(col, planos[col], idade_h)

        values['gpu_usage'] = "[" + values['gpu_usage'] + "]"

        row = []
        for col in CSV_COLUMNS:
            if col == 'user':
                row.append(mac)
            elif col == 'timestamp':
                row.append(ts)
            else:
                row.append(values.get(col, ''))
        linhas.append(";".join(row))

    return "\n".join(linhas).encode('utf-8')


def upload_bytes_to_s3(s3, content_bytes, empresa_id, mac, when):
    key = f"metrics/{when:%Y}/{when:%m}/{when:%d}/{empresa_id}-{when:%Y-%m-%d_%H-%M-%S}-{mac.replace(':', '-')}.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=content_bytes, ContentType='text/csv')
    return key


def sortear_estados(n, pesos):
    estados = []
    for estado, p in pesos.items():
        qtd = int(round(n * p))
        if estado == 'rising':
            qtd = max(1, qtd)
            if n >= 3:
                qtd = min(qtd, n - 2)
        elif n >= 3:
            qtd = max(1, qtd)
        estados += [estado] * qtd
    estados = estados[:n]
    estados += ['normal'] * (n - len(estados))
    random.shuffle(estados)
    return estados


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--empresa-id', '-e', type=int, default=2)
    p.add_argument('--files-per-machine', '-f', type=int, default=1, help='arquivos por maquina nos dias ANTERIORES')
    p.add_argument('--rows-per-file', '-r', type=int, default=6)
    p.add_argument('--trend-hours', type=int, default=10, help='horas de historico de hoje (minimo 6 para prever)')
    p.add_argument('--create-farms', type=int, default=0)
    p.add_argument('--sessions-per-farm', type=int, default=0)
    p.add_argument('--machines-per-session', type=int, default=0)
    p.add_argument('--mac-prefix', type=str, default=None)
    p.add_argument('--historic-days', type=int, default=0, help='N dias (inclui hoje). 0 = so hoje')
    args = p.parse_args()

    if args.trend_hours < 6:
        print('trend-hours < 6: o Gold nao consegue prever. Usando 6.')
        args.trend_hours = 6

    conexao = connect_db()
    cursor = conexao.cursor()

    if args.create_farms > 0 and args.sessions_per_farm > 0 and args.machines_per_session > 0:
        print(f"Creating {args.create_farms} farms, {args.sessions_per_farm} sessions per farm, {args.machines_per_session} machines per session for empresa {args.empresa_id}")
        ensure_componentes(cursor, conexao, list(COMPONENTE_COLUNAS.keys()))
        created = []
        for fi in range(args.create_farms):
            farm_id = ensure_farm(cursor, conexao, args.empresa_id, f"demo_farm_{fi + 1}")
            for si in range(args.sessions_per_farm):
                sess_id = ensure_sessao(cursor, conexao, farm_id, f"demo_sessao_{fi + 1}_{si + 1}")
                for mi in range(args.machines_per_session):
                    if args.mac_prefix:
                        parts = args.mac_prefix.split(':')
                        suffix = [f"{(fi + 1) & 0xff:02x}", f"{(si + 1) & 0xff:02x}", f"{(mi + 1) & 0xff:02x}"]
                        mac = ':'.join((parts + suffix)[:6])
                    else:
                        mac = ':'.join(f"{random.randint(0, 255):02x}" for _ in range(6))
                    mac = mac.lower()
                    mid = ensure_maquina(cursor, conexao, sess_id, mac)
                    for cname, cid in ensure_componentes(cursor, conexao, list(COMPONENTE_COLUNAS.keys())).items():
                        ensure_maquina_componente(cursor, conexao, mid, cid, cname)
                    created.append((farm_id, sess_id, mid, mac))
        print(f"Created {len(created)} machines")

    garantir_limites(cursor, conexao, args.empresa_id)

    machines_list = list_machines(cursor, args.empresa_id)
    if not machines_list:
        print('No machines found for empresa', args.empresa_id)
        return

    if not list_sessions(cursor, args.empresa_id):
        print('No sessions found for empresa, nothing to upload')
        return

    s3 = boto3.client('s3', region_name=REGION)
    agora = agora_local()
    n_machines = len(machines_list)
    uploaded = []

    days = args.historic_days if args.historic_days and args.historic_days > 0 else 1

    for day_offset in range(days):
        hoje = day_offset == 0
        assignment = sortear_estados(n_machines, PESOS_HOJE if hoje else PESOS_PASSADO)

        for idx, (f_id, s_id, m_id, mac) in enumerate(machines_list):
            mac_clean = str(mac).strip().lower().replace('-', ':')
            monitored = get_monitored_components(cursor, m_id)
            limits = get_limits_for_machine(cursor, m_id)
            estado = assignment[idx]
            alvos = escolher_alvos(estado, monitored)

            if hoje:
                planos = montar_planos(limits, estado, alvos, args.trend_hours)
                for k in range(args.trend_hours):
                    if k == 0:
                        idades = [random.randint(0, 240) for _ in range(args.rows_per_file)]
                    else:
                        idades = [k * 3600 + random.randint(0, 1800) for _ in range(args.rows_per_file)]
                    when = agora - timedelta(seconds=min(idades))
                    content = gerar_csv(mac_clean, agora, idades, planos)
                    uploaded.append(upload_bytes_to_s3(s3, content, args.empresa_id, mac_clean, when))
                print(f'Uploaded {args.trend_hours} arquivos horarios | rig {m_id} | estado={estado} | alvos={sorted(alvos)}')
            else:
                estado_dia = 'normal' if estado == 'rising' else estado
                planos = montar_planos(limits, estado_dia, alvos if estado_dia != 'normal' else set(), 1)
                for i in range(args.files_per_machine):
                    when = agora - timedelta(days=day_offset, minutes=random.randint(0, 120))
                    idades = [random.randint(0, 300) for _ in range(args.rows_per_file)]
                    content = gerar_csv(mac_clean, when, idades, planos)
                    key = upload_bytes_to_s3(s3, content, args.empresa_id, mac_clean, when)
                    uploaded.append(key)
                    print('Uploaded', key, f'(day={day_offset} estado={estado_dia})')

    print('Done. Uploaded', len(uploaded), 'files to', BUCKET)


if __name__ == '__main__':
    main()