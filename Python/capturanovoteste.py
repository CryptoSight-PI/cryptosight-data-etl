import csv
import os
import time
from datetime import datetime
from getmac import get_mac_address
import psutil
try:
    import pynvml
    pynvml.nvmlInit()
    NVML_OK = True
except Exception:
    pynvml = None
    NVML_OK = False
from config import cursor
from process import capturarprocesso
from s3config import mandars3

PASTA_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Csv"
)
os.makedirs(PASTA_CSV, exist_ok=True)

INTERVALO_CAPTURA = 10
INTERVALO_CSV = 60
INTERVALO_CONFIG = 300

user = get_mac_address()


def get_fan_speeds():
    if not hasattr(psutil, "sensors_fans"):
        return {}
    result = {}
    for chip, fans in psutil.sensors_fans().items():
        for fan in fans:
            result[fan.label] = fan.current
    return result


def get_cpu_gpu_temps():
    if not hasattr(psutil, "sensors_temperatures"):
        return None, None
    temps = psutil.sensors_temperatures()
    cpu = None
    for chip in ("k10temp", "coretemp", "zenpower"):
        if chip in temps and temps[chip]:
            cpu = temps[chip][0].current
            break
    gpu = temps["amdgpu"][0].current if temps.get("amdgpu") else None
    return cpu, gpu


def coletar_cpu():
    freq = psutil.cpu_freq()
    return {
        "cpu_percent": psutil.cpu_percent(interval=1),
        "cpu_frequency": round(freq.current / 1000, 2) if freq else None,
    }


def coletar_ram():
    return {"ram_percent": psutil.virtual_memory().percent}


def coletar_swap():
    swap = psutil.swap_memory()
    return {
        "swap_memory_total": round(swap.total / (1024 ** 3), 2),
        "swap_memory_used": round(swap.used / (1024 ** 3), 2),
        "swap_memory_percent": round(swap.percent, 2),
    }


ultima_rede = {"net": psutil.net_io_counters(), "tempo": time.time()}


def coletar_rede():
    agora = time.time()
    net = psutil.net_io_counters()
    delta = max(agora - ultima_rede["tempo"], 0.001)
    resultado = {
        "upload_speed": round((net.bytes_sent - ultima_rede["net"].bytes_sent) * 8 / (delta * 1_000_000), 2),
        "download_speed": round((net.bytes_recv - ultima_rede["net"].bytes_recv) * 8 / (delta * 1_000_000), 2),
    }
    ultima_rede["net"] = net
    ultima_rede["tempo"] = agora
    return resultado


def coletar_temperatura():
    cpu_temp, _ = get_cpu_gpu_temps()
    return {"temperature": cpu_temp}


def coletar_ventoinha():
    speeds = get_fan_speeds()
    return {"fans_speed": speeds.get("cpu_fan", next(iter(speeds.values()), None))}


def coletar_disco():
    return {"disk": round(psutil.disk_usage("/").free / (1024 ** 3), 2)}


def coletar_gpu():
    if not NVML_OK:
        return {}

    usos = []
    energias = []
    temperaturas = []
    fans = []

    try:
        for i in range(pynvml.nvmlDeviceGetCount()):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)

            usos.append(pynvml.nvmlDeviceGetUtilizationRates(handle).gpu)
            temperaturas.append(
                pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
            )

            try:
                energias.append(round(pynvml.nvmlDeviceGetPowerUsage(handle) / 1000, 2))
            except pynvml.NVMLError:
                energias.append(None)

            try:
                fans.append(pynvml.nvmlDeviceGetFanSpeed(handle))
            except pynvml.NVMLError:
                fans.append(None)
    except pynvml.NVMLError as e:
        print(f"Erro ao consultar NVIDIA: {e}")
        return {}

    return {
        "gpu_usage": usos,
        "gpu_energy": energias,
        "gpu_temp": temperaturas,
        "gpu_fan_speed": fans,
    }



COLETORES = {
    "cpu": (["cpu_percent", "cpu_frequency"], coletar_cpu),
    "ram": (["ram_percent"], coletar_ram),
    "swap": (["swap_memory_total", "swap_memory_used", "swap_memory_percent"], coletar_swap),
    "rede": (["upload_speed", "download_speed"], coletar_rede),
    "temperatura": (["temperature"], coletar_temperatura),
    "ventoinha": (["fans_speed"], coletar_ventoinha),
    "disco": (["disk"], coletar_disco),
    "gpu": (["gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed"], coletar_gpu),
}


def colunas_csv():
    colunas = ["user"]
    for cols, _ in COLETORES.values():
        for coluna in cols:
            colunas.append(coluna)
    colunas.append("timestamp")
    return colunas


def buscar_monitorados():
    query = ("select c.nome from maquina m join maquina_componente mc on mc.id_maquina = m.id join componente c on c.id = mc.id_componente where m.mac_address = (%s) and mc.monitorado = 1;")
    cursor.execute(query, [user])
    return {linha[0].lower() for linha in cursor.fetchall()}


def coletar(monitorados):
    dados = {"user": user}
    for nome in monitorados:
        if nome not in COLETORES:
            print(f"Sem coletor para '{nome}'")
            continue
        try:
            dados.update(COLETORES[nome][1]())
        except Exception as e:
            print(f"Falha ao coletar '{nome}': {e}")
    dados["timestamp"] = datetime.now()
    return dados


def enviar_s3(data, caminho):
    try:
        mandars3(data, caminho)
        os.remove(caminho)
    except Exception as e:
        print(f"Falha ao enviar {caminho}: {e}")


def novo_arquivo(id_empresa):
    data = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nome = os.path.join(
        PASTA_CSV, f"{id_empresa}-{data}-{user.replace(':', '-')}.csv"
    )
    return data, nome


def capture(monitorados, id_empresa):
    colunas = colunas_csv()
    tempo_inicio_csv = time.time()
    tempo_inicio_config = time.time()
    data_atual, nome_arquivo = novo_arquivo(id_empresa)

    while True:
        current_time = time.time()

        if current_time - tempo_inicio_config >= INTERVALO_CONFIG:
            try:
                monitorados = buscar_monitorados()
            except Exception as e:
                print(f"Falha ao atualizar componentes: {e}")
            tempo_inicio_config = current_time

        if current_time - tempo_inicio_csv >= INTERVALO_CSV:
            if os.path.exists(nome_arquivo):
                enviar_s3(data_atual, nome_arquivo)

            try:
                nome_arquivo_proc = os.path.join(
                    PASTA_CSV,
                    f"{id_empresa}_processos_{data_atual}_{user.replace(':', '-')}.csv",
                )
                capturarprocesso().to_csv(nome_arquivo_proc, sep=";", index=False)
                enviar_s3(data_atual, nome_arquivo_proc)
            except Exception as e:
                print(f"Falha ao capturar processos: {e}")

            data_atual, nome_arquivo = novo_arquivo(id_empresa)
            tempo_inicio_csv = current_time

        dados = coletar(monitorados)

        exhibit(dados)
        store(dados, nome_arquivo, colunas)

        time.sleep(max(0, INTERVALO_CAPTURA - (time.time() - current_time)))


def exhibit(dados):
    largura = 60
    linha = "-" * (largura + 4)
    print(f"\n    {linha}")
    for coluna, valor in dados.items():
        if isinstance(valor, datetime):
            valor = valor.strftime("%Y-%m-%d %H:%M:%S")
        print(f"    | {f'{coluna}: {valor}':<{largura}} |")
    print(f"    {linha}\n")


def store(dados, nome_arquivo, colunas):
    novo = not os.path.exists(nome_arquivo)

    with open(nome_arquivo, "a", newline="") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=colunas, delimiter=";", extrasaction="ignore"
        )
        if novo:
            writer.writeheader()
        writer.writerow(dados)


def main():
    query = ("select e.id from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id where m.mac_address = (%s);")
    cursor.execute(query, [user])
    resultado = cursor.fetchall()

    if not resultado:
        print("seu mac nao esta no banco")
        return

    id_empresa = resultado[0][0]
    monitorados = buscar_monitorados()
    print(f"Empresa {id_empresa} | componentes: {sorted(monitorados)}")

    try:
        capture(monitorados, id_empresa)
    except KeyboardInterrupt:
        print("Encerrado")
    finally:
        if pynvml:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass


if __name__ == "__main__":
    main()