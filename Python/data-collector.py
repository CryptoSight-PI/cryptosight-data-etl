import csv
import os
import time
from datetime import datetime
from getmac import get_mac_address
import psutil
import pynvml

try:
    import pynvml
except ImportError:
    pynvml = None

from config import cursor
from process import capturarprocesso
from s3config import mandars3

PASTA_CSV = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "Csv"
)
os.makedirs(PASTA_CSV, exist_ok=True)

intervalo_captura = 10
intervalo_csv = 60

user = get_mac_address()

RPM_MAX = 5200

def get_fan_speeds():
    if not hasattr(psutil, "sensors_fans"):
        return {}
    result = {}
    for chip, fans in psutil.sensors_fans().items():
        for fan in fans:
            result[fan.label] = min(100.0, fan.current / RPM_MAX * 100)
    return result

speeds = get_fan_speeds()


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


cpu_temp, gpu_temp = get_cpu_gpu_temps()


def capture(components):
    last_net = psutil.net_io_counters()
    last_time = time.time()
    tempo_inicio_csv = time.time()
    data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    nome_arquivo = os.path.join(
        PASTA_CSV, f"{id_empresa}-{data_atual}-{user.replace(':', '-')}.csv"
    )

    while True:
        current_time = time.time()

        if current_time - tempo_inicio_csv >= intervalo_csv:
            if os.path.exists(nome_arquivo):
                mandars3(data_atual, nome_arquivo)
                print("teste1")
            df_processo = capturarprocesso()
            nome_arquivo_proc = f"{id_empresa}_processos_{data_atual}_{user.replace(':', '-')}.csv"
            df_processo.to_csv(nome_arquivo_proc, sep=";", index=False)
            if os.path.exists(nome_arquivo_proc):
                mandars3(data_atual, nome_arquivo_proc)
                print("teste2")
            data_atual = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            nome_arquivo = os.path.join(
                PASTA_CSV,
                f"{id_empresa}-{data_atual}-{user.replace(':', '-')}.csv",
            )
            tempo_inicio_csv = current_time

        time_delta = current_time - last_time
        current_net = psutil.net_io_counters()

        cpu_percent = (
            psutil.cpu_percent(interval=1) if components[0] == 1 else None
        )
        cpu_frequency = (
            round(((psutil.cpu_freq().current) / 1000), 2)
            if components[1] == 1
            else None
        )
        ram_percent = (
            psutil.virtual_memory().percent if components[2] == 1 else None
        )
        swap_memory_total = (
            round((psutil.swap_memory().total) / (1024 ** 3), 2)
            if components[3] == 1
            else None
        )
        swap_memory_used = (
            round((psutil.swap_memory().used) / (1024 ** 3), 2)
            if components[4] == 1
            else None
        )
        swap_memory_percent = (
            round(psutil.swap_memory().percent, 2)
            if components[5] == 1
            else None
        )

        if components[6] == 1:
            try:
                bytes_sent_delta = current_net.bytes_sent - last_net.bytes_sent
                upload_speed = round(
                    (bytes_sent_delta * 8) / (time_delta * 1_000_000), 2
                )
            except Exception:
                upload_speed = 0.0
        else:
            upload_speed = None

        if components[7] == 1:
            try:
                bytes_recv_delta = current_net.bytes_recv - last_net.bytes_recv
                download_speed = round(
                    (bytes_recv_delta * 8) / (time_delta * 1_000_000), 2
                )
            except Exception:
                download_speed = 0.0
        else:
            download_speed = None

        last_net = current_net
        last_time = current_time

        if components[8] == 1:
            try:
                temperature = psutil.sensors_temperatures()[0].current
            except Exception:
                temperature = cpu_temp
        else:
            temperature = None

        if components[9] == 1:
            try:
                fans_speed = psutil.sensors_fans().current
            except Exception:
                fans_speed = speeds.get("cpu_fan")
        else:
            fans_speed = None

        disk = (
            round(((psutil.disk_usage("/").free) / (1024 ** 3)), 2)
            if components[10] == 1
            else None
        )

        if components[11] == 1 or components[12] == 1:
            gpu_usage = []
            gpu_memory_usage = []
            gpu_temperature = []
            gpu_energy = []
            gpu_fan_speed = []

            try:
                pynvml.nvmlInit()
                device_count = pynvml.nvmlDeviceGetCount()
                for i in range(device_count):
                    handle = pynvml.nvmlDeviceGetHandleByIndex(i)

                    utilization = pynvml.nvmlDeviceGetUtilizationRates(handle)
                    gpu_usage.append(utilization.gpu)

                    memory = pynvml.nvmlDeviceGetMemoryInfo(handle)
                    memory_percent = round(
                        memory.used * 100 / memory.total, 2
                    )
                    gpu_memory_usage.append(memory_percent)

                    temp = pynvml.nvmlDeviceGetTemperature(
                        handle, pynvml.NVML_TEMPERATURE_GPU
                    )
                    gpu_temperature.append(temp)

                    try:
                        power_mw = pynvml.nvmlDeviceGetPowerUsage(handle)
                        gpu_energy.append(round(power_mw / 1000, 2))
                    except pynvml.NVMLError:
                        gpu_energy.append(None)

                    try:
                        fan = speeds.get("gpu_fan")
                        gpu_fan_speed.append(round(fan, 0))
                    except pynvml.NVMLError:
                        gpu_fan_speed.append(None)

            except pynvml.NVMLError as e:
                print(f"Erro ao consultar NVIDIA: {e}")
                gpu_usage = None
                gpu_memory_usage = None
                gpu_temperature = None
                gpu_energy = None
                gpu_fan_speed = None
            finally:
                try:
                    pynvml.nvmlShutdown()
                except Exception:
                    pass
        else:
            gpu_usage = None
            gpu_memory_usage = None
            gpu_temperature = None
            gpu_energy = None
            gpu_fan_speed = None

        timestamp = datetime.now()

        dados = [
            user,
            cpu_percent,
            cpu_frequency,
            ram_percent,
            swap_memory_total,
            swap_memory_used,
            swap_memory_percent,
            upload_speed,
            download_speed,
            temperature,
            fans_speed,
            disk,
            gpu_usage,
            gpu_energy,
            gpu_temperature,
            gpu_fan_speed,
            timestamp,
        ]

        exhibit(dados)
        store(dados, nome_arquivo)

        time.sleep(intervalo_captura - 1)


def exhibit(data):
    line_user = f"Endereço MAC do dispositivo: {data[0]}"
    line_cpu_percent = f"Uso atual da CPU: {data[1]}%"
    line_cpu_frequency = f"Frequência atual da CPU: {data[2]} GHz"
    line_ram_percent = f"Uso atual de memória RAM: {data[3]}%"
    line_swap_memory_total = f"Total de memória swap: {data[4]} GiB"
    line_swap_memory_used = f"Total de memória swap usada: {data[5]} GiB"
    line_swap_memory_percent = f"Uso atual da memória swap: {data[6]}%"
    line_upload_bytes = (
        f"Tráfego atual de upload de bytes da rede: {data[7]} Mbps"
    )
    line_download_bytes = (
        f"Tráfego atual de download de bytes da rede: {data[8]} Mbps"
    )
    line_temperature = f"Temperatura atual: {data[9]} graus Celsius"
    line_fans_speed = f"Velocidade atual das ventoinhas: {data[10]} RPM"
    line_disk = f"Espaço livre em disco: {data[11]} GiB"
    line_gpu_usage = f"Uso atual da GPU: {data[12]}%"
    line_gpu_energy = f"Consumo atual de energia elétrica pela GPU: {data[13]} W"
    line_gpu_temp = f"Temperatura atual da GPU: {data[14]} graus Celsius"
    line_gpu_fan_speed = f"Velocidade da ventoinha da GPU: {data[15]} RPM"
    line_timestamp = (
        f"Momento de captura: {data[16].strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(
        f"""
    ----------------------------------------------------------------
    | {line_user:<60} |
    | {line_cpu_percent:<60} |
    | {line_cpu_frequency:<60} |
    | {line_ram_percent:<60} |
    | {line_swap_memory_total:<60} |
    | {line_swap_memory_used:<60} |
    | {line_swap_memory_percent:<60} |
    | {line_upload_bytes:<60} |
    | {line_download_bytes:<60} |
    | {line_temperature:<60} |
    | {line_fans_speed:<60} |
    | {line_disk:<60} |
    | {line_gpu_usage:<60} |
    | {line_gpu_energy:<60} |
    | {line_gpu_temp:<60} |
    | {line_gpu_fan_speed:<60} |
    | {line_timestamp:<60} |
    ----------------------------------------------------------------
    """
    )


def store(data, nome_arquivo):
    if not os.path.exists(nome_arquivo):
        with open(nome_arquivo, "w", newline="") as csvfile:
            writer = csv.writer(csvfile, delimiter=";")
            writer.writerow(
                [
                    "user",
                    "cpu_percent",
                    "cpu_frequency",
                    "ram_percent",
                    "swap_memory_total",
                    "swap_memory_used",
                    "swap_memory_percent",
                    "upload_speed",
                    "download_speed",
                    "temperature",
                    "fans_speed",
                    "disk",
                    "gpu_usage",
                    "gpu_energy",
                    "gpu_temp",
                    "gpu_fan_speed",
                    "timestamp",
                ]
            )

    with open(nome_arquivo, "a", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")
        writer.writerow(
            [
                data[0],
                data[1],
                data[2],
                data[3],
                data[4],
                data[5],
                data[6],
                data[7],
                data[8],
                data[9],
                data[10],
                data[11],
                data[12],
                data[13],
                data[14],
                data[15],
                data[16],
            ]
        )


query = "select e.id , m.mac_address from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id where m.mac_address = (%s);"
cursor.execute(query, [user])
resultado1 = cursor.fetchall()

if resultado1:
    id_empresa = resultado1[0][0]
    print(id_empresa)
    print("mac no banco")
    try:
        query2 = "select c.nome from empresa e join farm f on f.id_empresa = e.id join sessao s on s.id_farm = f.id join maquina m on m.id_sessao = s.id join maquina_componente mc on mc.id_maquina = m.id join componente c on c.id = mc.id_componente where m.mac_address = (%s) and mc.monitorado = 1;"
        cursor.execute(query2, [user])
        resultado2 = cursor.fetchall()
        print(resultado2)

        capture(
            [
                1
                if any("cpu" in i[0].lower() for i in resultado2)
                else 0,  # cpu_percent
                1
                if any("cpu" in i[0].lower() for i in resultado2)
                else 0,  # cpu_frequency
                1
                if any("ram" in i[0].lower() for i in resultado2)
                else 0,  # ram_percent
                1
                if any("swap" in i[0].lower() for i in resultado2)
                else 0,  # swap_memory_total
                1
                if any("swap" in i[0].lower() for i in resultado2)
                else 0,  # swap_memory_used
                1
                if any("swap" in i[0].lower() for i in resultado2)
                else 0,  # swap_memory_percent
                1
                if any("rede" in i[0].lower() for i in resultado2)
                else 0,  # upload_speed
                1
                if any("rede" in i[0].lower() for i in resultado2)
                else 0,  # download_speed
                1
                if any("temperatura" in i[0].lower() for i in resultado2)
                else 0,  # temperature
                1
                if any("ventoinha" in i[0].lower() for i in resultado2)
                else 0,  # fans_speed
                1
                if any("disco" in i[0].lower() for i in resultado2)
                else 0,  # disk
                1
                if any("gpu" in i[0].lower() for i in resultado2)
                else 0,  # gpu_usage
                1
                if any("gpu" in i[0].lower() for i in resultado2)
                else 0,  # gpu_energy
                1
                if any("gpu" in i[0].lower() for i in resultado2)
                else 0,  # gpu_temp
                1
                if any("gpu" in i[0].lower() for i in resultado2)
                else 0,  # gpu_fan_speed
            ]
        )
    except KeyboardInterrupt:
        print("Encerrado")
else:
    print("seu mac nao esta no banco")