import psutil
import time
from datetime import datetime
from getmac import get_mac_address
try:
    import pynvml
except ImportError:
    pynvml = None
import csv
import os
from config import cursor

intervalo_captura = 10
intervalo_csv = 60

user = get_mac_address()


def get_fan_speeds():
    if not hasattr(psutil, "sensors_fans"):
        return {} 

    result = {}
    for chip, fans in psutil.sensors_fans().items():
        for fan in fans:
            result[fan.label] = fan.current
    return result

speeds = get_fan_speeds() 

def get_cpu_gpu_temps():
    if not hasattr(psutil, "sensors_temperatures"):
        return None, None  

    temps = psutil.sensors_temperatures()

    cpu = None
    for chip in ('k10temp', 'coretemp', 'zenpower'):
        if chip in temps and temps[chip]:
            cpu = temps[chip][0].current
            break

    gpu = temps['amdgpu'][0].current if temps.get('amdgpu') else None

    return cpu, gpu
cpu_temp, gpu_temp = get_cpu_gpu_temps()

def capture(components):
    

    last_net = psutil.net_io_counters()
    last_time = time.time()
    tempo_inicio_csv = time.time()
    data_atual = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    nome_arquivo = f"{id_empresa}-{data_atual}-{user.replace(':', '-')}.csv"
    
    while True:
        current_time = time.time()

        if(current_time - tempo_inicio_csv >= intervalo_csv):
            data_atual = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            nome_arquivo = f"{id_empresa}-{data_atual}-{user.replace(':', '-')}.csv"
            tempo_inicio_csv = current_time
            
        time_delta = current_time - last_time
        current_net = psutil.net_io_counters()
        
        cpu_percent = psutil.cpu_percent(interval=1) if components[0] == 1 else None
        cpu_frequency = round(((psutil.cpu_freq().current) / 1000), 2) if components[1] == 1 else None

        ram_percent = psutil.virtual_memory().percent if components[2] == 1 else None

        swap_memory_total = round((psutil.swap_memory().total) / (1024 ** 3), 2) if components[3] == 1 else None
        swap_memory_used = round((psutil.swap_memory().used) / (1024 ** 3), 2) if components[4] == 1 else None
        swap_memory_percent = round(psutil.swap_memory().percent, 2) if components[5] == 1 else None

        if components[6] == 1:
            try:
                bytes_sent_delta = current_net.bytes_sent - last_net.bytes_sent
                upload_speed = round((bytes_sent_delta * 8) / (time_delta * 1_000_000), 2)
            except Exception:
                upload_speed = 0.0
        else:
            upload_speed = None

        if components[7] == 1:
            try:
                bytes_recv_delta = current_net.bytes_recv - last_net.bytes_recv
                download_speed = round((bytes_recv_delta * 8) / (time_delta * 1_000_000), 2)
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

                fans_speed = speeds.get('cpu_fan')
        else:
            fans_speed = None

        disk = round(((psutil.disk_usage('/').free) / (1024 ** 3)), 2) if components[10] == 1 else None

        if components[11] == 1:
            gpu_usage = 0.0
            gpu_temperature = 0
            gpu_fan_speed = 0
            try: 
                pynvml.nvmlInit()
                deviceCount = pynvml.nvmlDeviceGetCount()
                gpu_usage = []
                for j in range(deviceCount):
                    handle = pynvml.nvmlDeviceGetHandleByIndex(j)
                    info = pynvml.nvmlDeviceGetMemoryInfo(handle)

                    usage = round(((info.used * 100) / info.total), 2)
                    gpu_usage.append(usage)

                pynvml.nvmlShutdown()

                gpu_temperature = gpu_temp
                gpu_fan_speed = speeds.get('gpu_fan')
            except Exception:
                gpu_usage = 0.0
                gpu_temperature = 0
                gpu_fan_speed = 0
        else:
            gpu_usage = None
            gpu_temperature = None
            gpu_fan_speed = None

        if components[12] == 1:
            try:
                pynvml.nvmlInit()
                deviceCount = pynvml.nvmlDeviceGetCount()
                gpu_energy = []
                for j in range(deviceCount):
                    handle = pynvml.nvmlDeviceGetHandleByIndex(j)
                    power_mw = pynvml.nvmlDeviceGetPowerUsage(handle)
                    energy = round((power_mw / 1000.0), 2)
                    gpu_energy.append(energy)
                pynvml.nvmlShutdown()
            except Exception:
                gpu_energy = 0.0
        else:
            gpu_energy = None

        timestamp = datetime.now()

        

        dados = [user, cpu_percent, cpu_frequency, ram_percent, swap_memory_total,
                swap_memory_used, swap_memory_percent, upload_speed, download_speed,
                temperature, fans_speed, disk, gpu_usage, gpu_energy,
                gpu_temperature, gpu_fan_speed, timestamp]

        exhibit(dados)
        store(dados, nome_arquivo)
  
        time.sleep(intervalo_captura - 1)

def exhibit(data):
    line_user = f"Endereço MAC do dispositivo: {data[0]}"
    line_cpu_percent  = f"Uso atual da CPU: {data[1]}%"
    line_cpu_frequency  = f"Frequência atual da CPU: {data[2]} GHz"
    line_ram_percent  = f"Uso atual de memória RAM: {data[3]}%"
    line_swap_memory_total = f"Total de memória swap: {data[4]} GiB"
    line_swap_memory_used = f"Total de memória swap usada: {data[5]} GiB"
    line_swap_memory_percent = f"Uso atual da memória swap: {data[6]}%"
    line_upload_bytes = f"Tráfego atual de upload de bytes da rede: {data[7]} Mbps"
    line_download_bytes = f"Tráfego atual de download de bytes da rede: {data[8]} Mbps"
    line_temperature = f"Temperatura atual: {data[9]} graus Celsius"
    line_fans_speed = f"Velocidade atual das ventoinhas: {data[10]} RPM"
    line_disk = f"Espaço livre em disco: {data[11]} GiB"
    line_gpu_usage = f"Uso atual da GPU: {data[12]}%"
    line_gpu_energy = f"Consumo atual de energia elétrica pela GPU: {data[13]} W"
    line_gpu_temp = f"Temperatura atual da GPU: {data[14]} graus Celsius"
    line_gpu_fan_speed = f"Velocidade da ventoinha da GPU: {data[15]} RPM"
    line_timestamp = f"Momento de captura: {data[16].strftime('%Y-%m-%d %H:%M:%S')}"

    print(f"""
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
    """)

def store(data, nome_arquivo):
    if(not os.path.exists(nome_arquivo)):
        with open(nome_arquivo, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile, delimiter=';')
                writer.writerow(["user", "cpu_percent", "cpu_frequency", "ram_percent", "swap_memory_total", "swap_memory_used", "swap_memory_percent", "upload_speed", "download_speed", "temperature", "fans_speed", "disk", "gpu_usage", "gpu_energy", "gpu_temp", "gpu_fan_speed", "timestamp"])
    
    with open(nome_arquivo, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15], data[16]])
   


query = "select e.id , m.mac_address from empresa e join farm f on f.id_empresa = e.id join maquina m on m.id_farm = f.id where m.mac_address = (%s);"
cursor.execute(query , [user])
resultado1 = cursor.fetchall()
id_empresa = resultado1[0][0]
print(id_empresa)
if(resultado1):
    try:
        print("mac no banco")
        query2 = "select c.nome from empresa e join farm f on f.id_empresa = e.id join maquina m on m.id_farm = f.id join maquina_componente mc on mc.id_maquina = m.id join componente c on c.id = mc.id_componente where m.mac_address = (%s) and mc.monitorado = 1;"
        cursor.execute(query2, [user])
        resultado2 = cursor.fetchall()
        print(resultado2)
        capture([1 if 'cpu' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'cpu' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'ram' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'swap' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'swap' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'swap' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'rede' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'rede' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'temperatura' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'ventoinha' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'disco' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'gpu' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'gpu' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'gpu' in (i[0].lower() for i in resultado2) else 0,
                    1 if 'gpu' in (i[0].lower() for i in resultado2) else 0])
    except KeyboardInterrupt:
        print("Encerrado")
else:
    print("seu mac nao esta no banco")





