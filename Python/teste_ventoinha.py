# sudo apt install lm-sensors
# sudo sensors-detect   # responda YES para as perguntas
# sensors

import psutil

fans = psutil.sensors_fans()  # só funciona em Linux
if fans:
    for chip, entries in fans.items():
        for e in entries:
            print(f"{chip} {e.label or 'fan'}: {e.current} RPM")
else:
    print("Nenhuma ventoinha exposta pelo sistema")