
library(ggplot2)

data_marcelo <- `1.2026.10.02_20.07.18.9c.c7.d3.fe.61.5e`
data_yasmin <- `1.2026.10.02_20.21.23.9c.c7.d3.74.4d.a1` 
data_joao <- `1.2026.10.02_20.25.29.14.13.33.89.5d.4b`
data_minomo <- `1.2026.10.04_13.56.03.08.f9.7e.7f.a6.69`
data_lucas <- `1.2026.10.02_19.31.49.34.68.95.df.a0.09` 
data_vini <- `1.2026.10.02_19.04.38.10.ff.e0.67.07.0e`


data_geral <- rbind(data_marcelo , data_yasmin , data_minomo ,  data_lucas , data_joao , data_vini)

data_geral$timestamp_clean <- as.POSIXct(data_geral$timestamp)

print(colnames(data_marcelo))
print(colnames(data_yasmin))
print(colnames(data_minomo))

data_marcelo
data_minomo
data_yasmin
data_vini
data_lucas
data_joao


ncol(data_marcelo)
ncol(data_yasmin)
ncol(data_minomo)
ncol(data_lucas)
ncol(data_joao)
ncol(data_vini)



#opcional (se quiser mudar o mac pelo nome descomente)
#data_geral$user[data_geral$user == '9c:c7:d3:fe:61:5e'] <- "marcelo"
#data_geral$user[data_geral$user == '9c:c7:d3:74:4d:a1'] <- "yasmin"
#data_geral$user[data_geral$user == '14:13:33:89:5d:4b'] <- "joao"
#data_geral$user[data_geral$user == '08:f9:7e:7f:a6:69'] <- "minomo"
#data_geral$user[data_geral$user == '34:68:95:df:a0:09'] <- "lucas"
#data_geral$user[data_geral$user == '10:ff:e0:67:07:0e'] <- "vini"



library(ggplot2)

maquinas <- unique(data_geral$user)

df_alertas <- data.frame()

for(j in maquinas){
  linhas_usuario <- which(data_geral$user == j)
  
  contadorcpu <- 0
  contadorram <- 0
  contadortemp <- 0
  contadorgpu <- 0
  contadorswap <- 0
  contadordisco <- 0
  
  cpu_vetor   <- na.omit(data_geral$cpu_percent[linhas_usuario])
  ram_vetor   <- na.omit(data_geral$ram_percent[linhas_usuario])
  temp_vetor  <- na.omit(data_geral$temperature[linhas_usuario])
  gpu_vetor   <- na.omit(data_geral$gpu_usage[linhas_usuario])
  swap_vetor  <- na.omit(data_geral$swap_memory_percent[linhas_usuario])
  disco_vetor <- na.omit(data_geral$disk[linhas_usuario])
  
  for(i in cpu_vetor){
    if(i > 95 | i < 70){
      contadorcpu <- contadorcpu + 1
    }
  }
  
  for(i in ram_vetor){
    if(i > 95 | i < 80){
      contadorram <- contadorram + 1
    }
  }
  
  for(i in temp_vetor){
    if(i > 85){
      contadortemp <- contadortemp + 1
    }
  }
  
  for(i in gpu_vetor){
    if(i > 90){
      contadorgpu <- contadorgpu + 1
    }
  }
  
  for(i in swap_vetor){
    if(i > 50){
      contadorswap <- contadorswap + 1
    }
  }
  
  for(i in disco_vetor){
    if(i > 90){
      contadordisco <- contadordisco + 1
    }
  }
  total_alertas_maquina <- contadorcpu + contadorram + contadortemp + contadorgpu + contadorswap + contadordisco
  
  linha <- data.frame(
    user = j,
    componente = c("CPU", "RAM", "Temperatura", "GPU", "Swap", "Disco"),
    Freq = c(contadorcpu, contadorram, contadortemp, contadorgpu, contadorswap, contadordisco),
    total_maquina = total_alertas_maquina
  )

  
  df_alertas <- rbind(df_alertas, linha)
}


df_alertas




contadorgeral <- sum(df_alertas$Freq)
contador_base <- nrow(data_geral) * 6
leituras_normais <- contador_base - contadorgeral



(nrow(data_geral) - 2)
(ncol(data_geral) + 1)

data_geral$alerta_cpu  <- ifelse(data_geral$cpu_percent > 95 | data_geral$cpu_percent < 70, 1, 0)
data_geral$alerta_ram  <- ifelse(data_geral$ram_percent > 95 | data_geral$ram_percent < 80, 1, 0)
data_geral$alerta_temp <- ifelse(data_geral$temperature > 85, 1, 0)
data_geral$alerta_gpu  <- ifelse(data_geral$gpu_usage > 90, 1, 0)            
data_geral$alerta_swap <- ifelse(data_geral$swap_memory_percent > 50, 1, 0)  
data_geral$alerta_disco <- ifelse(data_geral$disk > 800, 1, 0) 

data_geral <- data_geral[order(data_geral$timestamp_clean), ]


contadorcpu



#CASO 1 - Derretimento de 90 GPU por causa de superaquecimento e derretimento nos cabos SATA
#https://www.estadao.com.br/tecmundo/big-techs/243000-minerador-tem-prejuizo-r-500-mil-incendio-placas-de-video/
#Derretimento das placas de video por causa da alta temperatura (não monitorada, gerou um prejuizo de 500 mil)


ggplot(data = NULL , aes(x = c(65, 70, 75, 90, 120, 180, 230), y = c(0, 0, 1, 10, 15, 60, 500))) +
  geom_line(color = "red" , size = 1 ) +
  geom_point(size = 3) +
  labs( title = "prejuizo x temperatura" , x = "temperatura" , y = "prejuizo (mil)")


#CASO 2 - Incendio em um fazendo de mineração de bitcoin na Tailândia
#https://qz.com/293418/an-enormous-bitcoin-mine-went-up-in-flames-affecting-the-entire-network
#Incendio causa prejuizo enorme para fazenda de mineração na Tailândia


ggplot(data = NULL, aes(x = factor(c("oct26", "oct28" , "oct30" ,"nov1" , "nov2" , "nov3" , "nov4"), levels = c("oct26", "oct28" , "oct30" ,"nov1" , "nov2" , "nov3" , "nov4")), y = c(230, 270 , 340 , 285 , 300 , 260 ,  300 ) , group = 1)) + 
  geom_line(color = "red" , size = 1) +
  geom_point(size = 3) + 
  labs(title = "hashrate x dias" , subtitle = "Gráfico da "  , y = "hashrate" , x = "dias")

ggplot(data = NULL, aes(x = c(30 , 40 , 60 , 70 ,80 , 120) , y = c(2000 , 1500 , 1000 , 500 , 100 , 50))) +
  geom_line( color = "red" , size = 1) + 
  geom_point(size =3) + 
  labs(title = "ventoinha x temperatura" , x = "temperatura" , y = "ventoinha (rpm)")

#INICIO ANALISE


barplot(table(data_geral$user), las = 2, col = "grey",
        main = "qtd leitura por maquina")



plot(data_geral$timestamp_clean , data_geral$cpu_percent , col = ifelse(data_geral$cpu_percent > 95 | data_geral$cpu_percent < 70, "red", "green") , pch = 19 , main = "cpu geral x tempo")
abline(h = c(70 ,95) , col = "green" , lwd = 4)


hist(data_geral$cpu_percent, col = "blue", breaks = 20,
     xlim = c(0, 100), main = "Distribuição CPU",
     xlab = "Uso da CPU (%)", ylab = "Quantidade de leituras")
abline(v = c(70, 95), col = "green", lwd = 4)




plot(data_geral$cpu_percent, data_geral$cpu_frequency, pch = 19,
     col = "green",
     main = "A CPU mantém a frequência quando está sobrecarregada?",
     xlab = "CPU (%)", ylab = "Freq da CPU")


boxplot(cpu_percent ~ user, data = data_geral, las = 2, col = "blue",
        ylim = c(0, 100), main = "Uso de CPU por máquina")
abline(h = 95, col = "red", lwd = 4)


faixas_cpu_per <- cut(data_geral$cpu_percent, breaks = c(0, 20, 40, 90, 100), 
                      labels = c("baixo", "medio", "alto", "muito alto"), ordered_result = TRUE)

faixas_cpu_per




pie(table(faixas_cpu_per) , main = "faixas - cpu (%)",  col = c("red" , "blue" , "green" , "purple"))
legend("topright" , legend = c("baixo" , "medio" , "alto" , "muito alto") , col = c("red" , "blue" , "green" , "purple") , pch = 19)





faixas_ram_per <- cut(data_geral$ram_percent, breaks = c(0, 40, 70, 95, 100), 
                      labels = c("baixo", "medio", "alto", "muito alto"), ordered_result = TRUE)
pie(table(faixas_ram_per) , main = "faixas - ram (%)",  col = c("red" , "blue" , "green" , "purple"))
legend("topright" , legend = c("baixo" , "medio" , "alto" , "muito alto") , col = c("red" , "blue" , "green" , "purple") , pch = 19)






ggplot(data_geral, aes(data_geral$timestamp_clean, data_geral$cpu_percent, color = user)) +
  geom_point(aes(col = ifelse(cpu_percent > 85, "Alerta (>85%)", user))) +
  geom_hline(yintercept  = 85, col = "red") +
  labs(
    title = "cpu x maq",
    col = "users"
  )



plot(data_geral$ram_percent , data_geral$swap_memory_percent , col = "green", pch = 19 )


boxplot(ram_percent ~ user, data = data_geral, las = 2, col = "blue",
        ylim = c(0, 100), main = "Uso ram por máquina")
abline(h = c(80 , 95), col = "red", lwd = 4)

plot(data_geral$timestamp_clean , data_geral$ram_percent , col = ifelse(data_geral$ram_percent < 80 | data_geral$ram_percent > 95 , "red" , "green") , pch = 19  , ylim = c(0 , 100))
abline(h = c(80 , 95) , col = "green" , lwd = 2)
legend("top" , legend = paste("alertas: " , sum(data_geral$alerta_ram)))




ggplot(data_geral, aes(data_geral$ram_percent, data_geral$swap_memory_percent, color = user)) +
  geom_point() +
  labs(title = "swap x ram por maq")


plot(data_geral$timestamp_clean , data_geral$ram_percent , type = "l" , ylim = c(0 ,100) , col = "blue" , lwd = 2 , main = "swap x ram por tempo")
lines(data_geral$timestamp_clean , data_geral$swap_memory_percent , col = "red" , lwd = 2)
legend("top" , legend = c("swap" , "ram") , col = c("red" , "blue") , pch = 19)


plot(data_geral$ram_percent, data_geral$swap_memory_percent, , ylim = c(0, 80), pch = 19, col = "blue",
     main = "URAM x Swap")
abline(v = 80, col = "orange")
abline(h = 50, col = "red")





plot(data_geral$upload_speed , data_geral$download_speed , col = "blue" , pch = 19 , main = "DOWNLOAD X UPLOAD")
abline(lm(data_geral$download_speed ~ data_geral$upload_speed) , col = "red" , lwd = 3)

print(sum(data_geral$alerta_ram))




sum(data_geral$alerta_cpu)
sum(data_geral$alerta_ram)
sum(data_geral$alerta_temp , na.rm = TRUE)
sum(data_geral$alerta_disco)
sum(data_geral$alerta_swap)
sum(data_geral$alerta_gpu)

pie(c(sum(data_geral$alerta_cpu) , sum(data_geral$alerta_ram) , sum(data_geral$alerta_temp , na.rm = TRUE) ,sum(data_geral$alerta_gpu) , sum(data_geral$alerta_swap) , sum(data_geral$alerta_disco) ) , col = c("red" , "blue" , "green" , "purple" , "pink" , "cyan") , labels = c( sum(data_geral$alerta_cpu) , sum(data_geral$alerta_ram) , sum(data_geral$alerta_temp) ,sum(data_geral$alerta_gpu) , sum(data_geral$alerta_swap) , sum(data_geral$alerta_disco)) , main = "distribuição de alertas por componente" )
legend("topright", legend = c("alerta cpu" , "alerta ram" , "alerta temperatura" , "alerta gpu" , "alerta swap" , "alerta disco"), col = c("red" , "blue" , "green"  , "purple" , "pink" , "cyan") , pch = 19)

 
pie(c((contador_base - contadorgeral), contadorgeral), col = c("green", "red"),labels = c(leituras_normais, contadorgeral),main = "alertas gerais") +
legend("topright", legend = c("Leituras normais", "Leituras que deram alerta"), col = c("green", "red"), pch = 19)


totais <- c()
for(m in df_alertas$user) {
  totais[m] <- sum(df_alertas$Freq[df_alertas$user == m])
}


pie(totais,
  col = c("red", "blue", "green", "purple", "orange", "cyan"),
  main = "Distribuição de Alertas por Dispositivo", 
  labels = totais
)
legend("topright" , legend = names(totais) , col = c("red", "blue", "green", "purple", "orange", "cyan") , pch = 19)


totais['10:ff:e0:67:07:0e']

contador_base 

contadorgeral


table(data_geral$user)


ggplot(df_alertas, aes(componente, user, fill = Freq)) +
  geom_tile() +
  geom_text(aes(label = Freq)) +
  scale_fill_gradient(low = "pink", high = "red") +
  labs(title = "Matriz componente x alertas")


ggplot(data_geral, aes(cpu_percent, col = user)) +
  geom_density() +
  labs(title = "cpu x usuario")


ggplot(data_geral, aes(cpu_percent, cpu_frequency, color = user) ) +
  geom_point() +
  labs(title = "cpu (%) x cpu freq")



ggplot(data_geral, aes(download_speed, cpu_percent, color = user)) +
  geom_point() + geom_smooth()




ggplot(data_geral, aes(format(timestamp_clean, "%H:00"),user, fill = ave(cpu_percent, user, format(timestamp_clean, "%H:00")))) +
  geom_tile() +
  scale_fill_gradient(low = "green", high = "red") +
  labs(title = "cpu media por hora e máquina" , fill = "cpu")




ggplot(data_geral, aes(x = ram_percent, y = swap_memory_percent, color = user)) +
  geom_point() +
  labs(title = "Uso de RAM x Uso de Memória swap")

ggplot(data_geral, aes(format(timestamp_clean, "%H:00"), user, fill = ave(ram_percent, user,  format(timestamp_clean, "%H:00") ))) +
  geom_tile() +
  scale_fill_gradient(low = "green" , high = "red") +
  labs(title = "ram por hora e maquina" , fill = "media ram")




ggplot(data_geral, aes(upload_speed, download_speed, color = user )) +
  geom_point() +
  geom_smooth() +
  labs(title = "download x upload por MAC")




ggplot(data_geral, aes(ram_percent, swap_memory_percent, color = disk)) +
  geom_point() +
  scale_color_gradient(low = "pink", high = "red") +
  labs(
    title = "RAM x Swap x Disco")



ggplot(df_alertas, aes(user, Freq, fill = componente)) +
  geom_col() +
  labs(
    title = "alertas por Máquina"
  )



ggplot(data_geral, aes(fans_speed, temperature, color = user)) +
  geom_point() +
  labs(title = "Rotação (RPM) vs Temperatura")



ggplot(df_alertas, aes(componente, Freq,
                       fill = ifelse(ave(Freq, componente, FUN = sum) > 30, "critico", "normal"))) +geom_col() + scale_fill_manual(values = c("critico" = "red", "normal" = "green")) +
labs(fill = "status")




barplot(table(format(data_geral$timestamp_clean[which(data_geral$cpu_percent > 80 | data_geral$ram_percent > 80)], "%H")),
        col = "red",
        main = "Em quais horas a CPU ou a RAM passam de 80%?")




barplot(colSums(is.na(data_geral)), las = 2, col = "red",
        main = "Leituras que não foram coletadas por sensor")

heatmap(cor(data_geral[c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")]))




print(as.table(cor(data_geral[, c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")], use = "pairwise.complete.obs")))


print(as.data.frame(as.table(cor(data_geral[ c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")], use = "pairwise.complete.obs"))))


ggplot(as.data.frame(as.table(cor(data_geral[ c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")], use = "pairwise.complete.obs"))),
       aes(Var1, Var2, fill = Freq)) +
  geom_tile() +
  labs(title = "coorelacoes (1 = positiva / -1 = negativa)" ) +
  scale_fill_gradient2(low = "blue", high = "red")


heatmap(
  cor(data_geral[ c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")
] ),
  col = hcl.colors(100, "Blue-Red")
) +
  legend("topright" , legend = c("coorelacao positiva (1)" , "coorelacao negativa (-1)") , col = c("red" , "blue") , pch = 19   )


print(as.table(cor(data_geral[, c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")], use = "pairwise.complete.obs")))



coorelacao <- lm(data_geral$swap_memory_percent ~ data_geral$ram_percent, data = data_geral)         

summary(coorelacao)

ggplot(data_geral, aes(ram_percent, swap_memory_percent, color = user)) +
  geom_point() +
  geom_smooth(method = "lm", color = "red") +
  labs(
    title = "Uso de RAM com Swap",
  ) 

predict(coorelacao)



ggplot(data_geral, aes(ram_percent, cpu_percent, color = user)) +
  geom_point(size = 4) +
  theme_minimal()


ggplot(data_geral , aes(timestamp_clean , cpu_percent , color = user)) +
  geom_point(size = 3)

