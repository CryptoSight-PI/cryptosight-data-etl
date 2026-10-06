  #prototipo R CRYPTOSIGHT
  library(ggplot2)
  


  data <- `1.2026.10.02_20.07.18.9c.c7.d3.fe.61.5e`
  #data <- `data.(2)`  

  
  
  
  df_alertas <- c()
  
  contadorcpu <- 0
  contadorram <- 0
  contadortemp <- 0
  contadorgpu <- 0
  contadorswap <- 0
  contadordisco <- 0
  
  
  
  
  
  cpu_vetor   <- na.omit(data$cpu_percent)
  ram_vetor   <- na.omit(data$ram_percent)
  temp_vetor  <- na.omit(data$temperature)
  gpu_vetor   <- na.omit(data$gpu_usage)
  swap_vetor  <- na.omit(data$swap_memory_percent)
  disco_vetor <- na.omit(data$disk)
  
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
    if(i > 800){
      contadordisco <- contadordisco + 1
    }
  }
  total_alertas_maquina <- contadorcpu + contadorram + contadortemp + contadorgpu + contadorswap + contadordisco
  
  linha <- data.frame(
    user = data$user[1],
    componente = c("CPU", "RAM", "Temperatura", "GPU", "Swap", "Disco"),
    Freq = c(contadorcpu, contadorram, contadortemp, contadorgpu, contadorswap, contadordisco),
    total_maquina = total_alertas_maquina
  )
  
  
  df_alertas <- rbind(df_alertas, linha)
  }
  
  
  df_alertas
  
  data$timestamp_clean <- as.POSIXct(data$timestamp)
  
  contadorgeral <- sum(df_alertas$Freq)
  contador_base <- nrow(data) * 6
  leituras_normais <- contador_base - contadorgeral
  
  
  
  data$alerta_cpu  <- ifelse(data$cpu_percent > 95 | data$cpu_percent < 70, 1, 0)
  data$alerta_ram  <- ifelse(data$ram_percent > 95 | data$ram_percent < 80, 1, 0)
  data$alerta_temp <- ifelse(data$temperature > 85, 1, 0)
  data$alerta_gpu  <- ifelse(data$gpu_usage > 90, 1, 0)            
  data$alerta_swap <- ifelse(data$swap_memory_percent > 50, 1, 0)  
  data$alerta_disco <- ifelse(data$disk > 800, 1, 0) 
  




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

  plot(data$timestamp_clean , data$cpu_percent , col = ifelse(data$cpu_percent > 95 | data$cpu_percent < 70, "red", "green") , pch = 19 , main = "cpu geral x tempo" , type = "o" )
  abline(h = c(70 ,95) , col = "green" , lwd = 4)
  
  
  hist(data$cpu_percent, col = "blue", breaks = 20,
       xlim = c(0, 100), main = "Distribuição CPU",
       xlab = "Uso da CPU (%)", ylab = "Quantidade de leituras")
  abline(v = c(70, 95), col = "green", lwd = 4)
  
  
  
  
  
  barplot(table(format(data$timestamp_clean[which(data$cpu_percent > 80 | data$ram_percent > 80)], "%M")),
          col = "red",
          main = paste("Minutos da hora", format(data$timestamp_clean[1], "%H:00"), "com CPU ou RAM > 80%"),
          xlab = "Minutos")
  
  
  barplot(
    table(factor(format(data$timestamp_clean[data$cpu_percent > 80 | data$ram_percent > 80], "%M"), levels = sprintf("%02d", 0:59))),
    col = "red",
    main = paste("Minutos da hora", format(data$timestamp_clean[1], "%H:00"), "com CPU ou RAM > 80%"),
    xlab = "Minutos (00-59)",
    ylab = "Freq",
    las = 2
  )
  
  
  
  pie(c(sum(data$alerta_cpu) , sum(data$alerta_ram) , sum(data$alerta_temp , na.rm = TRUE) ,sum(data$alerta_gpu) , sum(data$alerta_swap) , sum(data$alerta_disco) ) , col = c("red" , "blue" , "green" , "purple" , "pink" , "cyan") , labels = c( sum(data$alerta_cpu) , sum(data$alerta_ram) , sum(data$alerta_temp) ,sum(data$alerta_gpu) , sum(data$alerta_swap) , sum(data$alerta_disco)) , main = "distribuição de alertas por componente" )
  legend("topright", legend = c("alerta cpu" , "alerta ram" , "alerta temperatura" , "alerta gpu" , "alerta swap" , "alerta disco"), col = c("red" , "blue" , "green"  , "purple" , "pink" , "cyan") , pch = 19)
  
  
  
  
  pie(c((contador_base - contadorgeral), contadorgeral), col = c("green", "red"),labels = c(leituras_normais, contadorgeral),main = "alertas gerais")
  legend("topright", legend = c("Leituras normais", "Leituras que deram alerta"), col = c("green", "red"), pch = 19)
  

  
  boxplot(data$cpu_percent , ylim = c(0 , 100) , col = "blue" , main = "grafico boxplot cpu (%)")
  
  
  faixas_cpu_per <- cut(data$cpu_percent, breaks = c(0, 20, 70, 80, 100), 
                                      labels = c("baixo", "medio", "alto", "muito alto"), ordered_result = TRUE)
  
  faixas_cpu_per
  
  sum(table(faixas_cpu_per)[faixas_cpu_per == "baixo"] , na.rm = TRUE ) 
  sum(table(faixas_cpu_per)[faixas_cpu_per == "muito alto"] , na.rm = TRUE )
  
  pie(table(faixas_cpu_per) , main = "faixas - cpu (%)" , col = c("blue" , "red" , "green" , "purple") , labels = c(sum(table(faixas_cpu_per)[faixas_cpu_per == "baixo"] , na.rm = TRUE ) , sum(table(faixas_cpu_per)[faixas_cpu_per == "medio"] , na.rm = TRUE ) , sum(table(faixas_cpu_per)[faixas_cpu_per == "alto"] , na.rm = TRUE ) , sum(table(faixas_cpu_per)[faixas_cpu_per == "muito alto"] , na.rm = TRUE ) ) )
  legend( "topright" , legend = c("baixo", "medio", "alto", "muito alto") , col = c("blue" , "red" , "green" , "purple") , pch = 19 )
  
  
  barplot(table(faixas_cpu_per))
  
  data$timestamp_clean <- as.POSIXct(data$timestamp)
  
  plot(data$timestamp_clean, data$cpu_percent, 
       type = "o", col = "green", pch = 19, lwd = 3,
       ylim = c(0, 100), 
       xlab = "Timestamp", ylab = "(%)", 
       main = "CRYPTOSIGHT - CPU X RAM")
  
  lines(data$timestamp_clean, data$ram_percent, 
        type = "o", col = "blue", pch = 19, lwd = 3)
  
  legend("topright", legend = c("CPU", "RAM"), 
         col = c("green", "blue"), pch = 19, lwd = 3)
  
  

  plot(data$timestamp_clean, data$ram_percent, type = "o", col = "blue", lwd = 2,
       ylim = c(0, 100), xlab = "Tempo", ylab = "Percentual (%)", main = "Memória X Swap")
  lines(data$timestamp_clean, data$swap_memory_percent, type = "o", col = "red", lwd = 2)
  legend("topright", legend = c("RAM", "SWAP"), col = c("blue", "red"), lty = 1, lwd = 2)

  
  
  
  
  plot(data$ram_percent, data$swap_memory_percent, 
       xlab = "Uso de RAM (%)", ylab = "Uso de SWAP (%)",
       main = "RAM vs SWAP",
       pch = 19, col = c("red") , xlim = c(85, 95) , ylim = c(10,20))
  
  plot(data$timestamp_clean, data$swap_memory_used, 
       type = "o", col = "green", pch = 19, lwd = 3,
       ylim = c(0, 100), 
       xlab = "Timestamp", ylab = "(%)", 
       main = "CRYPTOSIGHT - SWAP")
  lines(data$timestamp_clean, data$swap_memory_percent, 
        type = "o", col = "blue", pch = 19, lwd = 3)
  lines(data$timestamp_clean, data$swap_memory_total, 
        type = "o", col = "red", pch = 19, lwd = 3)
  legend("topright", legend = c("Uso", "Percentual", "Total"), col = c("green", "blue", "red") , lwd = 4)
  
  
  
  
  
  
  
  
  plot(data$timestamp_clean, data$upload_speed, 
       type = "o", col = "green", pch = 19, lwd = 2,
       ylim = c(0, 100), 
       xlab = "Timestamp", ylab = "(%)", 
       main = "CRYPTOSIGHT - Upload x download")
  lines(data$timestamp_clean, data$download_speed, 
        type = "o", col = "blue", pch = 19, lwd = 2)
  legend("topright", legend = c("Upload", "download"), col = c("green", "blue") , lwd = 4)
         
     
  
  
  
  
  
  plot(data$timestamp_clean, data$cpu_percent, 
       type = "o", col = "green", pch = 19, lwd = 3,
       ylim = c(0, 100), 
       xlab = "Timestamp", ylab = "(%)", 
       main = "CRYPTOSIGHT - CPU X TEMPO")
  lines(data$timestamp_clean, data$cpu_frequency, 
        type = "o", col = "blue", pch = 19, lwd = 2)
  legend("topright", legend = c("(%)", "freq"), col = c("green", "blue") , lwd = 4)

  
  
  
  
boxplot(data$cpu_percent)


plot(data$timestamp_clean ,data$cpu_percent,  
     pch = 19, col = ifelse(data$cpu_percent > 80 | data$cpu_percent < 60, "red", "darkgreen"),
      ylim = c(0, 100), type = "o",
     xlab = "timestamp", ylab = "cpu (%)",
     main = "CRYPTOSIGHT - CPU")
abline( h = c(60,80) , col = "green" , lwd = 3)
legend("topleft" ,legend = paste("total alertas: " , contadorcpu))
legend("topright", legend = c("porcentagem de cpu"), col = c("darkgreen") , lwd = 4)




plot(data$timestamp_clean , data$ram_percent , col = "purple" , pch = 19 , lwd = 2 , type = "o" , ylim = c(0, 100) , main = "cryptosight - ram")
abline( h = 90 , col = "red" , lwd = 2)
legend("topleft" ,legend = paste("total alertas: " , contadorram))
legend("topright", legend = c("porcentagem de ram"), col = c("purple") , lwd = 4)



plot(data$timestamp_clean, data$cpu_percent, 
     type = "o", col = "green", pch = 19, lwd = 3,
     ylim = c(0, 100), 
     xlab = "Timestamp", ylab = "(%)", 
     main = "CRYPTOSIGHT - METRICAS")
lines(data$timestamp_clean, data$cpu_frequency, 
      type = "o", col = "blue", pch = 19, lwd = 2)
lines(data$timestamp_clean, data$ram_percent, type = "o" , col = "red" , pch = 19, lwd = 2)
lines(data$timestamp_clean, data$swap_memory_total, type = "o" , col = "purple" , pch = 19 , lwd = 2)
lines(data$timestamp_clean, data$swap_memory_used, type =  "o" , col = "pink" , pch = 19, lwd = 2)
lines(data$timestamp_clean , data$swap_memory_percent, type = "o" , col = "cyan" , pch = 19 , lwd = 2)
lines(data$timestamp_clean, data$upload_speed, type = "o", col = "brown" , pch = 19 , lwd = 2)
lines(data$timestamp_clean, data$download_speed, type = "o" , col = "yellow" , pch = 19 , lwd = 2)
legend("topright", legend = c("cpu(%)" , "cpu(freq)" , "ram" , "swap_total" , "swap used" , "swap(%)", "upload" , "download"), col = c("green" , "blue" , "red" , "purple" , "pink" , "cyan" , "brown" , "yellow") , lwd = 4)




plot(data$timestamp_clean , data$fans_speed , type = "o" , col = "green" , pch = 19 , lwd = 2 , main = "ventoinha x temperatura")
lines(data$timestamp_clean , data$temperature , type = "o" , col = "purple" , pch = 19 , lwd = 2)
legend("topright", legend = c("vel ventoinha" , "temperatura"), col = c("green" , "purple" ) , lwd = 4)



plot(data$upload_speed , data$download_speed , col = "blue" , pch = 19 , main = "DOWNLOAD X UPLOAD")
abline(lm(download_speed ~ upload_speed, data = data) , col = "red" , lwd = 3)





ggplot(df_alertas, aes(componente, user ,  fill = Freq)) +
  geom_tile() +
  geom_text(aes(label = Freq)) +
  scale_fill_gradient(low = "pink", high = "red") +
  labs(title = "Matriz componente x alertas")



ggplot(df_alertas, aes(user, Freq, fill = componente)) +
  geom_col() +
  labs(title = "Ranking de Alertas por Máquina",fill = "Componente")




ggplot(data, aes(ram_percent, swap_memory_percent, color = user)) +
  geom_point(alpha = 0.6, size = 2) +
  geom_smooth(method = "lm", color = "red") +
  labs(
    title = "Uso de RAM com Swap",
  ) 


ggplot(as.data.frame(as.table(cor(data[, c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")], use = "pairwise.complete.obs"))),
       aes(Var1, Var2, fill = Freq)) +
  geom_tile() +
  scale_fill_gradient2(low = "blue", high = "red")

print(as.data.frame(as.table(cor(data[, c("cpu_percent", "ram_percent", "swap_memory_percent", "disk", "cpu_frequency", "upload_speed", "download_speed")], use = "pairwise.complete.obs"))) )

print(cor(data$cpu_frequency , data$disk))


ggplot(df_alertas, aes(componente, Freq,
                       fill = ifelse(ave(Freq, componente, FUN = sum) > 30, "critico", "normal"))) +geom_col() + scale_fill_manual(values = c("critico" = "red", "normal" = "green")) +
  labs(fill = "status")



ggplot(data, aes(ram_percent, swap_memory_percent, color = disk)) +
  geom_point() +
  scale_color_gradient(low = "pink", high = "red") +
  labs(
    title = "RAM x Swap x Disco")



ggplot(data, aes(upload_speed, download_speed, color = user )) +
  geom_point() +
  geom_smooth() +
  labs(title = "download x upload por MAC")


ggplot(data, aes(format(timestamp_clean, "%H:00"),user, fill = ave(cpu_percent, user, format(timestamp_clean, "%H:00")))) +
  geom_tile() +
  scale_fill_gradient(low = "green", high = "red") +
  labs(title = "cpu media por hora" , fill = "cpu")


ggplot(data , aes(ram_percent , cpu_percent ))+
  geom_point(size = 4 , colour = "purple")

ggplot(data , aes(timestamp_clean , cpu_percent)) +
  geom_point(size = 4) + geom_smooth( colour = "green")  + 
  theme_minimal()  +
  labs(title = "cpu x tempo")


