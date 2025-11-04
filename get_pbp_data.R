library(tidyverse)
library(bigballR)
library(arrow)

prev_day <- (Sys.Date())-1
print(paste0("Scraping games for ", prev_day)) 

if (!dir.exists("data/2025-26"))
  dir.create("data/2025-26")

games <- get_date_games(date = prev_day)
print(games)

time <- c(5:8)

pbp_dfs <- list()

i <- 1
while(i <= nrow(games)) {
  pbp <- get_play_by_play(games[i, 'GameID'])
  pbp_dfs[[i]] <- pbp
  
  sleep <- sample(time)[1]
  print(paste0("Resting for ", sleep, " seconds..." ))
  Sys.sleep(sleep)  

  i <- i + 1
}

pbp_all <- bind_rows(pbp_dfs)

write_parquet(pbp_all, paste0("data/2025-26/",  "pbp_", gsub("-", "_", prev_day), ".parquet"))




