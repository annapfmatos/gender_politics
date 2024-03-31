################################################################################
#
#       DPI 410 Final Paper
#       
#    TITLE: 1. TSE Data
#    MEMBERS: Alexa Gonzalez, Claudia Velarde, Sara Wong Becerra, Kathy
#             Gutierrez, Anna Matos
#
#    VERSION HYSTORY: 03/25/2024, 08/02/2024
#
#    DESCRIPTION: this code reads, cleans and makes the data set of electoral
#                 data from Brazil ready for merging.
#
#
################################################################################


##### ENVIRONMENT --------------------------------------------------------------

## Libraries
if (!require(tidyverse)) install.packages("tidyverse"); library(tidyverse)
if (!require(sf)) install.packages("sf"); library(haven)


## Working directories 

# Main data folder
setwd("C:/Users/annap/Desktop/Harvard/2nd_sem/DPI 410/Final paper/datos")

# TSE folder with electoral data zipfiles
tsewd <- paste0(
  "C:/Users/annap/Desktop/Harvard/2nd_sem/DPI 410/Final paper/datos/tse")

# IBGE folder with Brazilian Census Bureau data
ibgewd <- paste0(
  "C:/Users/annap/Desktop/Harvard/2nd_sem/DPI 410/Final paper/datos/ibge")



##### 0. DATA EXTRACTION -------------------------------------------------------

# Creates empty lists to store data, per type of data.
l_vote_cand <- list()    # (1) nominal votes per candidate
#l_electorate <- list()   # (2) descriptive data on electorate, per municipality
l_vote_detail <- list()  # (3) Details about voting in each municipality
l_candidate <- list()   # (4) details for every candidate in Municipal Elections


# Vector with file names, per type of data.
v_vote_cand <- list.files(path = tsewd, pattern = "^votacao_candidato_munzona_.*csv$")
#v_electorate <- list.files(path = tsewd, pattern = "^perfil_eleitorado_.*csv$")
v_vote_detail <- list.files(path = tsewd, pattern = "^detalhe_votacao_munzona_.*csv$")
v_candidate <- list.files(path = tsewd, pattern = "^consulta_cand_.*csv$")



##### 1. VOTES FOR EACH CANDIDATE ----------------------------------------------

# Function to read .csv files in the TSE
f_csv_vote_cand <- function(x) {
  read.csv(
    paste0(tsewd, "/", x), header = T, sep = ";", colClasses = "character",
    na.strings = "#NULO", fileEncoding = "latin1") |>
    
    # Select only candidates running as mayor
    filter(DS_CARGO == "Prefeito") |>
    as_tibble() |>
    dplyr::select(
      -c(
        "DT_GERACAO", "HH_GERACAO", "CD_TIPO_ELEICAO", "NM_TIPO_ELEICAO",
        "DS_ELEICAO", "DT_ELEICAO", "TP_ABRANGENCIA", "SG_UF", "NM_UE",
        "NM_MUNICIPIO", "CD_CARGO", "SQ_CANDIDATO", "NM_CANDIDATO",
        "NM_URNA_CANDIDATO", "NM_SOCIAL_CANDIDATO", "CD_SITUACAO_CANDIDATURA",
        "CD_DETALHE_SITUACAO_CAND", "TP_AGREMIACAO", "NR_PARTIDO", "SG_PARTIDO",
        "NM_PARTIDO", "SQ_COLIGACAO", "NM_COLIGACAO", "DS_COMPOSICAO_COLIGACAO",
        "ST_VOTO_EM_TRANSITO", "CD_SIT_TOT_TURNO"))
}


# Apply the code to the file names vector
l_vote_cand <- map(v_vote_cand, f_csv_vote_cand)


# Remove further columns from specific datasets
l_vote_cand[[1]] <- l_vote_cand[[1]] |>
  select(
    -c(
      "NR_FEDERACAO", "NM_FEDERACAO", "DS_DETALHE_SITUACAO_CAND",
      "DS_COMPOSICAO_FEDERACAO", "NM_TIPO_DESTINACAO_VOTOS",
      "QT_VOTOS_NOMINAIS", "SG_FEDERACAO")) |>
  rename(
    DS_DETALHE_SITUACAO_CAND = DS_SITUACAO_CANDIDATURA,
    QT_VOTOS_NOMINAIS = QT_VOTOS_NOMINAIS_VALIDOS)
l_vote_cand[[6]] <- l_vote_cand[[6]] |>
  select(
    -c(
      "NR_FEDERACAO", "NM_FEDERACAO", "DS_SITUACAO_CANDIDATURA",
      "DS_COMPOSICAO_FEDERACAO", "NM_TIPO_DESTINACAO_VOTOS",
      "QT_VOTOS_NOMINAIS_VALIDOS", "SG_FEDERACAO"))
for (i in 2:5) {
  l_vote_cand[[i]] <- select(l_vote_cand[[i]], -c("DS_SITUACAO_CANDIDATURA"))
}


# Append all the datasets stored in the list into a single dataframe
df_vote_cand <- do.call(rbind, l_vote_cand) |>
  
  # Create ID variables, for merging
  mutate(
    id_cand = paste0(
      ANO_ELEICAO,"_",NR_TURNO,"_",CD_ELEICAO,"_",SG_UE,"_",NR_CANDIDATO),
    id_area = paste0(
      ANO_ELEICAO,"_",NR_TURNO,"_",CD_ELEICAO,"_",SG_UE,"_",NR_ZONA),
    id_total = paste0(
    ANO_ELEICAO,"_",NR_TURNO,"_",CD_ELEICAO,"_",SG_UE,"_",NR_ZONA,"_",NR_CANDIDATO)) |>
    #  ANO_ELEICAO,"_",NR_TURNO,"_",CD_ELEICAO,"_",SG_UE,"_",NR_ZONA,"_",NR_CANDIDATO),
    
    # Flag invalid observations #################################################### This part of the code only selected candidated that were able to WIN the election (they had valid candidacies)
    #valid_candidacy = case_when(
    #  DS_DETALHE_SITUACAO_CAND %in% c(
    #    "DEFERIDO", "DEFERIDO COM RECURSO", "SUB JUDICE") ~ 1,
    #  TRUE ~ 0)) |>
  
  # Change the variable name to avoid replication with another dataset
  rename(votes_won = QT_VOTOS_NOMINAIS)
  #rename(votes_won = QT_VOTOS_NOMINAIS) |>
  
  # Remove invalid observations (otherwise cause problems when merging)
  #filter(valid_candidacy == 1)




##### 3. ELECTION/VOTES INFORMATION --------------------------------------------

# Function to read .csv files in the TSE
f_csv_vote_detail <- function(x) {
  read.csv(
    paste0(tsewd, "/", x), header = T, sep = ";", colClasses = "character",
    fileEncoding = "latin1") |>
    
    
    # Select only candidates running as mayor
    filter(DS_CARGO == "Prefeito") |>
    select(
      -c(
        "DT_GERACAO", "HH_GERACAO", "CD_TIPO_ELEICAO", "NM_TIPO_ELEICAO",
        "DS_ELEICAO", "DT_ELEICAO", "TP_ABRANGENCIA", "SG_UF", "NM_UE",
        "NM_MUNICIPIO", "CD_CARGO")
    )
}


# Apply the function to read .csv files
l_vote_detail <- map(v_vote_detail, f_csv_vote_detail)


# Remove further columns from specific datasets
for (i in 1:5) {
  l_vote_detail[[i]] <- select(
    l_vote_detail[[i]],
    -c(
      "QT_SECOES", "QT_SECOES_AGREGADAS", "QT_APTOS_TOT", "QT_SECOES_TOT",
      "ST_VOTO_EM_TRANSITO", "QT_VOTOS_LEGENDA", "QT_VOTOS_PENDENTES",
      "QT_VOTOS_ANULADOS", "HH_ULTIMA_TOTALIZACAO", "DT_ULTIMA_TOTALIZACAO"))
}

l_vote_detail[[6]] <- l_vote_detail[[6]] |>
  select(
    -c(
      "QT_SECOES_PRINCIPAIS", "QT_SECOES_AGREGADAS", "QT_SECOES_NAO_INSTALADAS",
      "QT_TOTAL_SECOES", "QT_ELEITORES_SECOES_NAO_INSTALADAS",
      "ST_VOTO_EM_TRANSITO", "QT_VOTOS", "QT_VOTOS_VALIDOS", 
      "QT_TOTAL_VOTOS_ANULADOS", "QT_TOTAL_VOTOS_LEG_VALIDOS",
      "QT_VOTOS_LEGENDA_VALIDOS", "QT_VOTOS_NOMINAIS_CONVR_LEG",
      "QT_VOTOS_NOMINAIS_ANULADOS", "QT_VOTOS_LEGENDA_ANULADOS",
      "QT_TOTAL_VOTOS_ANUL_SUBJUD", "QT_TOTAL_VOTOS_NULOS",
      "QT_VOTOS_NULO_TECNICO", "QT_VOTOS_ANULADOS_APU_SEP",
      "HH_ULTIMA_TOTALIZACAO", "DT_ULTIMA_TOTALIZACAO", "QT_VOTOS_CONCORRENTES",
      "QT_VOTOS_LEGENDA_ANUL_SUBJUD", "QT_VOTOS_NOMINAIS_ANUL_SUBJUD")) |>
  rename(QT_VOTOS_NOMINAIS = QT_VOTOS_NOMINAIS_VALIDOS)


# Append all the datasets stored in the list into a single dataframe
df_vote_detail <- do.call(rbind, l_vote_detail) |>
  
  # Create ID variable, for merging
  mutate(id_area = paste0(
    ANO_ELEICAO,"_",NR_TURNO,"_",CD_ELEICAO,"_",SG_UE,"_",NR_ZONA))







##### 4. CANDIDATES CHARACTERISTICS --------------------------------------------

# Function to clean the datasets: candidates
f_candidate <- function(x) {
  read.csv(
    paste0(tsewd, "/", x), header = T, sep = ";", colClasses = "character",
    na.strings = c("#NE", "#NULO#"), fileEncoding = "latin1") |>
    
    # Select only candidates running as mayor based on the ballot id number
    #filter(str_count(NR_CANDIDATO) < 3) |>
    filter(DS_CARGO == "PREFEITO") |>
    select(
      ANO_ELEICAO, NR_TURNO, CD_ELEICAO, SG_UE, NR_CANDIDATO, DS_CARGO,
      DS_DETALHE_SITUACAO_CAND, DS_GENERO, DS_GRAU_INSTRUCAO, DS_ESTADO_CIVIL,
      DS_OCUPACAO, DS_SIT_TOT_TURNO, ST_REELEICAO
    )
}
### It is only possible to use ST_REELEICAO for elections > 2000.


# Apply the code to the file names vector
l_candidate <- map(v_candidate, f_candidate)


# Append all the datasets stored in the list into a single dataframe
df_candidate <- do.call(rbind, l_candidate) |>
  
  # Create ID variables, for merging
  mutate(id_cand = paste0(
    ANO_ELEICAO,"_",NR_TURNO,"_",CD_ELEICAO,"_",SG_UE,"_",NR_CANDIDATO)) |>
  group_by(ANO_ELEICAO, SG_UE, NR_CANDIDATO) |>
  mutate(
    last_run = case_when(
        max(NR_TURNO) == NR_TURNO ~ 1,
        max(NR_TURNO) != NR_TURNO ~ 0,
        TRUE ~ NA)) |>
  ungroup() |>
  
  
  # Keep the last registry of the candidate
  filter(last_run == 1 & !is.na(DS_SIT_TOT_TURNO))

# Perhaps leave the 2nd Run observations, paste the information within ID as well, so it would be possible to perfectly merge with vote_cand?


      
      
##### 5. MERGING THE THREE TABLES ----------------------------------------------

# Merging (keeping all obs from candidates with vote info)
tse <- left_join(df_vote_cand, df_candidate, by = "id_cand")
tse <- left_join(tse, df_vote_detail, by = "id_area")


# Merging (only exact matches)
tse2 <- inner_join(df_vote_cand, df_candidate, by = "id_cand")
tse2 <- inner_join(tse2, df_vote_detail, by = "id_area")
      


# Smell tests, check for duplicates & problems with merging
check_vote_cand <- df_vote_cand[duplicated(df_vote_cand[c("id_total")]),] # Passed!
check_vote_detail <- df_vote_detail[duplicated(df_vote_detail[c("id_area")]), ] # Passed!
check_candidate <- df_candidate[duplicated(df_candidate[c("id_cand")]), ] # 441 duplicates. See observation below.

test1 <- anti_join(df_vote_cand, df_candidate, by = "id_cand") # It is possible that those who are not "2nd Turn" and appear in this list are those who took part in elections that were cancelled.
test2 <- anti_join(df_vote_cand, df_vote_detail, by = "id_area") # Passed!
test3 <- group_by(df_candidate, id_cand) |> # 879 occurencies of duplicates. Possible that those were the vice-candidates that ran in case of renunciation or nullment of candidacy.
  filter(n() > 1) |>
  ungroup()
test4 <- filter(tse, DS_DETALHE_SITUACAO_CAND.y != DS_DETALHE_SITUACAO_CAND.x) |>
  select(DS_DETALHE_SITUACAO_CAND.y, DS_DETALHE_SITUACAO_CAND.x) # There are 782 observations where the candidate electoral situation is different from vote_cand and vote_detail datasets. It can be that one deals with before of TSE decisions, and one after?



##### 6. GENERAL DATA WRANGLING ------------------------------------------------

tse <- left_join(df_vote_cand, df_candidate, by = "id_cand")
tse <- left_join(tse, df_vote_detail, by = "id_area")



# Cleans datasets for replicates variables, rename them for better comprehension
tse <- tse |>
  mutate(
    female = case_when(
      DS_GENERO == "FEMININO" ~ 1,
      TRUE ~ 0),
    reelection = case_when( ##### Should we take into account if the person was an incumbent in the past? (complicated to model!)
      ST_REELEICAO == "S" ~ 1,
      ST_REELEICAO == "N" ~ 0,
      TRUE ~ NA),
    across(
      .cols = c(QT_APTOS, QT_COMPARECIMENTO, QT_ABSTENCOES, QT_VOTOS_NOMINAIS,
                QT_VOTOS_BRANCOS, QT_VOTOS_NULOS, votes_won),
      .fns = as.numeric)) |>
  select(
    id_cand, id_area, id_total, ANO_ELEICAO.x, NR_CANDIDATO.x, CD_ELEICAO.x,
    NR_TURNO.x, SG_UE.x, CD_MUNICIPIO.x, NR_ZONA.x,
    DS_DETALHE_SITUACAO_CAND.x, DS_DETALHE_SITUACAO_CAND.y,
    DS_SIT_TOT_TURNO.x, DS_SIT_TOT_TURNO.y, votes_won,
    QT_APTOS, QT_COMPARECIMENTO, QT_ABSTENCOES, QT_VOTOS_NOMINAIS,
    QT_VOTOS_BRANCOS, QT_VOTOS_NULOS, female, DS_GRAU_INSTRUCAO,
    DS_ESTADO_CIVIL, DS_OCUPACAO, reelection, last_run
  ) |>
  rename(
    year = ANO_ELEICAO.x,
    round = NR_TURNO.x,
    id_tse_elet = CD_ELEICAO.x,
    id_tse_loca = SG_UE.x,
    id_tse_muni = CD_MUNICIPIO.x,
    id_tse_zone = NR_ZONA.x,
    ballot_num = NR_CANDIDATO.x,
    cand_status1 = DS_DETALHE_SITUACAO_CAND.x,
    cand_status2 = DS_DETALHE_SITUACAO_CAND.y,
    cand_status3 = DS_SIT_TOT_TURNO.x,
    cand_status4 = DS_SIT_TOT_TURNO.y,
    t_electorate_z = QT_APTOS,
    t_turnoff_z = QT_COMPARECIMENTO,
    t_abstention_z = QT_ABSTENCOES,
    t_nomi_votes_z = QT_VOTOS_NOMINAIS,
    t_whit_votes_z = QT_VOTOS_BRANCOS,
    t_null_votes_z = QT_VOTOS_NULOS,
    cand_prof = DS_OCUPACAO,
    cand_educ = DS_GRAU_INSTRUCAO,
    cand_mari = DS_ESTADO_CIVIL,
    votes_won_z = votes_won
  ) |>
  
  # Make total votes per candidate
  group_by(year, id_tse_loca, ballot_num, id_tse_elet) |>
  mutate(
    votes_won_m = sum(votes_won_z)
  ) |>
  ungroup() |>
  
  
  # Group by municipality, year, for the mixed-gender dummy
  group_by(year, id_tse_loca) |>
  
  # Make a dummy == 1 if the local elections had mixed genders, and 0 otherwise
  mutate(mixed_gender = if_else(n_distinct(female) > 1, 1, 0)) |>
  ungroup() |>
  
  
  # Make total votes per municipality
  group_by(year, id_tse_loca, id_tse_elet) |>
  mutate(
    t_electorate_m = sum(t_electorate_z),
    t_turnoff_m = sum(t_turnoff_z),
    t_abstention_m = sum(t_abstention_z),
    t_nomi_votes_m = sum(t_nomi_votes_z),
    t_whit_votes_m = sum(t_whit_votes_z),
    t_null_votes_m = sum(t_null_votes_z)
  ) |>
  ungroup() |>
  
  
  # Relevant variables
  mutate(
    valid_votes_m = t_turnoff_m - t_whit_votes_m - t_null_votes_m,
    p_turnoff_m = t_turnoff_m/t_electorate_m,
    p_nomi_votes_m = t_nomi_votes_m/t_turnoff_m,
    p_cand_votes_m = votes_won_m/valid_votes_m,
    
    # Smell test: won at first round
    test1 = case_when(
      p_cand_votes_m >= 0.5 & round == 1 & cand_status4 == "ELEITO" ~ TRUE,
      p_cand_votes_m < 0.5 & round == 1 & cand_status4 == "ELEITO" ~ FALSE,
    )
  )



# More smell tests
#tse |> group_by(test1) |>
#  summarise(n())

#test1 <- filter(tse, p_cand_votes_m < 0.5 & round == 1 & cand_status3 == "ELEITO")
#test1 <- filter(tse, p_cand_votes_m >= 0.5 & round == 1 & cand_status3 == "ELEITO")
#test1 <- filter(tse, cand_status3 != cand_status4)

tse <- tse |>
  mutate(
    elected_female = case_when(
      female == 1 & cand_status4 == "ELEITO" ~ 1,
      TRUE ~ 0
    )) |>
  group_by(year) |>
    mutate(
      lag_elected_female = lag(elected_female, 1) 
    ) |>
  ungroup() |>
  mutate(
    year = as.factor(year),
    ballot_num = as.factor(ballot_num)
  )

tse3 <- as.data.frame(tse) %>% 
  filter(!is.na(elected_female), last_run == 1) |>
  #group_by(year, ballot_num, id_tse_loca) |>
  distinct(across(c(year, ballot_num, id_tse_loca)), .keep_all = T) |>
  #unique(year, ballot_num, id_tse_loca) |>
  arrange(year, id_tse_loca)





#typeof(tse)
#tse_reg <- filter(as.data.frame(tse), !is.na(tse))

#reg 1
reg <- lm(female ~ lag_elected_female, data = tse3, subset = mixed_gender == 1)

#reg 2 with controls
reg2 <- lm(female ~ lag_elected_female + year, data = tse3, subset = mixed_gender == 1)

#reg 3 with controls
reg3 <- lm(female ~ lag_elected_female, data = tse3)

#reg 4 with controls
reg3 <- lm(female ~ lag_elected_female + year, data = tse3)

#reg 5 with controls
reg3 <- lm(female ~ lag_elected_female + year, data = tse3)

#reg 6 with controls
reg3 <- lm(female ~ region, data = tse3)

reg3 <- lm(mixed_gender ~ lag_elected_female + year + region + ballot_num, data = tse3)

#Final results
reg1 <- glm(mixed_gender ~ lag_elected_female, data = tse3, family = binomial)
reg2 <- glm(mixed_gender ~ lag_elected_female + year + region, data = tse3, family = binomial) #fixed effects
reg3 <- glm(mixed_gender ~ lag_elected_female + year + region + ballot_num, data = tse3, family = binomial) #Control variables
reg4 <- lm(mixed_gender ~ lag_elected_female + year + region + ballot_num, data = tse3)


# Output the regression table to Word
stargazer(
  reg1, reg2, reg3, reg4, type = "html", title = "Regression models",
  omit = c("year", "region", "ballot_num"), out = "tablas.html",
  add.lines = list(
    c("Year FE", "No", "Yes", "Yes", "Yes"),
    c("Region FE", "No", "Yes", "Yes", "Yes"),
    c("Party FE", "No", "No", "Yes", "Yes")),
  omit = c("aic", "logLik", "rss", "rsq"))

##### Alexa Descriptive statistics 
summary_stats <- tse3 %>% 
  select(mixed_gender,elected_female)
  



##### 7. IBGE DATA -------------------------------------------------------------

# Load the Brazilian municipalities shapefile
df_mun <- st_read(paste0(ibgewd,"/BR_Municipios_2022.shp")) |>
  rename(codigo_ibge = CD_MUN) |>
  mutate(codigo_ibge = as.numeric(codigo_ibge))


# Load the table that converts TSE municipality codes into IBGE codes
df_ibge <- read.csv(paste0(ibgewd, "/municipios_brasileiros_tse.csv")) |>
  mutate(
    codigo_ibge_str = as.character(codigo_ibge),
    region = as.factor(substr(codigo_ibge, 1, 1))
  )




# Merge the IBGE codes into the TSE dataset
tse3 <- tse3 |>
  mutate(
    id_tse_muni = as.numeric(id_tse_muni),
    codigo_tse = id_tse_muni) |>
  arrange(codigo_tse)
tse3 <- left_join(tse3, df_ibge, by = "codigo_tse")


# Merge the shapefile into TSE dataset
tse3 <- left_join(tse3, df_mun, by = "codigo_ibge")





