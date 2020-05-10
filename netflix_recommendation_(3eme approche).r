# Big Data Project

#Students: 

#Taous Iggui 21909286 (VMI)
#Amira Kouider 21914040 (MLDS)
#Nadia Radouani 21911973 (MLDS)
#Ghazeleh Ghanizadeh 21914004 (VMI)


library("readr")
library("dplyr")
library("lubridate")
library("randomForest")

# Le chemain de la DataBase 
setwd("C:/Taous/TP/Projet BigData/RandomForestR/")

train_data_file <- "combined_data_1.txt"
train_set <- read_csv(train_data_file,skip = 1,col_names = FALSE)

colnames(train_set)<-c("userID","MovieScore","Date")
movie_info_file <- "movie_titles.csv"

movie_info_data <- read_csv(movie_info_file,col_names = FALSE)

colnames(movie_info_data) <- c("movieID","date_sortie","Titre")


train_set <- train_set%>%filter(!is.na(MovieScore))

mymovie_IDs <- c(grep(":",train_set$userID),nrow(train_set)) 


unique_movieID <- 1:length(mymovie_IDs)

mymovie_IDs <- diff(c(0,mymovie_IDs) )
mymovie_IDs <- purrr::map(.x = unique_movieID,~rep(.x, times = mymovie_IDs[.x]))

mymovie_IDs <- unlist(mymovie_IDs)


train_set <- train_set%>%mutate(movieID = mymovie_IDs)

# enrichir les donnees pour inclure la date de sortie du film 



train_set <- train_set%>%left_join(movie_info_data,by =c("movieID"))


train_set <- train_set%>%select(-Titre)
# calculer l'age du film au jour de l'evaluation par utilisateur


train_set <- train_set%>%mutate(movie_age = year(Date) - date_sortie)

# ajouter le facteur temps (mois(ete ou hiver) , jour de la semaine(week end ou pas ))

train_set <- train_set%>%mutate(mois = month(Date),jour_semaine = wday(Date))
# Apprentissage du model : 
# La probelmatique de scoring est de type  classification vu que les score sont des entier et ont une valeur entre 
# 1 et 5 

# on utilise l'algorithme de "RandomForest" pour 2 raisons principales: 
# 1 - y a des variables numeriques et categoriques
# 2 - rapide et donne des resultats stables

train_set <- head(train_set,200000)

train_set<-train_set%>%mutate(MovieScore = as.factor(MovieScore))



nombre_arbre <- 12 
nombre_cpu   <- 4  

paralleliser <- FALSE # FALSE/TRUE

stop("Dagi")
if(paralleliser==TRUE){
  print("parallelisation")

  library("foreach")
  library("doParallel")
  
  registerDoParallel(nombre_cpu)
  
  print("on distribue le nombre d'arbre selon le nombre de CPU")
  
  arbre_map <- rep(floor(nombre_arbre/nombre_cpu),nombre_cpu)
  
  
  start_time <- Sys.time()
  
  recommendation_model <- foreach(ntree = arbre_map, .combine = combine, .multicombine = T, .packages = 'randomForest') %dopar% {    
    randomForest(formula = MovieScore ~ .,data =train_set  ,ntree  = ntree)
  }
  
  
  par_end_time <- Sys.time()-  start_time
  
  print("Temps d'execution en parallel:")
  par_end_time
}else{
  print("en Serie")
  
  start_time <- Sys.time()
  recommendation_model <- randomForest(formula = MovieScore ~ .,data =train_set  ,ntree = nombre_arbre )
  ser_end_time <- Sys.time()-  start_time
  
  print("Temps d'execution en serie:")
  ser_end_time
}


importance(recommendation_model)





# Tester et utiliser 

test_data_file <- "qualifying.txt"
test_set <- read_csv(test_data_file,skip = 1,col_names = FALSE)

colnames(test_set)<-c("userID","Date")



mymovie_IDs <- c(grep(":",test_set$userID),nrow(test_set)) 


unique_movieID <- 1:length(mymovie_IDs)

mymovie_IDs <- diff(c(0,mymovie_IDs) )
mymovie_IDs <- purrr::map(.x = unique_movieID,~rep(.x, times = mymovie_IDs[.x]))

mymovie_IDs <- unlist(mymovie_IDs)


test_set <- test_set%>%mutate(movieID = mymovie_IDs)


test_set <- test_set%>%left_join(movie_info_data,by =c("movieID"))

# calculer l'age du film au jour de l'evaluation par utilisateur


test_set <- test_set%>%mutate(movie_age = year(Date) - date_sortie)

# ajouter le facteur temps (mois(ete ou hiver) , jour de la semaine(week end ou pas ))

test_set <- test_set%>%mutate(mois = month(Date),jour_semaine = wday(Date))


# utiliser les nouvelles donnees pour predire le score de chaque film par
# utilisateur pour chaque
predicted_score <- predict(recommendation_model,newdata = test_set)


test_set <- test_set%>%mutate(predicted_score = predicted_score)

test_set <- test_set%>%select(userID,Date,predicted_score,Titre)


myuser <- "2112862"
mydate <- "2004-12-02"
# selectioner les donner de mon utilisateur
recommendataion <-  test_set%>%filter(userID == myuser)

# trier en ordre decroissant selon le score 

recommendataion <- recommendataion%>%arrange(desc(predicted_score))
recommendataion <- head(recommendataion%>%select(userID,Titre),2)

recommendataion
View(test_set)
