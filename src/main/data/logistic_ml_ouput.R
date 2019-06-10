###########################################################################################
################################## ML-Sorteerstraatjes ####################################
###########################################################################################


#______________________________Load packages/options______________________________________#
{
  
  setwd("C:/Users/sa79998/Documents/Work-Antwerpen")
  #set options 
  options(java.parameters = "- Xmx15360m") 
  options(stringsAsFactors = FALSE)
  options(scipen=999)
  
  #load packages
  library("pROC")
  library("gbm")
  library("dplyr")
  library("car")
}
#_______________________________________Functions_________________________________________#
{

  dbcleaner <- function(df)
  {
    
    for(i in 1:length(names(df)))
    {
      
      names(df) <- gsub('[^a-z0-9]+','_', tolower(names(df)))
      
      names(df) <- tolower(names(df))
      
      names(df) <- make.names(names(df), unique=TRUE, allow_=TRUE)
      
      names(df)  <- gsub("(\\.)", "_", names(df))
      
      df[[i]] <- ifelse(grepl("^[ \t\n]*$", df[[i]]), NA,
                        ifelse(df[[i]] == "-", NA,
                               ifelse(df[[i]] == ".", NA,
                                      ifelse(df[[i]] == "#LEEG!",NA,
                                             df[[i]]))))
      
      df[[i]] <- gsub("\\s*\\([^\\)]+\\)", "", df[[i]])
      df[[i]] <- gsub(",", ".", df[[i]])
      
    }
    
    return(df)
  }
  
}
#________________________________________Load data________________________________________#
{
  df <- read.csv2("ml_input_final_2.csv", sep=',')
  df <- df[, -c(1)]
  
  df <- na.omit(df) #van 587762 naar 543685 observaties 
  
  df <- df[-which(df$container_afvaltype=='+'),]
  
  df$container_afvaltype <-Recode(df$container_afvaltype, "'REST'='Rest';'GLAS'='Glas'; 'PK'='Papier'")
  
  df <- dbcleaner(df)
  
  df$container_afvaltype <- as.factor(df$container_afvaltype)
  df$date_part <- as.factor(df$date_part)
  df$verplicht <- as.factor(df$verplicht)
  df$ledfreq <- as.factor(df$ledfreq)
  df$led_dag <- as.factor(df$led_dag)
  df$aantal <- as.numeric(df$aantal)
  df$lediging_24h_later <- as.numeric(df$lediging_24h_later)
  df$volume_sinds_lediging <- as.numeric(df$volume_sinds_lediging)
  df$volume_dag <- as.numeric(df$volume_dag)
  df$container_nr <- as.factor(df$container_nr)
  df$niscode <- as.factor(df$niscode)
  
  df <- df[, -c(2,7,8:11)]
  
  
  sample<- sample(seq(nrow(df)), length(df$container_nr)*0.9)
  
  train <- df[sample, ]
  test <- df[-sample, ]
  
}
#________________________________________Analyses_________________________________________#
{
  
#logistic regression
{

  summary(m1 <- glm(lediging_24h_later ~ container_afvaltype+volume_sinds_lediging+ volume_dag + date_part + verplicht
                    + ledfreq +  aantal  , family = binomial(link = "logit"),  data = train))
  
  #check fit/ROC
  pred <- as.data.frame(as.matrix(predict(m1,test[,-c(3)])))
  
  names(pred)[1] <- "logodds"
  
  pred$exp <- exp(pred$logodds)
  
  pred$prob = pred$exp / (1 + pred$exp)

  pred$test <- test[,3]
  
  ROC <- roc(pred$test,pred$prob)
  plot.roc(ROC, auc.polygon=T, col="blue", identity.col="red", identity.lty=2, print.auc=T, print.thres=T)
  
  
}

##boosted trees (GMB)
{

  gbm <- gbm(
    formula = lediging_24h_later ~ .,
    distribution = "bernoulli",
    data = train,
    n.trees = 100,
    interaction.depth = 5,
    shrinkage = 0.3,
    n.minobsinnode = 10,
    bag.fraction = .65, 
    train.fraction = 1,
    n.cores = NULL, 
    verbose = FALSE
  )  
  
  summary(gbm)
  
  pred <- as.data.frame(as.matrix(predict(gbm,test[,-c(2)], n.trees=100, type="response")))
  
  names(pred)[1] <- "prob"
  
  pred$test <- test[,2]
  
  ROC <- roc(pred$test,pred$prob)
  plot.roc(ROC, auc.polygon=T, col="blue", identity.col="red", identity.lty=2, print.auc=T, print.thres=T)

#space-search for best hyper-parameters
hyper_grid <- expand.grid(
  shrinkage = c(.01, .1, .3),
  interaction.depth = c(1, 3, 5),
  n.minobsinnode = c(5, 10, 15),
  bag.fraction = c(.65, .8, 1), 
  optimal_trees = 0,               
  min_RMSE = 0                     
)
nrow(hyper_grid)

#re-iterate model across the hypergrid
pb <- txtProgressBar(min = 0, max = nrow(hyper_grid), style = 3)

for(i in 1:nrow(hyper_grid)) {
  
  # train model
  gbm.tune <- gbm(
    formula = lediging_24h_later ~ .,
    distribution = "bernoulli",
    data = train,
    n.trees = 100,
    interaction.depth = hyper_grid$interaction.depth[i],
    shrinkage = hyper_grid$shrinkage[i],
    n.minobsinnode = hyper_grid$n.minobsinnode[i],
    bag.fraction = hyper_grid$bag.fraction[i],
    train.fraction = .75,
    n.cores = NULL, # will use all cores by default
    verbose = FALSE
  )
  
  # add min training error and trees to grid
  hyper_grid$optimal_trees[i] <- which.min(gbm.tune$valid.error)
  hyper_grid$min_RMSE[i] <- sqrt(min(gbm.tune$valid.error))
  
  setTxtProgressBar(pb, i)
}

close(pb)

hyper_grid %>% 
  dplyr::arrange(min_RMSE) %>%
  head(10)

gbm.fit.final <- gbm(
  formula = lediging_24h_later ~ .,
  distribution = "bernoulli",
  data = train,
  n.trees = 100,
  interaction.depth = 5,
  shrinkage = 0.3,
  n.minobsinnode = 10,
  bag.fraction = 1, 
  train.fraction = 1,
  n.cores = NULL, 
  verbose = FALSE
)  

summary(gbm.fit.final)

pred <- as.data.frame(as.matrix(predict(gbm.fit.final,test[,-c(2)], n.trees=100, type="response")))

names(pred)[1] <- "prob"

summary(pred$prob)

pred$test <- test[,2]

ROC <- roc(pred$test,pred$prob)
plot.roc(ROC, auc.polygon=T, col="blue", identity.col="red", identity.lty=2, print.auc=T, print.thres=T)


}
  
}
#________________________________________________________________________________________#

# download the e1071 library
install.packages("e1071")

# download the SparseM library
install.packages("SparseM")

# load the libraries
library(e1071)
library(SparseM)

# convert labels into numeric format

df1 <- df

df1$container_nr <- as.numeric(df1$container_nr)
df1$lediging_24h_later <- as.numeric(df1$lediging_24h_later)
df1$volume_sinds_lediging <- as.numeric(df1$volume_sinds_lediging)
df1$volume_dag <- as.numeric(df1$volume_dag)
df1$date_part<- as.numeric(df1$date_part)

# convert from data.frame to matrix format
x <- as.matrix(df1[,c(1,4:6)])

# put the labels in a separate vector
y <- df[,3]

# convert to compressed sparse row format
xs <- as.matrix.csr(x)

# write the output libsvm format file 
write.matrix.csr(xs, y=y, file="out4.txt")


length(table(df1$container_nr))

##Trash
{
  results <- data.frame(percentage =c() , cut=c())
  
  for(i in seq(0,1, 0.01))
  {
    pred$cat <- ifelse(pred$prob > i, 1, 0)
    
    pred$test <- test[,2]
    
    x <- as.data.frame(as.matrix(table(pred$cat+pred$test)))
    
    b <- (x[1,1] + x[3,1])/40546
    
    d <- x[3,1] / 6166
    
    c <- cbind(i,b,d)
    
    results <- rbind(results,c)
    
    
  }
  
  results <- na.omit(results)
  
  results$sum <- results$b + results$d
  
  results2 <- results[order(results$sum),]
  
  
  #An area under the ROC curve of 0.8, for example, means that a randomly selected case from the group with the target equals 1 has a score larger than that for a randomly chosen case from the group with the target equals 0 in 80% of the time.
  
  
  results <- data.frame(percentage =c() , cut=c())
  
  for(i in seq(0,1, 0.01))
  {
    pred$cat <- ifelse(pred$prob > i, 1, 0)
    
    pred$test <- test[,2]
    
    x <- as.data.frame(as.matrix(table(pred$cat+pred$test)))
    
    b <- (x[1,1] + x[3,1])/40546
    
    d <- x[3,1] / 6166
    
    c <- cbind(i,b,d)
    
    results <- rbind(results,c)
    
    
  }
  
  results <- na.omit(results)
  
  results$sum <- results$b + results$d
  
  results2 <- results[order(results$sum),]
  results2
  
  
  c <- data.frame(c=c())
  for(i in seq(0,1, 0.01))
  {
    
    pred$cat <- ifelse(pred$prob > i, 1, 0)
    
    ROC <- roc(pred$test,pred$cat)
    
    a <- ROC$auc
    
    b <- cbind(a,i)
    
    c <- rbind(c,b)
    
  }
  
  c2 <- c[order(c$a),]
  c2
  pred$cat <- ifelse(pred$prob > 0.16, 1, 0)
  
  ROC <- roc(pred$test,pred$cat)
  plot.roc(ROC, auc.polygon=T, col="blue", identity.col="red", identity.lty=2, print.auc=T, print.thres=T)
  
  ROC <- roc(pred$test,pred$cat)
  plot.roc(ROC, auc.polygon=T, col="blue", identity.col="red", identity.lty=2, print.auc=T, print.thres=T)
  
  
  
  par(mar = c(5, 8, 1, 1))
  summary(
    gbm.fit.final, 
    cBars = 10,
    method = relative.influence, # also can use permutation.test.gbm
    las = 2
  )
  
  
  
}
