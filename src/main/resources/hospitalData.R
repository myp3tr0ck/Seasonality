install.packages("randomForest")
install.packages("pmml")
install.packages("XML")
install.packages("rpart")
install.packages("rattle")
install.packages("ggplot2")

setClass('myDate')
setAs("character","myDate", function(from) as.Date(from, format="%m/%d/%Y") )

mydata <- read.delim("c:\\users\\addoyle1\\Desktop\\HospitalData2.txt", colClasses=c('myDate', 'integer', 'integer', 'integer', 'integer', 'integer', 'integer', 'integer'), header=TRUE)

mydata$MONDAY <- mydata$Weekday == 2
mydata$TUESDAY <- mydata$Weekday == 3
mydata$WEDNESDAY <- mydata$Weekday == 4
mydata$THURSDAY <- mydata$Weekday == 5
mydata$FRIDAY <- mydata$Weekday == 6
mydata$SATURDAY <- mydata$Weekday == 7
mydata$SUNDAY <- mydata$Weekday == 1

mydata$JANUARY <- mydata$Month == 1
mydata$FEBRUARY <- mydata$Month == 2
mydata$MARCH <- mydata$Month == 3
mydata$APRIL <- mydata$Month == 4
mydata$MAY <- mydata$Month == 5
mydata$JUNE <- mydata$Month == 6
mydata$JULY <- mydata$Month == 7
mydata$AUGUST <- mydata$Month == 8
mydata$SEPTEMBER <- mydata$Month == 9
mydata$OCTOBER <- mydata$Month == 10
mydata$NOVEMBER <- mydata$Month == 11
mydata$DECEMBER <- mydata$Month == 12

index <- sample(1:nrow(mydata),size = 0.7*nrow(mydata)) 
train <- mydata [index,]  
test <- mydata [-index,] 

# Baseline model
best.guess <- mean(train$Total)
RMSE.baseline <- sqrt(mean((best.guess-test$Total)^2))
RMSE.baseline

MAE.baseline <- mean(abs(best.guess-test$Total))
MAE.baseline

# Decision trees
library(rpart)
library(rattle)

rt <- rpart(Total ~ MONDAY + TUESDAY + WEDNESDAY + THURSDAY + FRIDAY + SATURDAY + SUNDAY +
              JANUARY + FEBRUARY + MARCH + APRIL + MAY + JUNE + JULY + AUGUST + SEPTEMBER + OCTOBER + NOVEMBER + DECEMBER + 
              Holiday + PlusOne + MinusOne, data = train)
test.pred.rtree <- predict(rt,test) 
 
RMSE.rtree <- sqrt(mean((test.pred.rtree-test$Total)^2))
RMSE.rtree
MAE.rtree <- mean(abs(test.pred.rtree-test$Total))
MAE.rtree

printcp(rt)

min.xerror <- rt$cptable[which.min(rt$cptable[,"xerror"]),"CP"]
min.xerror


# ...and use it to prune the tree
rt.pruned <- prune(rt,cp = min.xerror) 

# Plot the pruned tree
fancyRpartPlot(rt.pruned)

test.pred.rtree.p <- predict(rt.pruned,test)
RMSE.rtree.pruned <- sqrt(mean((test.pred.rtree.p-test$Total)^2))
RMSE.rtree.pruned

MAE.rtree.pruned <- mean(abs(test.pred.rtree.p-test$Total))
MAE.rtree.pruned


# Random Forest
library(randomForest)
set.seed(123)
rf <- randomForest(Total ~ MONDAY + TUESDAY + WEDNESDAY + THURSDAY + FRIDAY + SATURDAY + SUNDAY +
              JANUARY + FEBRUARY + MARCH + APRIL + MAY + JUNE + JULY + AUGUST + SEPTEMBER + OCTOBER + NOVEMBER + DECEMBER + 
              Holiday + PlusOne + MinusOne, data = train, importance = TRUE, ntree=1000)

which.min(rf$mse)

# Plot rf to see the estimated error as a function of the number of trees
# (not running it here)
plot(rf) 
 
imp <- as.data.frame(sort(importance(rf)[,1],decreasing = TRUE),optional = T)
names(imp) <- "% Inc MSE"
imp

test.pred.forest <- predict(rf,test)
RMSE.forest <- sqrt(mean((test.pred.forest-test$Total)^2))
RMSE.forest
 
MAE.forest <- mean(abs(test.pred.forest-test$Total))
MAE.forest



# Determine best model

library(ggplot2)
library(tidyr)
accuracy <- data.frame(Method = c("Baseline","Full tree","Pruned tree","Random forest"),
                         RMSE   = c(RMSE.baseline,RMSE.rtree,RMSE.rtree.pruned,RMSE.forest),
                         MAE    = c(MAE.baseline,MAE.rtree,MAE.rtree.pruned,MAE.forest)) 
 
# Round the values and print the table
accuracy$RMSE <- round(accuracy$RMSE,2)
accuracy$MAE <- round(accuracy$MAE,2) 
accuracy

all.predictions <- data.frame(actual = test$Total,
                              baseline = best.guess,
                              full.tree = test.pred.rtree,
                              pruned.tree = test.pred.rtree.p,
                              random.forest = test.pred.forest)

all.predictions

all.predictions <- gather(all.predictions,key = model,value = predictions,2:5)

ggplot(data = all.predictions,aes(x = actual, y = predictions)) + 
     geom_point(colour = "blue") + 
     geom_abline(intercept = 0, slope = 1, colour = "red") +
     geom_vline(xintercept = 23, colour = "green", linetype = "dashed") +
     facet_wrap(~ model,ncol = 2) + 
     coord_cartesian(xlim = c(200,600),ylim = c(200,600)) +
     ggtitle("Predicted vs. Actual, by model")

# Save to PMML
library(pmml)

mypmml <- pmml(rf, name="patient-seasonality", data=train)
xmlFile <- file.path(getwd(), "patient-seasonality.xml")
saveXML(mypmml, xmlFile)