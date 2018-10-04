require(dplyr)
require(stringr)

mean <- function(timepoint) {
  sum_of_reviews <- 0
  sum_of_words <- 0
  for (i in 1:timepoint) {
    #sum the total number of reviews and the number of words at each timepoint
    sum_of_reviews <- sum_of_reviews + (reviews$positives[i] + reviews$negatives[i])
    sum_of_words <- sum_of_words + reviews$number_of_words[i]
  }
  #total words divided by the total number of reviews is the mean number of words in each review
  sum_of_words/sum_of_reviews
}

standard.deviation <- function(timepoint) {
  mean <- mean(timepoint)
  number_of_reviews_list <- number.of.reviews(timepoint)
  number_of_reviews <- number_of_reviews_list[[1]] + number_of_reviews_list[[2]]
  sum_of_error <- 0
  
  for (i in 1:number_of_reviews) {
    sum_of_error <- sum_of_error + (mean - data[i, "word_count"])^2
  }
  sqrt(sum_of_error/number_of_reviews)
}

number.of.users <- function(timepoint) {
  number_of_reviews_list <- number.of.reviews(timepoint)
  number_of_reviews <- number_of_reviews_list[[1]] + number_of_reviews_list[[2]]
  
  user_ids <- data$user_id[1:number_of_reviews]
  length((unique(user_ids)))
}

number.of.reviews <- function(timepoint) {
  positives <- 0
  negatives <- 0
  for (i in 1:timepoint) {
    positives <- positives + reviews$positives[i]
    negatives <- negatives + reviews$negatives[i]
  }
  return(list(positives, negatives))
}


#READ AND SORT 10 ROWS
######################
#data <- read.csv(filename, nrows=10)
#data$word_count <- str_count(data$text, '\\S+')
#data <- sort.dataset(data)
######################



#READING AND SORTING
#############################################################################

filename <- "C:\\My Files\\OVGU\\Data Mining 2\\datasets\\yelp_review.csv"
chunkSize = 300000

index <- 0
con <- file(description=filename, open="r")
dataChunk <- read.table(con, nrows=chunkSize, header=TRUE, fill=TRUE, sep=",")

data <- dataChunk


actualColumnNames <- names(dataChunk)

repeat{
  index <- index + 1
  print(paste('Processing Rows:', index * chunkSize))
  
  if (nrow(dataChunk) != chunkSize){
    print('Processed all records')
    break
  }
  a <- Sys.time()
  dataChunk <- read.table(con, nrows=chunkSize, skip=0, header=FALSE, fill=TRUE, col.names = actualColumnNames, sep = ",")
  print(paste("Time taken to read records: ", Sys.time() - a))
  a <- Sys.time()
  data <- bind_rows(data, dataChunk)
  print(paste("Time taken to bind: ", Sys.time() - a))
}

close(con) 



#add number of words column
#data$word_count <- str_count(data$text, '\\S+')

#data <- sort.dataset(data)
data <- data[order(as.Date(data$date)),]

#############################################################################



#COMPUTE TIMEPOINTS
#############################################################################

#create required data structures
reviews = list(positives=0, negatives=0, number_of_words=0)

#get month value of first row
previous_month <- as.numeric(format(as.Date(data[1, "date"]), "%m"))

timepoint <- 1
sum_of_words <- 0

for (row_index in 1:nrow(data)) {
  
  #extract date and convert month to numeric
  current_month <- as.numeric(format(as.Date(data[row_index, "date"]), "%m"))
  
  if (current_month != previous_month){ #change in month
    
    #set the number of words field to the sum of words, this is the sum of words at timepoint t
    reviews$number_of_words[timepoint] <- sum_of_words
    
    timepoint = timepoint + 1 #increment timepoint
    
    #create next subscript for the next timepoint
    reviews$positives[timepoint] <- 0 
    reviews$negatives[timepoint] <- 0
    reviews$number_of_words[timepoint] <- 0
    
    previous_month <- current_month
    
    print(row_index)
  }
  
  #check if good or bad reivew and increment counter at that timepoint
  if (data[row_index, "stars"] > 2){
    reviews$positives[timepoint] <- reviews$positives[timepoint] + 1
  }
  else{
    reviews$negatives[timepoint] <- reviews$negatives[timepoint] + 1  
  }
  # keep a running total of number of words in each record
  sum_of_words = sum_of_words + str_count(data[row_index, "text"], '\\S+')
  #reviews$number_of_words[timepoint] <- reviews$number_of_words[timepoint] + data[row_index, "word_count"]
}

#store all user_ids and the word counts of the full dataset
all_user_ids <- data.frame("user_id" = data$user_id)

#############################################################################






