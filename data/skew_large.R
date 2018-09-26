library(ggplot2)
library(reshape2)
library(gridExtra)

data <- read.csv("results_skew_large.csv");
data$t_repartition <- data$t_repartition / 1000000000 # ns to s
data$t_broadcast <- data$t_broadcast / 1000000000
data$t_merge <- data$t_merge / 1000000000
data$t_sort_merge <- data$t_sort_merge / 1000000000

d <- melt(data[,c("skew", "t_repartition","t_broadcast","t_merge")], id=c("skew"));

### Repartition Join ###

ggplot(data=data, aes(x=skew, y=t_repartition)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE, color="#DD3356", size=.5) +
  labs(x = "Skew", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("skew_repartition_join_large.pdf")

### Repartition Join Reducer Skew ###

reducer_times_01 = as.vector(as.numeric(unlist(strsplit(as.character(data$rt_1[1]), ";")))) / 1000;
reducer_times_03 = as.vector(as.numeric(unlist(strsplit(as.character(data$rt_1[7]), ";")))) / 1000;
reducer_times_06 = as.vector(as.numeric(unlist(strsplit(as.character(data$rt_1[9]), ";")))) / 1000;
reducer_times_10 = as.vector(as.numeric(unlist(strsplit(as.character(data$rt_1[10]), ";")))) / 1000;

p1 <- ggplot() + aes(reducer_times_01) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Reducers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Reducer Runtimes (skew = 0.1)")

p2 <- ggplot() + aes(reducer_times_03) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Reducers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Reducer Runtimes (skew = 0.7)")

p3 <- ggplot() + aes(reducer_times_06) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Reducers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Reducer Runtimes (skew = 0.9)")

p4 <- ggplot() + aes(reducer_times_10) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Reducers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Reducer Runtimes (skew = 1.0)")

grid.arrange(p1, p2, p3, p4, nrow=2, ncol=2);

### Broadcast Join ###

ggplot(data=data, aes(x=skew, y=t_broadcast)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE, color="#DD3356", size=.5) +
  labs(x = "Skew", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("skew_broadcast_join_large.pdf")

### Broadcast Join Mapper Skew ###

mapper_times_01 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_2[1]), ";")))) / 1000;
mapper_times_03 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_2[3]), ";")))) / 1000;
mapper_times_06 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_2[6]), ";")))) / 1000;
mapper_times_10 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_2[10]), ";")))) / 1000;

p1 <- ggplot() + aes(mapper_times_01) + geom_histogram(binwidth=4, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 0.1)")

p2 <- ggplot() + aes(mapper_times_03) + geom_histogram(binwidth=4, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 0.3)")

p3 <- ggplot() + aes(mapper_times_06) + geom_histogram(binwidth=4, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 0.6)")

p4 <- ggplot() + aes(mapper_times_10) + geom_histogram(binwidth=4, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 1.0)")

grid.arrange(p1, p2, p3, p4, nrow=2, ncol=2);

### Merge Join ##

ggplot(data=data, aes(x=skew, y=t_merge)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE,   color="#DD3356", size=.5) +
  labs(x = "Skew", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("skew_merge_join_large.pdf");

### Merge Join Skew ###

mapper_times_01 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_3[1]), ";")))) / 1000;
mapper_times_03 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_3[3]), ";")))) / 1000;
mapper_times_06 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_3[6]), ";")))) / 1000;
mapper_times_10 = as.vector(as.numeric(unlist(strsplit(as.character(data$mt_3[10]), ";")))) / 1000;

p1 <- ggplot() + aes(mapper_times_01) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 0.1)")

p2 <- ggplot() + aes(mapper_times_03) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 0.3)")

p3 <- ggplot() + aes(mapper_times_06) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 0.6)")

p4 <- ggplot() + aes(mapper_times_10) + geom_histogram(binwidth=.5, color="black", fill="#45CC56") +
  labs(x = "Time (Seconds)", y = "# Mappers") +
  scale_x_continuous(labels=scales::comma) +
  ggtitle("Mapper Runtimes (skew = 1.0)")

grid.arrange(p1, p2, p3, p4, nrow=2, ncol=2);

### Comparison of all Joins ###

ggplot(d) + geom_line(aes(x=skew, y=value, color=variable)) +
  labs(x = "Skew", y = "Time(seconds") +
  scale_x_continuous(breaks = seq(0, 1.2, by = 0.1))

ggsave("skew_comparison_large.pdf")