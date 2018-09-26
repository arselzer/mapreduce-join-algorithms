library(ggplot2)
library(reshape2)

data <- read.csv("results_speedup_0.7.csv");
data$t_repartition <- data$t_repartition / 1000000000 # ns to s
data$t_broadcast <- data$t_broadcast / 1000000000
data$t_merge <- data$t_merge / 1000000000
data$t_sort_merge <- data$t_sort_merge / 1000000000

d <- melt(data[,c("rows", "t_repartition","t_broadcast","t_merge")], id=c("rows"));

ggplot(data=data, aes(x=rows, y=t_repartition)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE, color="#DD3356", size=.5) +
  labs(x = "Rows", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("repartition_join_0.7.pdf")

ggplot(data=data, aes(x=rows, y=t_broadcast)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE, color="#DD3356", size=.5) +
  labs(x = "Rows", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("broadcast_join_0.7.pdf")

ggplot(data=data, aes(x=rows, y=t_merge)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE, color="#DD3356", size=.5) +
  labs(x = "Rows", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("merge_join_0.7.pdf")

ggplot(d) + geom_line(aes(x=rows, y=value, color=variable)) +
  labs(x = "Rows", y = "Time(seconds") +
  scale_x_continuous(labels=scales::comma)

ggsave("comparison_0.7.pdf")