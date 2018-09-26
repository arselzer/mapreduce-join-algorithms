library(ggplot2)
library(reshape2)

data <- read.csv("results_skew.csv");
data$t_repartition <- data$t_repartition / 1000000000 # ns to s
data$t_broadcast <- data$t_broadcast / 1000000000
data$t_merge <- data$t_merge / 1000000000
data$t_sort_merge <- data$t_sort_merge / 1000000000

d <- melt(data[,c("skew", "t_repartition","t_broadcast","t_merge")], id=c("skew"));

ggplot(data=data, aes(x=skew, y=t_repartition)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE, color="#DD3356", size=.5) +
  labs(x = "Skew", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("skew_repartition_join.pdf")

ggplot(data=data, aes(x=skew, y=t_broadcast)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE, color="#DD3356", size=.5) +
  labs(x = "Skew", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("skew_broadcast_join.pdf")

ggplot(data=data, aes(x=skew, y=t_merge)) + geom_line() + geom_point() +
  geom_smooth(method='lm', formula=y~x, se=FALSE,   color="#DD3356", size=.5) +
  labs(x = "Skew", y = "Time (seconds)") +
  scale_x_continuous(labels=scales::comma)

ggsave("skew_merge_join.pdf")

ggplot(d) + geom_line(aes(x=skew, y=value, color=variable)) +
  labs(x = "Skew", y = "Time(seconds") +
  scale_x_continuous(breaks = seq(0, 1.2, by = 0.1))

ggsave("skew_comparison.pdf")