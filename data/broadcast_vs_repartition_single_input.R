library(ggplot2)
library(reshape2)

data <- read.csv("broadcast_vs_repartition_single_input.csv");
data$t_repartition <- data$t_repartition / 1000000000 # ns to s
data$t_broadcast <- data$t_broadcast / 1000000000

d <- melt(data[,c("rows", "t_repartition","t_broadcast")], id=c("rows"));

ggplot(d) + geom_line(aes(x=rows, y=value, color=variable)) +
  labs(x = "Rows", y = "Time(seconds") +
  scale_x_continuous(labels=scales::comma)

ggsave("broadcast_vs_repartition_single_input.pdf")