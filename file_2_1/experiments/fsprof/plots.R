library(ggplot2)
library(dplyr)

data <- read.csv("results.csv")

data$concurrency <- as.factor(data$concurrency)

x <- data[data$error == "false",]


ggplot(x, aes(x=concurrency, y=elapsed)) + geom_boxplot() + facet_wrap(~op,  scales="free") +
  ggtitle("Storage Latencies by Concurrency") + xlab("Concurrency") + ylab("Latency (seconds)") +
  theme(plot.title = element_text(hjust=0.5))


bw <- x %>%
  group_by(op, concurrency) %>%
  summarise(total_time = sum(elapsed), bytes_transferred=sum(bytes_transferred))

bw$mbps <- bw$bytes_transferred/(1024*1024) / (bw$total_time / as.numeric(bw$concurrency))

ggplot(bw, aes(x=concurrency, y=mbps, group=op, colour=op)) + geom_line() +
  ggtitle("Aggregate throughput by concurrency") + xlab("Concurrency") +
  ylab("Aggregate throughput")
