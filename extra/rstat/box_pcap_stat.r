# prepare data:
# tshark -r ./box.pcap -Y 'iproto'  -T fields -E separator=, -E quote=d -e frame.number -e frame.time -e ip.src -e ip.dst -e iproto.msg -e iproto.len -e silverbox.limit -e silverbox.count -e silverbox.obj_space  -e silverbox.key_count -e silverbox.index > box.csv
#
# run:
# Rscript box_pcap_stat.r box.csv

library(plyr)

options("width"=250)

cur_file = commandArgs(TRUE)[1]

ds = read.csv(cur_file, header=FALSE, sep = ",", quote = "\"")
colnames(ds) <- c('frame_num', 'time', 'ip_src', 'ip_dst', 'iproto_msg', 'iproto_len', 'sb_limit', 'sb_count', 'obj_space', 'sb_key_count', 'sb_index')

ds$obj_space <- as.factor(ds$obj_space)
ds$sb_index <- as.factor(ds$sb_index)


top_n <- function(ds, column, count = 0) {
        if (count) {
                print(head(ds[order(-ds[[column]] ), ], count))

        }
        else {
                print(ds[order(-ds[[ column]] ), ])
        }
}

ds_stat <- function(ds, column, message, count = 0) {
        cat("\n", message, ":\n")
        print(summary(ds[[column]]))
        cat("top", count, "items\n")
        top_n(ds, column, count)
}


summary(ds)

cat("iproto req + reply count:", length(ds$iproto_msg), "\n" )

tmp <- subset(ds, iproto_len > 500)

cat("iproto messages > 500 bytes length:", length(tmp$iproto_msg), "\n")
rm(tmp)

req_ds   = subset(ds, is.na(sb_count))
reply_ds = subset(ds, is.na(sb_limit))

req_ds$sb_count <- NULL
reply_ds$sb_limit <- NULL
reply_ds$obj_space <- NULL
reply_ds$sb_key_count <- NULL
reply_ds$sb_index <- NULL

cat("iproto req count:", length(req_ds$iproto_msg), "\n" )
cat("iproto reply count:", length(reply_ds$iproto_msg), "\n" )


req_aggr <- aggregate(data = req_ds, iproto_len ~ ip_src, sum)
req_cnt_ds <- count(req_ds, vars = 'ip_src')
colnames(req_cnt_ds) = c('ip_src', 'msg_count')
req_aggr <- merge(req_aggr, req_cnt_ds, by = 'ip_src', all = TRUE)
cat("\nactive peer cnt that send at least one req:", length(req_aggr$iproto_len), "\n")
cat("total iproto requests size per peer:\n")
summary(req_aggr$iproto_len)
cat("total iproto requests count per peer:\n")
summary(req_aggr$msg_count)
cat("30 peers that send more bytes than other\n")
top_n(req_aggr, 'iproto_len', 30)


reply_aggr <- aggregate(data = reply_ds, iproto_len ~ ip_dst, sum)
reply_cnt_ds <- count(reply_ds, vars = 'ip_dst')
colnames(reply_cnt_ds) = c('ip_dst', 'msg_count')
reply_aggr <- merge(reply_aggr, reply_cnt_ds, by = 'ip_dst', all = TRUE)
cat("\nactive peer cnt that receive at least one reply:", length(reply_aggr$iproto_len), "\n")
cat("total iproto reply size per peer:\n")
summary(reply_aggr$iproto_len)
cat("total iproto reply count per peer:\n")
summary(reply_aggr$msg_count)
cat("30 peers that receive more bytes than other\n")
top_n(reply_aggr, 'iproto_len', 30)


ds_stat(aggregate(data = req_ds, sb_limit ~ ip_src, sum),
                'sb_limit',
                'box total limit distribution per peer',
                30)

ds_stat(aggregate(data = reply_ds, sb_count ~ ip_dst, sum),
                'sb_count',
                'box total reply count distribution per peer',
                30)

ds_stat(req_ds, 'sb_limit', 'box limit stat', 30)
ds_stat(reply_ds, 'sb_count', 'box reply count stat', 30)

ds_stat(req_ds, 'iproto_len', 'box max iproto request', 50)
ds_stat(reply_ds, 'iproto_len', 'box max iproto reply', 50)

ds_stat(req_ds, 'sb_key_count', 'box requests with max keys count', 50)
