# Octopus box statistics

stats calculated from pcap file usually made by [tcpdump](http://www.tcpdump.org/tcpdump_man.html)

Dependence:
 * [R Project](https://www.r-project.org/) 
   * plyr package for R (simply run inside R: `install.packages('plyr')`)
 * patched [tshark](https://github.com/Vespertinus/wireshark) 


Usage: 
 1. Write traffic from box to pcap, for example:
    1. `tcpdump -s 65535 -i eth0 port 10000 -w data.pcap`
 1. Convert pcap to csv using tshark. **Note that wireshark init.lua might call octopus.lua** from extra/wireshark dir       
    1. `tshark -r data.pcap -Y 'iproto'  -T fields -E separator=, -E quote=d -e frame.number -e frame.time -e ip.src -e ip.dst -e iproto.msg -e iproto.len -e silverbox.limit -e silverbox.count > data.csv`
 1. Run stat calculation:
    1. `Rscript extra/rstat/box_pcap_stat.r data.csv`


