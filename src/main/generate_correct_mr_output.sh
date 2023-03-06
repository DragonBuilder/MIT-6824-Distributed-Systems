rm mr-out*
rm mr-correct-wc.txt
go run mrsequential.go wc.so pg*.txt
sort mr-out-0 > mr-correct-wc.txt