build:
	go build -buildmode=c-shared -o out_arrow.so main.go

run:
	fluent-bit -e ./out_arrow.so -c conf/example.cfg
