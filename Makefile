build:
	go build -buildmode=c-shared -o out_arrow.so main.go

run_example:
	fluent-bit -e ./out_arrow.so -c examples/conf/example.cfg
