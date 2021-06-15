.DEFAULT_GOAL=run
run: build	
	@./main

build:
	@go build -o main .
