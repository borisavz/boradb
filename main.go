package main

var engine *Engine

func main() {
	engine = InitializeEngine()
	InitializeHTTP()
}
