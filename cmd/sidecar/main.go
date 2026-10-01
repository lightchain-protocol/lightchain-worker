package main

import (
	"os"

	"github.com/lightchain/worker/internal/service"
)

func main() {
	os.Exit(service.Main())
}
