package main

import (
	"fmt"
	"github.com/dantin/syncer/config"
)

func main() {
	cfg := config.NewConfig()
	fmt.Println(cfg)
}
