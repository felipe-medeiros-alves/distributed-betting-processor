package main

import (
	"github.com/felipemalves/distributed-betting-processor/internal/app"
	"go.uber.org/fx"
)

func main() {
	fx.New(app.Module()).Run()
}
