package server

import (
	"fmt"

	"go.uber.org/zap"
)

func main() {
	fmt.Println("Hello Go")

	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}

	defer log.Sync()
}
