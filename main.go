package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"

	"github.com/consulthys/retsbeat/beater"
)

func main() {
	err := beat.Run("retsbeat", "", beater.New)
	if err != nil {
		os.Exit(1)
	}
}
