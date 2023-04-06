package main

import (
	"github.com/gin-gonic/gin"
	"log"
)

var r *gin.Engine

func main() {
	log.Println("Hello, World!")
	configEngine(r)
}

func configEngine(r *gin.Engine) bool {
	return true
}
