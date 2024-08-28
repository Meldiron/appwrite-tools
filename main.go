package main

import (
	"os"

	"github.com/meldiron/appwrite-tools/action"
	"github.com/meldiron/appwrite-tools/connect"
	"github.com/meldiron/appwrite-tools/resource"
)

func main() {
	actionResult := action.Run()

	if actionResult == "" {
		println("Action not provided. Exiting now.")
		os.Exit(1)
	}

	resourceResult := resource.Run(actionResult)

	if resourceResult == "" {
		println("Resource not provided. Exiting now.")
		os.Exit(1)
	}

	connectResult := connect.Run()

	if connectResult.Endpoint == "" {
		println("Endpoint not provided. Exiting now.")
		os.Exit(1)
	}

	if connectResult.ApiKey == "" {
		println("API key not provided. Exiting now.")
		os.Exit(1)
	}

	println(actionResult, resourceResult, connectResult.Endpoint, connectResult.ApiKey)
}
