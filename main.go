package main

import (
	"os"

	"github.com/akamensky/argparse"

	"github.com/meldiron/appwrite-tools/action"
	"github.com/meldiron/appwrite-tools/connect"
	"github.com/meldiron/appwrite-tools/resource"
)

func main() {
	parser := argparse.NewParser("tools", "Tools to manipulate Appwrite project data")

	actionParam := parser.String("a", "action", &argparse.Options{
		Required: false,
		Help:     "Action to perform. Can be 'backup' or 'restore'",
		Default:  "",
	})

	resourceParam := parser.String("r", "resource", &argparse.Options{
		Required: false,
		Help:     "Resource to action upon. Can be 'users' or 'databases' or 'storage'",
		Default:  "",
	})

	endpointParam := parser.String("e", "endpoint", &argparse.Options{
		Required: false,
		Help:     "Appwrite endpoint, like https://cloud.appwrite.io/v1",
		Default:  "https://cloud.appwrite.io/v1",
	})

	projectIdParam := parser.String("p", "project-id", &argparse.Options{
		Required: false,
		Help:     "Appwrite Project ID, like 66cc768b003728bc3490",
	})

	apiKeyParam := parser.String("k", "api-key", &argparse.Options{
		Required: false,
		Help:     "Appwrite API key, like standard_a77a6f...ca65",
		Default:  "",
	})

	err := parser.Parse(os.Args)
	if err != nil {
		println(parser.Usage(err))
	}

	actionValue := *actionParam
	resourceValue := *resourceParam
	endpointValue := *endpointParam
	apiKeyValue := *apiKeyParam
	projectIdValue := *projectIdParam

	if actionValue == "" {
		actionValue = action.Run()
	}

	if actionValue == "" {
		println("Action not provided. Exiting now.")
		os.Exit(1)
	}

	if resourceValue == "" {
		resourceValue = resource.Run(actionValue)
	}

	if resourceValue == "" {
		println("Resource not provided. Exiting now.")
		os.Exit(1)
	}

	if endpointValue == "" || apiKeyValue == "" || projectIdValue == "" {
		connectResult := connect.Run(endpointValue, apiKeyValue, projectIdValue)
		endpointValue = connectResult.Endpoint
		projectIdValue = connectResult.ProjectId
		apiKeyValue = connectResult.ApiKey
	}

	if endpointValue == "" {
		println("Endpoint not provided. Exiting now.")
		os.Exit(1)
	}

	if apiKeyValue == "" {
		println("API key not provided. Exiting now.")
		os.Exit(1)
	}

	if projectIdValue == "" {
		println("Project ID not provided. Exiting now.")
		os.Exit(1)
	}

	println(actionValue, resourceValue, endpointValue, projectIdParam, apiKeyValue)
}
