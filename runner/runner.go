package runner

// A simple example that shows how to render a progress bar in a "pure"
// fashion. In this example we bump the progress by 25% every second,
// maintaining the progress state on our top level model using the progress bar
// model's ViewAs method only for rendering.
//
// The signature for ViewAs is:
//
//     func (m Model) ViewAs(percent float64) string
//
// So it takes a float between 0 and 1, and renders the progress bar
// accordingly. When using the progress bar in this "pure" fashion and there's
// no need to call an Update method.
//
// The progress bar is also able to animate itself, however. For details see
// the progress-animated example.

import (
	"fmt"
	"os"
	"strings"
	"time"

	"encoding/csv"
	"encoding/json"

	"github.com/appwrite/sdk-for-go/appwrite"
	"github.com/appwrite/sdk-for-go/client"
	"github.com/appwrite/sdk-for-go/query"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type RunnerOptions struct {
	Endpoint  string
	ProjectId string
	ApiKey    string
	Action    string
	Resource  string
}

const (
	padding  = 2
	maxWidth = 80
)

var helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#626262")).Render

func Run(options RunnerOptions) string {
	prog := progress.New(progress.WithScaledGradient("#fd356e", "#fe526c"))

	app := model{
		progress: prog,
		options:  options,
		fileName: "",
	}

	if _, err := tea.NewProgram(&app).Run(); err != nil {
		fmt.Println("Oh no!", err)
		os.Exit(1)
	}

	return app.fileName
}

type tickMsg time.Time

type model struct {
	fileName string
	percent  float64
	progress progress.Model
	options  RunnerOptions
}

func (m *model) Init() tea.Cmd {
	go func() {
		ActionRunner(m)
	}()

	return tickCmd()
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			return m, tea.Quit
		}

	case tea.WindowSizeMsg:
		m.progress.Width = msg.Width - padding*2 - 4
		if m.progress.Width > maxWidth {
			m.progress.Width = maxWidth
		}
		return m, nil

	case tickMsg:
		if m.percent >= 1.0 {
			return m, tea.Quit
		}
		return m, tickCmd()

	default:
		return m, nil
	}

	return m, nil
}

func (m model) View() string {
	pad := strings.Repeat(" ", padding)
	return "\n" +
		pad + m.progress.ViewAs(m.percent) + "\n\n" +
		pad + helpStyle("Press any key to quit")
}

func tickCmd() tea.Cmd {
	return tea.Tick(time.Millisecond*50, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func ActionRunner(m *model) {
	response := false
	if m.options.Action == "backup" {
		if m.options.Resource == "users" {
			response = BackupUsers(m)
		}
		if m.options.Resource == "databases" {
			response = BackupDatabases(m)
		}
	}

	if !response {
		os.Exit(0)
	}
}

func NewClient(m *model) client.Client {
	client := appwrite.NewClient(
		appwrite.WithEndpoint(m.options.Endpoint),
		appwrite.WithProject(m.options.ProjectId),
		appwrite.WithKey(m.options.ApiKey),
	)

	return client
}

func WriteFile(m *model, column string, cb func(client client.Client, lastId *string, writer *csv.Writer) (string, int, int)) {
	file, err := os.Create(m.fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{column}
	writer.Write(headers)

	client := NewClient(m)

	total := 0
	current := 0

	lastId := ""

	for {
		cbLastId, cbCurrent, cbTotal := cb(client, &lastId, writer)

		lastId = cbLastId
		current = cbCurrent

		if total == 0 {
			total = cbTotal
		}

		if current >= total {
			total += cbCurrent
		}

		if total == 0 || current == 0 {
			m.percent = 0
		} else {
			m.percent = float64(current) / float64(total)
		}

		if lastId == "" {
			m.percent = 1
			break
		}
	}
}

func BackupUsers(m *model) bool {
	m.fileName = "backup_" + m.options.ProjectId + "_users_" + time.Now().Format(time.RFC3339) + ".csv"
	WriteFile(m, "users", func(client client.Client, lastId *string, writer *csv.Writer) (string, int, int) {
		users := appwrite.NewUsers(client)

		queries := []string{
			query.Limit(1),
		}

		if *lastId != "" {
			queries = append(queries, query.CursorAfter(*lastId))
		}

		response, err := users.List(users.WithListQueries(queries))
		if err != nil {
			panic(err)
		}

		for _, user := range response.Users {
			json, err := json.Marshal(user)
			if err != nil {
				panic(err)
			}

			writer.Write([]string{string(json)})
		}

		if len(response.Users) == 0 {
			return "", len(response.Users), response.Total
		}

		return response.Users[len(response.Users)-1].Id, len(response.Users), response.Total
	})

	return true
}

func BackupDatabases(m *model) bool {
	c := NewClient(m)
	databases := appwrite.NewDatabases(c)

	dbsResponse, err := databases.List(databases.WithListQueries([]string{
		query.Limit(1000),
	}))

	if err != nil {
		panic(err)
	}

	for _, database := range dbsResponse.Databases {
		colsResponse, err := databases.ListCollections(database.Id, databases.WithListCollectionsQueries([]string{
			query.Limit(1000),
		}))

		if err != nil {
			panic(err)
		}

		for _, collection := range colsResponse.Collections {
			m.fileName = "backup_" + m.options.ProjectId + "_db_ " + database.Id + "_col_" + collection.Id + "_" + time.Now().Format(time.RFC3339) + ".csv"

			WriteFile(m, "docments", func(client client.Client, lastId *string, writer *csv.Writer) (string, int, int) {
				databases := appwrite.NewDatabases(client)

				queries := []string{
					query.Limit(1),
				}

				if *lastId != "" {
					queries = append(queries, query.CursorAfter(*lastId))
				}

				response, err := databases.ListDocuments(database.Id, collection.Id, databases.WithListDocumentsQueries(queries))
				if err != nil {
					panic(err)
				}

				for _, doc := range response.Documents {
					json, err := json.Marshal(doc)
					if err != nil {
						panic(err)
					}

					writer.Write([]string{string(json)})
				}

				if len(response.Documents) == 0 {
					return "", len(response.Documents), response.Total
				}

				return response.Documents[len(response.Documents)-1].Id, len(response.Documents), response.Total
			})
		}
	}

	return true
}
