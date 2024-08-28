package resource

import (
	"fmt"
	"os"

	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var docStyle = lipgloss.NewStyle().Margin(1, 2)

type item struct {
	title, desc, id string
}

func (i item) Title() string       { return i.title }
func (i item) Description() string { return i.desc }
func (i item) FilterValue() string { return i.id }

type model struct {
	list list.Model
	Quit bool
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.String() == "ctrl+c" {
			m.Quit = true
			return m, tea.Quit
		}
		if msg.String() == "enter" {
			return m, tea.Quit
		}
	case tea.WindowSizeMsg:
		h, v := docStyle.GetFrameSize()
		m.list.SetSize(msg.Width-h, msg.Height-v)
	}

	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m model) View() string {
	m.list.SetShowStatusBar(false)
	return docStyle.Render(m.list.View())
}

func Run(operation string) string {
	items := []list.Item{
		item{title: "Users", desc: "All project users and their passwords", id: "users"},
		item{title: "Databases", desc: "Data from all collections from all databases", id: "databases"},
		item{title: "Storage", desc: "All files from all buckets", id: "storage"},
	}

	m := model{list: list.New(items, list.NewDefaultDelegate(), 0, 0), Quit: false}
	m.list.Title = "Pick " + operation + " action"

	p := tea.NewProgram(&m, tea.WithAltScreen())

	_, err := p.Run()
	if err != nil {
		fmt.Println("Error running program:", err)
		os.Exit(1)
	}

	if m.Quit {
		return ""
	}

	return m.list.SelectedItem().FilterValue()
}
