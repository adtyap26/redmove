package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
)

type menuItem struct {
	op   Operation
	name string
	desc string
}

var menuItems = []menuItem{
	{OpPing, "Ping", "Verify connectivity and measure latency"},
	{OpReplicate, "Replicate", "Copy keys from source to target Redis"},
	{OpStats, "Stats", "Analyze keyspace: types, prefixes, TTL"},
	{OpImport, "Import", "Load file (CSV/JSON/JSONL) into Redis"},
	{OpExport, "Export", "Dump Redis keys to file"},
	{OpGenerate, "Generate", "Generate synthetic data into Redis"},
	{OpCompare, "Compare", "Sample-based diff between two Redis instances"},
}

type menuModel struct {
	cursor int
}

func newMenuModel() menuModel {
	return menuModel{}
}

func (m menuModel) Init() tea.Cmd {
	return nil
}

func (m menuModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(menuItems)-1 {
				m.cursor++
			}
		case "enter":
			return m, func() tea.Msg {
				return selectOperationMsg{op: menuItems[m.cursor].op}
			}
		case "q":
			return m, tea.Quit
		}
	}
	return m, nil
}

func (m menuModel) View() tea.View {
	return tea.NewView(m.viewContent())
}

func (m menuModel) viewContent() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("Redmove"))
	b.WriteString("\n")
	b.WriteString(subtitleStyle.Render("Redis Data Migration & Operations Tool"))
	b.WriteString("\n\n")

	for i, item := range menuItems {
		cursor := "  "
		nameStyle := menuItemStyle
		if i == m.cursor {
			cursor = "> "
			nameStyle = menuSelectedStyle
		}

		line := fmt.Sprintf("%s%-12s %s", cursor, item.name, menuDescStyle.Render(item.desc))
		b.WriteString(nameStyle.Render(line))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(footerStyle.Render("j/k: navigate  enter: select  q: quit"))

	return b.String()
}
