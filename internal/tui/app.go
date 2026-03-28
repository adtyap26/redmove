package tui

import tea "charm.land/bubbletea/v2"

// screen identifies which sub-model is active.
type screen int

const (
	screenMenu screen = iota
	screenForm
	screenRun
)

// Transition messages.
type selectOperationMsg struct{ op Operation }
type submitFormMsg struct {
	op     Operation
	values map[string]string
}
type backToMenuMsg struct{}

// App is the top-level TUI model.
type App struct {
	current screen
	menu    menuModel
	form    formModel
	run     runModel
}

// NewApp creates the top-level App starting at the menu screen.
func NewApp() App {
	return App{
		current: screenMenu,
		menu:    newMenuModel(),
	}
}

func (a App) Init() tea.Cmd {
	return a.menu.Init()
}

func (a App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// Global key handling.
	if km, ok := msg.(tea.KeyPressMsg); ok {
		if km.String() == "ctrl+c" && a.current != screenRun {
			return a, tea.Quit
		}
	}

	// Screen transition messages.
	switch msg := msg.(type) {
	case selectOperationMsg:
		a.form = newFormModel(msg.op)
		a.current = screenForm
		return a, a.form.Init()

	case submitFormMsg:
		a.run = newRunModel(msg.op, msg.values)
		a.current = screenRun
		return a, a.run.Init()

	case backToMenuMsg:
		a.menu = newMenuModel()
		a.current = screenMenu
		return a, nil
	}

	// Delegate to active sub-model.
	var cmd tea.Cmd
	switch a.current {
	case screenMenu:
		updated, c := a.menu.Update(msg)
		a.menu = updated.(menuModel)
		cmd = c
	case screenForm:
		updated, c := a.form.Update(msg)
		a.form = updated.(formModel)
		cmd = c
	case screenRun:
		updated, c := a.run.Update(msg)
		a.run = updated.(runModel)
		cmd = c
	}
	return a, cmd
}

func (a App) View() tea.View {
	var content string
	switch a.current {
	case screenMenu:
		content = a.menu.viewContent()
	case screenForm:
		content = a.form.viewContent()
	case screenRun:
		content = a.run.viewContent()
	}
	v := tea.NewView(content)
	v.AltScreen = true
	return v
}
