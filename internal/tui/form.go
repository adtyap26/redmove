package tui

import (
	"fmt"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/bubbles/v2/textinput"
)

type formModel struct {
	spec    FormSpec
	inputs  []textinput.Model // one per text/select field
	toggles []bool            // parallel to spec.Fields, only meaningful for FieldToggle
	focused int               // index into spec.Fields
	errMsg  string
}

func newFormModel(op Operation) formModel {
	spec := specForOperation(op)
	inputs := make([]textinput.Model, len(spec.Fields))
	toggles := make([]bool, len(spec.Fields))

	for i, f := range spec.Fields {
		ti := textinput.New()
		ti.Placeholder = f.Placeholder
		if f.DefaultValue != "" {
			ti.SetValue(f.DefaultValue)
		}
		if f.Kind == FieldSelect && f.DefaultValue == "" && len(f.Choices) > 0 {
			ti.SetValue(f.Choices[0])
		}
		if i == 0 {
			ti.Focus()
		}
		inputs[i] = ti
	}

	return formModel{
		spec:    spec,
		inputs:  inputs,
		toggles: toggles,
	}
}

func (m formModel) Init() tea.Cmd {
	return textinput.Blink
}

func (m formModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch msg.String() {
		case "esc":
			return m, func() tea.Msg { return backToMenuMsg{} }

		case "tab", "down":
			m.focused = (m.focused + 1) % len(m.spec.Fields)
			return m, m.updateFocus()

		case "shift+tab", "up":
			m.focused = (m.focused - 1 + len(m.spec.Fields)) % len(m.spec.Fields)
			return m, m.updateFocus()

		case "enter":
			// If on last field or explicit submit, validate and submit.
			if err := m.validate(); err != "" {
				m.errMsg = err
				return m, nil
			}
			return m, func() tea.Msg {
				return submitFormMsg{op: m.spec.Op, values: m.collectValues()}
			}

		case " ":
			// Toggle for toggle fields.
			if m.spec.Fields[m.focused].Kind == FieldToggle {
				m.toggles[m.focused] = !m.toggles[m.focused]
				return m, nil
			}

		case "left", "right":
			// Cycle select fields.
			f := m.spec.Fields[m.focused]
			if f.Kind == FieldSelect && len(f.Choices) > 0 {
				current := m.inputs[m.focused].Value()
				idx := 0
				for i, c := range f.Choices {
					if c == current {
						idx = i
						break
					}
				}
				if msg.String() == "right" {
					idx = (idx + 1) % len(f.Choices)
				} else {
					idx = (idx - 1 + len(f.Choices)) % len(f.Choices)
				}
				m.inputs[m.focused].SetValue(f.Choices[idx])
				return m, nil
			}
		}
	}

	// Update the focused text input.
	f := m.spec.Fields[m.focused]
	if f.Kind == FieldText || f.Kind == FieldSelect {
		var cmd tea.Cmd
		m.inputs[m.focused], cmd = m.inputs[m.focused].Update(msg)
		return m, cmd
	}

	return m, nil
}

func (m formModel) View() tea.View {
	return tea.NewView(m.viewContent())
}

func (m *formModel) updateFocus() tea.Cmd {
	for i := range m.inputs {
		if i == m.focused && m.spec.Fields[i].Kind != FieldToggle {
			m.inputs[i].Focus()
		} else {
			m.inputs[i].Blur()
		}
	}
	return nil
}

func (m formModel) validate() string {
	for i, f := range m.spec.Fields {
		if !f.Required {
			continue
		}
		switch f.Kind {
		case FieldText, FieldSelect:
			if strings.TrimSpace(m.inputs[i].Value()) == "" {
				return fmt.Sprintf("%s is required", f.Label)
			}
		}
	}
	return ""
}

func (m formModel) collectValues() map[string]string {
	vals := make(map[string]string, len(m.spec.Fields))
	for i, f := range m.spec.Fields {
		switch f.Kind {
		case FieldText, FieldSelect:
			vals[f.Key] = strings.TrimSpace(m.inputs[i].Value())
		case FieldToggle:
			if m.toggles[i] {
				vals[f.Key] = "true"
			} else {
				vals[f.Key] = "false"
			}
		}
	}
	return vals
}

func (m formModel) viewContent() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render(m.spec.Title))
	b.WriteString("\n\n")

	for i, f := range m.spec.Fields {
		isFocused := i == m.focused
		cursor := "  "
		if isFocused {
			cursor = "> "
		}

		label := f.Label
		if f.Required {
			label += requiredStyle.Render(" *")
		}

		switch f.Kind {
		case FieldText:
			b.WriteString(fmt.Sprintf("%s%s\n", cursor, labelStyle.Render(label)))
			b.WriteString(fmt.Sprintf("    %s\n", m.inputs[i].View()))

		case FieldToggle:
			check := "[ ]"
			if m.toggles[i] {
				check = "[x]"
			}
			style := labelStyle
			if isFocused {
				style = menuSelectedStyle
			}
			b.WriteString(fmt.Sprintf("%s%s %s\n", cursor, check, style.Render(label)))

		case FieldSelect:
			b.WriteString(fmt.Sprintf("%s%s\n", cursor, labelStyle.Render(label)))
			// Show choices with current highlighted.
			current := m.inputs[i].Value()
			var choices []string
			for _, c := range f.Choices {
				if c == current {
					choices = append(choices, menuSelectedStyle.Render("["+c+"]"))
				} else {
					choices = append(choices, dimStyle.Render(c))
				}
			}
			b.WriteString(fmt.Sprintf("    %s\n", strings.Join(choices, "  ")))
		}
	}

	if m.errMsg != "" {
		b.WriteString("\n")
		b.WriteString(errorStyle.Render("  " + m.errMsg))
		b.WriteString("\n")
	}

	b.WriteString("\n")
	b.WriteString(footerStyle.Render("tab/shift+tab: navigate  space: toggle  left/right: select  enter: run  esc: back"))

	return b.String()
}
