package tui

import "charm.land/lipgloss/v2"

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FF6F61")).
			MarginBottom(1)

	subtitleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#ABABAB"))

	menuItemStyle = lipgloss.NewStyle().
			PaddingLeft(2)

	menuSelectedStyle = lipgloss.NewStyle().
				PaddingLeft(2).
				Foreground(lipgloss.Color("#FF6F61")).
				Bold(true)

	menuDescStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#666666"))

	labelStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#ABABAB")).
			Bold(true)

	requiredStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF6F61"))

	errorStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF4444")).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#44FF88")).
			Bold(true)

	dimStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#666666"))

	statsLabelStyle = lipgloss.NewStyle().
			Width(20).
			Foreground(lipgloss.Color("#ABABAB"))

	statsValueStyle = lipgloss.NewStyle().
			Bold(true)

	footerStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#666666")).
			MarginTop(1)
)
