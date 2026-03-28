package cmd

import (
	"fmt"

	tea "charm.land/bubbletea/v2"
	"github.com/adtyap26/redmove/internal/tui"
	"github.com/spf13/cobra"
)

func newTuiCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tui",
		Short: "Launch interactive TUI mode",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := tui.NewApp()
			p := tea.NewProgram(app)
			_, err := p.Run()
			if err != nil {
				return fmt.Errorf("TUI error: %w", err)
			}
			return nil
		},
	}
}
