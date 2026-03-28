package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/bubbles/v2/progress"
	"charm.land/bubbles/v2/spinner"
	"github.com/adtyap26/redmove/internal/pipeline"
)

type runPhase int

const (
	phaseRunning runPhase = iota
	phaseDone
)

// resultMsg is sent when the operation finishes.
type resultMsg struct {
	result ExecuteResult
}

// startMsg carries ctx/cancel and pre-fetched total back into the model.
type startMsg struct {
	ctx    context.Context
	cancel context.CancelFunc
	total  int64
}

// statsReadyMsg is sent when the pipeline has started and its stats are accessible.
type statsReadyMsg struct {
	stats    *pipeline.Stats
	resultCh <-chan ExecuteResult
}

// tickMsg fires periodically to refresh the display.
type tickMsg time.Time

type runModel struct {
	op        Operation
	values    map[string]string
	phase     runPhase
	spinner   spinner.Model
	progress  progress.Model
	result    ExecuteResult
	cancel    context.CancelFunc
	elapsed   time.Duration
	start     time.Time
	liveStats *pipeline.Stats
	total     int64
}

func newRunModel(op Operation, values map[string]string) runModel {
	s := spinner.New()
	s.Spinner = spinner.Dot

	prog := progress.New(progress.WithDefaultBlend(), progress.WithWidth(50))

	return runModel{
		op:       op,
		values:   values,
		phase:    phaseRunning,
		spinner:  s,
		progress: prog,
		start:    time.Now(),
	}
}

func (m runModel) Init() tea.Cmd {
	op := m.op
	vals := m.values
	return tea.Batch(
		m.spinner.Tick,
		tickCmd(),
		func() tea.Msg {
			ctx, cancel := context.WithCancel(context.Background())
			total := fetchTotal(op, vals)
			return startMsg{ctx: ctx, cancel: cancel, total: total}
		},
	)
}

func (m runModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case startMsg:
		m.cancel = msg.cancel
		m.total = msg.total
		ctx := msg.ctx
		op := m.op
		vals := m.values

		statsCh := make(chan *pipeline.Stats, 1)
		resultCh := make(chan ExecuteResult, 1)

		go func() {
			resultCh <- executeOperation(ctx, op, vals, func(s *pipeline.Stats) {
				statsCh <- s
			})
		}()

		return m, func() tea.Msg {
			select {
			case s := <-statsCh:
				return statsReadyMsg{stats: s, resultCh: resultCh}
			case result := <-resultCh:
				return resultMsg{result: result}
			}
		}

	case statsReadyMsg:
		m.liveStats = msg.stats
		resultCh := msg.resultCh
		return m, func() tea.Msg {
			return resultMsg{result: <-resultCh}
		}

	case resultMsg:
		m.phase = phaseDone
		m.result = msg.result
		m.elapsed = time.Since(m.start)
		return m, nil

	case tickMsg:
		if m.phase == phaseRunning {
			m.elapsed = time.Since(m.start)
			return m, tickCmd()
		}
		return m, nil

	case tea.KeyPressMsg:
		if m.phase == phaseDone {
			switch msg.String() {
			case "enter":
				return m, func() tea.Msg { return backToMenuMsg{} }
			case "q":
				return m, tea.Quit
			}
		}
		if msg.String() == "ctrl+c" && m.phase == phaseRunning {
			if m.cancel != nil {
				m.cancel()
			}
			return m, nil
		}

	case spinner.TickMsg:
		if m.phase == phaseRunning {
			var cmd tea.Cmd
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
	}

	return m, nil
}

func (m runModel) View() tea.View {
	return tea.NewView(m.viewContent())
}

func (m runModel) viewContent() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render(m.op.String()))
	b.WriteString("\n\n")

	if m.phase == phaseRunning {
		b.WriteString(fmt.Sprintf("  %s Running %s...\n\n", m.spinner.View(), m.op.String()))

		if m.liveStats != nil {
			written := m.liveStats.RecordsWritten.Load()

			if m.total > 0 {
				pct := float64(written) / float64(m.total)
				if pct > 1.0 {
					pct = 1.0
				}
				b.WriteString("  " + m.progress.ViewAs(pct) + "\n\n")
				b.WriteString(fmt.Sprintf("  %s %d / %d\n",
					statsLabelStyle.Render("records:"), written, m.total))
			} else {
				b.WriteString(fmt.Sprintf("  %s %d\n",
					statsLabelStyle.Render("records:"), written))
			}

			if m.elapsed > 0 && written > 0 {
				rps := float64(written) / m.elapsed.Seconds()
				b.WriteString(fmt.Sprintf("  %s %.0f/sec\n",
					statsLabelStyle.Render("speed:"), rps))
			}
		}

		b.WriteString(fmt.Sprintf("  %s %s\n",
			statsLabelStyle.Render("elapsed:"), m.elapsed.Truncate(time.Second)))

		b.WriteString("\n")
		b.WriteString(footerStyle.Render("ctrl+c: cancel"))
	} else {
		if m.result.Err != nil {
			b.WriteString(errorStyle.Render("  Error: "+m.result.Err.Error()) + "\n\n")
		} else {
			b.WriteString(successStyle.Render("  Completed successfully") + "\n\n")
		}

		if m.result.Summary != "" {
			for _, line := range strings.Split(m.result.Summary, "\n") {
				b.WriteString("  " + line + "\n")
			}
		}

		b.WriteString("\n")
		b.WriteString(footerStyle.Render("enter: back to menu  q: quit"))
	}

	return b.String()
}

func tickCmd() tea.Cmd {
	return tea.Tick(200*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}
