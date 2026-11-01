package app

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-manager/types"
)

func TestDataEngineModeSkipsOnlyTheISCSIHostCheck(t *testing.T) {
	sentinel := errors.New("initiator unavailable")
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only, "unknown"} {
		called := 0
		err := checkISCSIForDataEngineMode(mode, func() error {
			called++
			return sentinel
		})
		switch mode {
		case types.DataEngineModeV2Only:
			if err != nil || called != 0 {
				t.Fatalf("V2-only required an iSCSI host executor: calls=%d err=%v", called, err)
			}
		case "unknown":
			if err == nil || called != 0 || !strings.Contains(err.Error(), "unsupported data engine mode") {
				t.Fatalf("unknown mode reached the host executor: calls=%d err=%v", called, err)
			}
		default:
			if !errors.Is(err, sentinel) || called != 1 {
				t.Fatalf("default mode lost the host prerequisite or its error: calls=%d err=%v", called, err)
			}
		}
	}
}

func TestDataEngineModeFlagAtEveryEntrypoint(t *testing.T) {
	for _, constructor := range []func() *cli.Command{DaemonCmd, GlobalCmd, PreUpgradeCmd, SystemRolloutCmd} {
		for _, test := range []struct {
			input, want string
			valid       bool
		}{
			{"", "v1-capable", true},
			{"v1-capable", "v1-capable", true},
			{"v2-only", "v2-only", true},
			{"v2", "", false},
			{"V2-ONLY", "", false},
			{" v2-only", "", false},
		} {
			command := constructor()
			t.Run(command.Name+"/"+test.input, func(t *testing.T) {
				called, observed := false, ""
				// Exercise the actual entrypoint's flag parser without starting
				// its host executor, clients or process-wide fatal handler.
				command.Action = func(_ context.Context, c *cli.Command) error {
					called, observed = true, c.String("data-engine-mode")
					return nil
				}
				command.Writer, command.ErrWriter = io.Discard, io.Discard
				args := []string{"manager"}
				if test.input != "" {
					args = append(args, "--data-engine-mode", test.input)
				}
				if command.Name == "pre-upgrade" {
					args = append(args, "--namespace", "longhorn-system")
				}
				err := command.Run(t.Context(), args)
				if test.valid {
					if err != nil || !called || observed != test.want {
						t.Fatalf("mode intake: called=%t observed=%q err=%v", called, observed, err)
					}
				} else if err == nil || called || !strings.Contains(err.Error(), "unsupported data engine mode") {
					t.Fatalf("invalid mode reached the command action or lost its diagnostic: called=%t err=%v", called, err)
				}
			})
		}
	}
}
