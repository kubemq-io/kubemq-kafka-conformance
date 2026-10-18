// Package verify judges one leg of a client row: the driver's ledger (SCENARIO.md) plus the
// fresh reader's view of the log and of the group's committed offsets. It is pure, so its
// mutation self-tests need no broker.
package verify

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Event is one ledger line. Only the fields of its kind are set.
type Event struct {
	Ev            string `json:"ev"`
	Client        string `json:"client,omitempty"`
	Version       string `json:"version,omitempty"`
	Mode          string `json:"mode,omitempty"`
	GroupProtocol string `json:"group_protocol,omitempty"`
	// AssignmentHidden: the client's consumer API does not expose its group assignment, so the
	// driver cannot report assigned events (start event only).
	AssignmentHidden bool `json:"assignment_hidden,omitempty"`
	// AckOffsetsUnknown: the producer runs without broker acknowledgement (acks=0, a client's
	// stock default), so its acks carry offset -1 and only the content can be checked (start only).
	AckOffsetsUnknown bool        `json:"ack_offsets_unknown,omitempty"`
	Topic             string      `json:"topic,omitempty"`
	Partitions        []int32     `json:"-"`
	NumPartitions     int32       `json:"-"`
	Phase             int         `json:"phase,omitempty"`
	Seq               *int        `json:"seq,omitempty"`
	Partition         int32       `json:"partition"`
	Offset            int64       `json:"offset"`
	ErrorCode         int         `json:"error_code"`
	Error             string      `json:"error,omitempty"`
	Step              string      `json:"step,omitempty"`
	Key               *string     `json:"key"`
	Value             *string     `json:"value"`
	Headers           [][2]string `json:"headers,omitempty"`
	Line              int         `json:"-"`
}

// rawEvent decodes "partitions", which is a count on create and a list on assigned.
type rawEvent struct {
	Event
	RawPartitions json.RawMessage `json:"partitions"`
}

// Ledger is a parsed ledger. Unparseable lines are problems, not errors: a driver that writes
// garbage on standard output has misbehaved, and that must be visible.
type Ledger struct {
	Events   []Event
	Problems []string
}

// ParseLedger reads JSON lines. Blank lines are ignored.
func ParseLedger(r io.Reader) Ledger {
	var l Ledger
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var re rawEvent
		if err := json.Unmarshal([]byte(line), &re); err != nil || re.Ev == "" {
			l.Problems = append(l.Problems, fmt.Sprintf("ledger line %d is not a ledger event: %.120q", n, line))
			continue
		}
		e := re.Event
		e.Line = n
		if len(re.RawPartitions) > 0 {
			var list []int32
			var count int32
			if json.Unmarshal(re.RawPartitions, &list) == nil {
				e.Partitions = list
			} else if json.Unmarshal(re.RawPartitions, &count) == nil {
				e.NumPartitions = count
			}
		}
		l.Events = append(l.Events, e)
	}
	if err := sc.Err(); err != nil {
		l.Problems = append(l.Problems, "ledger unreadable: "+err.Error())
	}
	return l
}
