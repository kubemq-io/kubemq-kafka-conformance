package verify

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// Spec is what one row asks a driver to do (SCENARIO.md).
type Spec struct {
	Mode       string // "scenario" or "stock-defaults"
	Client     string // driver's client name, as it reports it in the start event
	Version    string // pinned client version the driver must have loaded
	Partitions int32  // partitions asked for; 0 means the broker default (stock-defaults)
	Records    int    // produce phase 1
	Records2   int    // produce phase 2 (scenario only)
	Keys       int    // distinct keys (scenario only)
	Admin      bool   // the driver creates the topic itself
	// Limits are client limitations the matrix row allows (and so publishes): "assignment-hidden"
	// (the consumer API does not expose its group assignment) and "ack-offsets-unknown" (the
	// producer reports no offsets: acks=0 by default, or a tool that prints none), and
	// "headers-per-run" (a command-line tool sets headers for a whole run, not per record, so
	// records carry only the client header), and "commit-on-close" (the consumer commits only
	// on close, by the client's own auto commit, so no explicit commit is reported; the committed
	// offsets are still checked). A driver that declares a limit its row does not allow is a problem.
	Limits []string
}

const (
	LimitAssignmentHidden  = "assignment-hidden"
	LimitAckOffsetsUnknown = "ack-offsets-unknown"
	LimitHeadersPerRun     = "headers-per-run"
	LimitCommitOnClose     = "commit-on-close"
)

// Limits is every limit a row may allow.
var Limits = []string{LimitAssignmentHidden, LimitAckOffsetsUnknown, LimitHeadersPerRun, LimitCommitOnClose}

func (s Spec) allows(limit string) bool {
	for _, l := range s.Limits {
		if l == limit {
			return true
		}
	}
	return false
}

// LogRecord is one record as the fresh reader found it.
type LogRecord struct {
	Partition int32
	Offset    int64
	Key       *string
	Value     *string
	Headers   [][2]string
	Timestamp int64 // milliseconds since the epoch, as stored
	CreateTS  bool  // the batch carries the producer's create time (not log-append time)
}

// timestampSlack bounds how far a stored create time may lie outside the leg's own wall-clock
// window (driver and broker clocks are the same host's, or synced; the preflight checks skew).
const timestampSlack = 60 * 1000

// View is the fresh reader's view after the driver finished: every partition read from the
// start to its high watermark, and the group's committed offsets.
type View struct {
	Partitions     int32
	Records        []LogRecord
	HighWatermarks map[int32]int64
	Committed      map[int32]int64 // partitions with no committed offset are absent
	ReadError      string          // the fresh reader could not complete
	// LegStartMS and LegEndMS bound the leg in wall-clock milliseconds, for the timestamp check;
	// zero skips it (unit tests that do not model time).
	LegStartMS, LegEndMS int64
}

// Observation is the comparable summary of one leg; it holds no timings or broker-assigned ids.
type Observation map[string]any

// maxListed bounds how many examples one problem names.
const maxListed = 5

const (
	scenarioMode   = "scenario"
	stockMode      = "stock-defaults"
	verifiableMode = "verifiable" // Kafka's own kafka-verifiable-producer and -consumer
)

// keyed: records carry keys whose partition affinity and order are checked.
func (s Spec) keyed() bool { return s.Mode != stockMode && s.Keys > 0 }

// explicitCommits: the consumer commits explicitly and must end at the log end.
func (s Spec) explicitCommits() bool { return s.Mode != stockMode }

// Expected is the content of record seq under spec (SCENARIO.md, "Record content").
func (s Spec) Expected(seq int) LogRecord {
	if s.Mode == verifiableMode {
		// The verifiable producer sends the message number as the value and, with
		// --repeating-keys, the number modulo the key count as the key.
		k, v := strconv.Itoa(seq%s.Keys), strconv.Itoa(seq)
		return LogRecord{Key: &k, Value: &v}
	}
	v := fmt.Sprintf("value-%06d", seq)
	r := LogRecord{Value: &v}
	if s.Mode == stockMode {
		return r
	}
	k := fmt.Sprintf("key-%02d", seq%s.Keys)
	r.Key = &k
	r.Headers = [][2]string{{"seq", strconv.Itoa(seq)}, {"client", s.Client}}
	if s.allows(LimitHeadersPerRun) {
		r.Headers = [][2]string{{"client", s.Client}}
	}
	return r
}

func (s Spec) total() int { return s.Records + s.Records2 }

// phaseOf is the produce/consume phase record seq belongs to.
func (s Spec) phaseOf(seq int) int {
	if seq < s.Records {
		return 1
	}
	return 2
}

// SeqOf recovers the record number from its value ("value-NNNNNN", or the bare number in
// verifiable mode), or -1.
func (s Spec) SeqOf(value *string) int {
	if value == nil {
		return -1
	}
	v := *value
	if s.Mode != verifiableMode {
		if !strings.HasPrefix(v, "value-") {
			return -1
		}
		v = strings.TrimPrefix(v, "value-")
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

type pos struct {
	p int32
	o int64
}

// Check judges one leg. Problems are named defects; an empty list means the leg is clean.
func Check(spec Spec, l Ledger, v View) (Observation, []string) {
	c := &checker{spec: spec, problems: append([]string(nil), l.Problems...)}
	c.envelope(l.Events)
	acks := c.acks(l.Events)
	logPos := c.log(v, acks)
	// A client that reports no offsets (limit ack-offsets-unknown) is held to the positions the
	// fresh reader found, so key affinity, per-key order and delivered positions are still checked.
	for seq, p := range acks {
		if p == unknownPos {
			if lp, ok := logPos[seq]; ok {
				acks[seq] = lp
			}
		}
	}
	if c.spec.keyed() {
		c.keyOrder(acks)
	}
	c.positions(l.Events, acks)
	delivered := map[int]int{}
	for _, ph := range c.phases() {
		delivered[ph] = c.consumePhase(ph, l.Events, acks)
	}
	committedAtEnd := c.commits(l.Events, v)
	obs := Observation{
		"mode":             spec.Mode,
		"client_version":   c.version,
		"group_protocol":   c.groupProtocol,
		"partitions":       v.Partitions,
		"records_acked":    len(acks),
		"log_records":      len(v.Records),
		"log_timestamps":   timestampKind(v),
		"phase1_delivered": delivered[1],
		"committed_at_end": committedAtEnd,
	}
	if spec.Mode == scenarioMode {
		obs["phase2_delivered"] = delivered[2]
	}
	if spec.keyed() {
		// Keyed records land on partitions by the client's own partitioner, so the distribution
		// is deterministic and must match the oracle's: a broker that ignores the requested
		// partition shows here even when the client reports no offsets.
		obs["log_records_per_partition"] = perPartition(v)
	}
	if spec.Admin {
		obs["create_error_code"] = c.createCode
	}
	return obs, c.problems
}

func perPartition(v View) []int {
	out := make([]int, v.Partitions)
	for _, r := range v.Records {
		if r.Partition >= 0 && r.Partition < v.Partitions {
			out[r.Partition]++
		}
	}
	return out
}

// timestampKind summarizes the stored timestamp type: "create-time", "log-append-time", "mixed"
// or "none" (no records).
func timestampKind(v View) string {
	create, appendT := 0, 0
	for _, r := range v.Records {
		if r.CreateTS {
			create++
		} else {
			appendT++
		}
	}
	switch {
	case create == 0 && appendT == 0:
		return "none"
	case appendT == 0:
		return "create-time"
	case create == 0:
		return "log-append-time"
	}
	return "mixed"
}

// positions: a stock-defaults consumer's reported start position (the readiness barrier) must be
// resolved and must not lie after the first record sent to that partition. Scenario consumers
// report positions too (a resumed member starts at its committed offset), which the delivered-
// records checks already judge, so only stock defaults is checked here.
func (c *checker) positions(evs []Event, acks map[int]pos) {
	if c.spec.Mode != stockMode {
		return
	}
	first := map[int32]int64{}
	for _, p := range acks {
		if p == unknownPos {
			continue
		}
		if o, ok := first[p.p]; !ok || p.o < o {
			first[p.p] = p.o
		}
	}
	var bad []string
	for _, e := range evs {
		if e.Ev != "position" {
			continue
		}
		f, ok := first[e.Partition]
		switch {
		case e.Offset < 0:
			bad = append(bad, fmt.Sprintf("partition %d reported unresolved position %d", e.Partition, e.Offset))
		case ok && e.Offset > f:
			bad = append(bad, fmt.Sprintf("partition %d started at %d, after the first record sent (offset %d)", e.Partition, e.Offset, f))
		}
	}
	c.list("consumer start positions that would miss records (readiness barrier failed)", bad)
}

type checker struct {
	spec          Spec
	problems      []string
	version       string
	groupProtocol string
	createCode    int
	hiddenAssign  bool
	ackUnknown    bool
}

// unknownPos marks a record acknowledged without an offset (acks=0).
var unknownPos = pos{-1, -1}

func (c *checker) fail(format string, a ...any) {
	c.problems = append(c.problems, fmt.Sprintf(format, a...))
}

func (c *checker) phases() []int {
	if c.spec.Mode == scenarioMode {
		return []int{1, 2}
	}
	return []int{1}
}

// envelope: start first, done last, the pinned client, no error event, the create call.
func (c *checker) envelope(evs []Event) {
	c.createCode = -2 // no create event
	if len(evs) == 0 {
		c.fail("ledger is empty: the driver wrote nothing")
		return
	}
	if s := evs[0]; s.Ev != "start" {
		c.fail("ledger does not begin with a start event (first is %q)", s.Ev)
	} else {
		c.version, c.groupProtocol, c.hiddenAssign, c.ackUnknown = s.Version, s.GroupProtocol, s.AssignmentHidden, s.AckOffsetsUnknown
		if s.AckOffsetsUnknown && !c.spec.allows(LimitAckOffsetsUnknown) {
			c.fail("driver reports no acknowledged offsets, which its matrix row does not allow (limit %s)", LimitAckOffsetsUnknown)
			c.ackUnknown = false
		}
		if s.AssignmentHidden && !c.spec.allows(LimitAssignmentHidden) {
			c.fail("driver reports no group assignment, which its matrix row does not allow (limit %s)", LimitAssignmentHidden)
			c.hiddenAssign = false
		}
		if s.Client != c.spec.Client {
			c.fail("driver reports client %q, row expects %q", s.Client, c.spec.Client)
		}
		if s.Version != c.spec.Version {
			c.fail("driver loaded %s %q, the row pins %q", s.Client, s.Version, c.spec.Version)
		}
		if s.Mode != c.spec.Mode {
			c.fail("driver ran mode %q, row asks %q", s.Mode, c.spec.Mode)
		}
	}
	if last := evs[len(evs)-1]; last.Ev != "done" {
		c.fail("ledger does not end with a done event (last is %q at line %d): the driver stopped early", last.Ev, last.Line)
	}
	creates := 0
	for _, e := range evs {
		switch e.Ev {
		case "error":
			c.fail("driver step %q failed: error code %d: %s", e.Step, e.ErrorCode, e.Error)
		case "create":
			creates++
			c.createCode = e.ErrorCode
			if e.ErrorCode != 0 {
				c.fail("create topic: error code %d %s", e.ErrorCode, e.Error)
			}
			if c.spec.Partitions > 0 && e.NumPartitions != c.spec.Partitions {
				c.fail("driver asked for %d partitions, row asks %d", e.NumPartitions, c.spec.Partitions)
			}
		}
	}
	if c.spec.Admin && creates != 1 {
		c.fail("expected one create event, found %d", creates)
	}
	if !c.spec.Admin && creates != 0 {
		c.fail("the track creates this client's topic, yet the driver reported %d create events", creates)
	}
}

// acks: every record acknowledged exactly once, without error, at a unique position; each
// key's records on one partition in send order. Returns seq -> acknowledged position.
func (c *checker) acks(evs []Event) map[int]pos {
	out := map[int]pos{}
	byPos := map[pos]int{}
	var dup, bad, errs []string
	for _, e := range evs {
		if e.Ev != "ack" {
			continue
		}
		if e.Seq == nil || *e.Seq < 0 || *e.Seq >= c.spec.total() {
			bad = append(bad, fmt.Sprintf("line %d", e.Line))
			continue
		}
		seq := *e.Seq
		if e.ErrorCode != 0 || e.Error != "" {
			errs = append(errs, fmt.Sprintf("seq %d: error code %d %s", seq, e.ErrorCode, e.Error))
			continue
		}
		if e.Phase != c.spec.phaseOf(seq) {
			bad = append(bad, fmt.Sprintf("seq %d reported in phase %d", seq, e.Phase))
		}
		if _, ok := out[seq]; ok {
			dup = append(dup, strconv.Itoa(seq))
			continue
		}
		if c.ackUnknown && e.Offset == -1 {
			out[seq] = unknownPos
			continue
		}
		if e.Offset < 0 || e.Partition < 0 {
			bad = append(bad, fmt.Sprintf("seq %d acknowledged at partition %d offset %d", seq, e.Partition, e.Offset))
			continue
		}
		p := pos{e.Partition, e.Offset}
		if other, ok := byPos[p]; ok {
			c.fail("partition %d offset %d acknowledged for two records (seq %d and %d)", p.p, p.o, other, seq)
		}
		byPos[p] = seq
		out[seq] = p
	}
	c.list("acknowledgements with an error", errs)
	c.list("records acknowledged more than once", dup)
	c.list("malformed acknowledgements", bad)
	var missing []string
	for seq := 0; seq < c.spec.total(); seq++ {
		if _, ok := out[seq]; !ok {
			missing = append(missing, strconv.Itoa(seq))
		}
	}
	c.list("records never acknowledged", missing)
	return out
}

func (c *checker) keyOrder(acks map[int]pos) {
	part := map[int]int32{}
	last := map[int]pos{}
	var split, order []string
	for seq := 0; seq < c.spec.total(); seq++ {
		p, ok := acks[seq]
		if !ok || p == unknownPos {
			continue
		}
		k := seq % c.spec.Keys
		if q, seen := part[k]; seen && q != p.p {
			split = append(split, fmt.Sprintf("key-%02d on partitions %d and %d", k, q, p.p))
			continue
		}
		part[k] = p.p
		if q, seen := last[k]; seen && p.o <= q.o {
			order = append(order, fmt.Sprintf("key-%02d seq %d at offset %d after offset %d", k, seq, p.o, q.o))
		}
		last[k] = p
	}
	c.list("keys spread over more than one partition", split)
	c.list("keys stored out of send order", order)
}

// log: the fresh reader found exactly the acknowledged records, once each, where acknowledged,
// with the right content and a producer create time inside the leg, in a topic with the
// asked-for partition count. Returns where each record is stored.
func (c *checker) log(v View, acks map[int]pos) map[int]pos {
	stored := map[int]pos{}
	if v.ReadError != "" {
		c.fail("fresh reader could not read the topic: %s", v.ReadError)
		return stored
	}
	if c.spec.Partitions > 0 && v.Partitions != c.spec.Partitions {
		c.fail("topic has %d partitions, %d were asked for", v.Partitions, c.spec.Partitions)
	}
	var dup, stray, moved, content, stamps []string
	for _, r := range v.Records {
		seq := c.spec.SeqOf(r.Value)
		if seq < 0 || seq >= c.spec.total() {
			stray = append(stray, fmt.Sprintf("partition %d offset %d", r.Partition, r.Offset))
			continue
		}
		if _, seen := stored[seq]; seen {
			dup = append(dup, fmt.Sprintf("seq %d again at partition %d offset %d", seq, r.Partition, r.Offset))
			continue
		}
		stored[seq] = pos{r.Partition, r.Offset}
		if a, ok := acks[seq]; !ok {
			stray = append(stray, fmt.Sprintf("seq %d (never acknowledged)", seq))
		} else if a != unknownPos && a != (pos{r.Partition, r.Offset}) {
			moved = append(moved, fmt.Sprintf("seq %d acknowledged at %d/%d, stored at %d/%d", seq, a.p, a.o, r.Partition, r.Offset))
		}
		if d := c.contentDiff(seq, r); d != "" {
			content = append(content, d)
		}
		if v.LegStartMS > 0 && (!r.CreateTS || r.Timestamp < v.LegStartMS-timestampSlack || r.Timestamp > v.LegEndMS+timestampSlack) {
			kind := "create time"
			if !r.CreateTS {
				kind = "log-append time"
			}
			stamps = append(stamps, fmt.Sprintf("seq %d stored with %s %d, leg ran %d..%d", seq, kind, r.Timestamp, v.LegStartMS, v.LegEndMS))
		}
	}
	var lost []string
	for seq := range acks {
		if _, ok := stored[seq]; !ok {
			lost = append(lost, strconv.Itoa(seq))
		}
	}
	sort.Slice(lost, func(i, j int) bool { a, _ := strconv.Atoi(lost[i]); b, _ := strconv.Atoi(lost[j]); return a < b })
	c.list("acknowledged records missing from the log", lost)
	c.list("records stored twice in the log", dup)
	c.list("records in the log that were never acknowledged", stray)
	c.list("records stored at another position than acknowledged", moved)
	c.list("records stored with wrong content", content)
	c.list("records stored without the producer's create time inside the run", stamps)
	return stored
}

func (c *checker) contentDiff(seq int, r LogRecord) string {
	want := c.spec.Expected(seq)
	if !sameStr(want.Key, r.Key) {
		return fmt.Sprintf("seq %d key %s, expected %s", seq, show(r.Key), show(want.Key))
	}
	if !sameStr(want.Value, r.Value) {
		return fmt.Sprintf("seq %d value %s, expected %s", seq, show(r.Value), show(want.Value))
	}
	if fmt.Sprint(want.Headers) != fmt.Sprint(r.Headers) {
		return fmt.Sprintf("seq %d headers %v, expected %v", seq, r.Headers, want.Headers)
	}
	return ""
}

// consumePhase: the phase delivered exactly its records, once each, in offset order within a
// partition, where acknowledged, with the right content, only from assigned partitions.
// Returns how many distinct records of the phase were delivered.
func (c *checker) consumePhase(ph int, evs []Event, acks map[int]pos) int {
	lo, hi := 0, c.spec.Records
	if ph == 2 {
		lo, hi = c.spec.Records, c.spec.total()
	}
	assigned := map[int32]bool{}
	sawAssign := false
	got := map[int]bool{}
	lastOff := map[int32]int64{}
	var dup, foreign, early, reorder, moved, content, unassigned []string
	for _, e := range evs {
		if e.Phase != ph {
			continue
		}
		switch e.Ev {
		case "assigned":
			sawAssign = true
			for _, p := range e.Partitions {
				assigned[p] = true
			}
		case "recv":
			seq := c.spec.SeqOf(e.Value)
			r := LogRecord{Partition: e.Partition, Offset: e.Offset, Key: e.Key, Value: e.Value, Headers: e.Headers}
			if seq < 0 || seq >= c.spec.total() {
				foreign = append(foreign, fmt.Sprintf("partition %d offset %d", e.Partition, e.Offset))
				continue
			}
			if !assigned[e.Partition] && !c.hiddenAssign {
				unassigned = append(unassigned, fmt.Sprintf("seq %d from partition %d", seq, e.Partition))
			}
			if last, ok := lastOff[e.Partition]; ok && e.Offset <= last {
				reorder = append(reorder, fmt.Sprintf("partition %d offset %d after offset %d", e.Partition, e.Offset, last))
			}
			lastOff[e.Partition] = e.Offset
			if seq < lo || seq >= hi {
				early = append(early, strconv.Itoa(seq))
				continue
			}
			if got[seq] {
				dup = append(dup, strconv.Itoa(seq))
				continue
			}
			got[seq] = true
			if a, ok := acks[seq]; ok && a != unknownPos && a != (pos{e.Partition, e.Offset}) {
				moved = append(moved, fmt.Sprintf("seq %d acknowledged at %d/%d, delivered as %d/%d", seq, a.p, a.o, e.Partition, e.Offset))
			}
			if d := c.contentDiff(seq, r); d != "" {
				content = append(content, d)
			}
		}
	}
	name := fmt.Sprintf("consume phase %d", ph)
	if !sawAssign && !c.hiddenAssign {
		c.fail("%s: the consumer reported no partition assignment", name)
		unassigned = nil
	}
	var lost []string
	for seq := lo; seq < hi; seq++ {
		if !got[seq] {
			lost = append(lost, strconv.Itoa(seq))
		}
	}
	c.list(name+": records never delivered", lost)
	c.list(name+": records delivered twice", dup)
	c.list(name+": records of another phase delivered (wrong start offset)", early)
	c.list(name+": records that were never produced", foreign)
	c.list(name+": records delivered out of offset order", reorder)
	c.list(name+": records delivered at another position than acknowledged", moved)
	c.list(name+": records delivered with wrong content", content)
	c.list(name+": records from partitions not assigned to the member", unassigned)
	return len(got)
}

// commits: in scenario and verifiable mode every explicit commit succeeded and the group's committed offsets
// equal the high watermarks. Returns whether they do (both modes, as an observation).
func (c *checker) commits(evs []Event, v View) bool {
	if c.spec.explicitCommits() && !c.spec.allows(LimitCommitOnClose) {
		per := map[int]int{}
		for _, e := range evs {
			if e.Ev != "commit" {
				continue
			}
			per[e.Phase]++
			if e.ErrorCode != 0 || e.Error != "" {
				c.fail("consume phase %d: commit failed: error code %d %s", e.Phase, e.ErrorCode, e.Error)
			}
		}
		for _, ph := range c.phases() {
			if per[ph] == 0 {
				c.fail("consume phase %d: no commit reported", ph)
			}
		}
	}
	if v.ReadError != "" {
		return false
	}
	atEnd := len(v.HighWatermarks) > 0
	var behind []string
	parts := make([]int32, 0, len(v.HighWatermarks))
	for p := range v.HighWatermarks {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
	for _, p := range parts {
		hw := v.HighWatermarks[p]
		got, ok := v.Committed[p]
		if hw == 0 && !ok {
			continue // nothing to commit on an empty partition
		}
		if !ok || got != hw {
			atEnd = false
			cm := "none"
			if ok {
				cm = strconv.FormatInt(got, 10)
			}
			behind = append(behind, fmt.Sprintf("partition %d committed %s, high watermark %d", p, cm, hw))
		}
	}
	if c.spec.explicitCommits() {
		c.list("committed offsets differ from the log end", behind)
	}
	return atEnd
}

// list adds one problem naming up to maxListed items.
func (c *checker) list(what string, items []string) {
	if len(items) == 0 {
		return
	}
	shown := items
	more := ""
	if len(shown) > maxListed {
		shown = shown[:maxListed]
		more = fmt.Sprintf(" and %d more", len(items)-maxListed)
	}
	c.fail("%s: %d (%s%s)", what, len(items), strings.Join(shown, "; "), more)
}

func sameStr(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func show(s *string) string {
	if s == nil {
		return "null"
	}
	return strconv.Quote(*s)
}
