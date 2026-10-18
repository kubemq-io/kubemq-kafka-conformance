// Command migration-track runs the rows of matrix/migration.yaml (Track G, docs/migration.md):
// the cutover procedure from Apache Kafka and the rollback, as evidence.
//
// One leg per broker under test, oracle first. A leg starts a fresh source Kafka on a private
// network, fills it (an orders topic with consumer groups committed mid-log, a compacted topic
// with tombstones, a topic with a committed and an aborted transaction, a topic carrying
// retention.bytes), mirrors it into the broker under test with MirrorMaker 2 (identity topic
// names, as a real cutover uses), keeps producing while the mirror runs, cuts over, and resumes
// consumers on the broker under test. Then the broker under test takes writes, a reverse mirror
// copies it to a fresh rollback Kafka, and a consumer resumes there.
//
// Containers and MirrorMaker 2 come from the tools track's shared packages (suites/tools/box,
// suites/tools/mirror); the client steps run migration-agent inside a container on the network.
//
//	PASS     the KubeMQ leg's observation equals the oracle's
//	GAP      it differs exactly as pre-registered and signed off in docs/gaps.md
//	FAIL     it differs otherwise, or the KubeMQ leg could not produce the row's evidence
//	VOID     the oracle leg could not, or its observation is not a correct migration
//	NOT-RUN  excluded by -only
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/migration/verify"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol/gaps"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/box"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/kout"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/tools/mirror"
)

type matrixRow struct {
	ID         string `yaml:"id"`
	Group      string `yaml:"group"`
	Title      string `yaml:"title"`
	Provenance string `yaml:"provenance"`
	Source     string `yaml:"source"`
}

type Leg struct {
	OK          bool           `json:"ok"`
	Error       string         `json:"error,omitempty"`
	Observation map[string]any `json:"observation,omitempty"`
	Notes       []string       `json:"notes,omitempty"`
}

type Row struct {
	ID         string `json:"id"`
	Kind       string `json:"kind"`
	Title      string `json:"title"`
	Verdict    string `json:"verdict"`
	Provenance string `json:"provenance"`
	Source     string `json:"source"`
	Detail     string `json:"detail,omitempty"`
	Deviation  string `json:"deviation,omitempty"`
	Oracle     *Leg   `json:"oracle,omitempty"`
	KubeMQ     *Leg   `json:"kubemq,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

type Results struct {
	Schema  int               `json:"schema"`
	Track   string            `json:"track"`
	Rows    []Row             `json:"rows"`
	Clients map[string]string `json:"clients"`
}

// Row ids, as in matrix/migration.yaml.
const (
	rowMidLog      = "migration.offsets.mid-log"
	rowResumeFranz = "migration.cutover.resume.franz-go"
	rowResumeJava  = "migration.cutover.resume.kafka-clients"
	rowCompacted   = "migration.mm2.compacted-fidelity"
	rowTxn         = "migration.mm2.transactional-fidelity"
	rowConfig      = "migration.mm2.config-refusal"
	rowRollback    = "migration.rollback.end-to-end"
)

// Workload sizes.
const (
	parts          = 3
	firstBatch     = 300 // orders before the mirror starts
	liveBatch      = 150 // orders produced while the mirror runs
	afterCutover   = 60  // orders the broker under test takes before the rollback
	txnCommitted   = 40
	txnAborted     = 20
	compactRecords = 200
	retBytesValue  = "1073741824"
)

// good is the oracle precondition per row: a migration the oracle itself does not get right is
// a broken scenario (VOID), not a reference.
var good = map[string]func(map[string]any) bool{
	rowMidLog:      func(o map[string]any) bool { return o["never_ahead"] == true && o["some_translated"] == true },
	rowResumeFranz: resumeOK,
	rowResumeJava:  resumeOK,
	rowCompacted: func(o map[string]any) bool {
		return o["same_latest_values"] == true && o["deleted_keys_deleted"] == true
	},
	rowTxn:      func(o map[string]any) bool { return o["committed_records_on_target"] == txnCommitted },
	rowConfig:   func(o map[string]any) bool { return o["mirror_completed"] == true },
	rowRollback: func(o map[string]any) bool { return resumeOK(o) && o["records_identical"] == true },
}

func resumeOK(o map[string]any) bool {
	for _, k := range []string{"some_translated", "never_ahead", "started_at_commit", "nothing_skipped", "no_duplicates", "in_order"} {
		if o[k] != true {
			return false
		}
	}
	return true
}

func main() {
	matrixPath := flag.String("matrix", "", "matrix/migration.yaml")
	oracleAddr := flag.String("oracle", os.Getenv("ORACLE_BOOTSTRAP"), "Kafka oracle bootstrap address (as containers reach it)")
	kubemqAddr := flag.String("kubemq", os.Getenv("KUBEMQ_BOOTSTRAP"), "KubeMQ bootstrap address (as containers reach it)")
	agent := flag.String("agent", "", "migration-agent binary for the container architecture")
	ctrPath := flag.String("ctr", "", "suites/tools/ctr.sh")
	out := flag.String("out", os.Getenv("TRACK_RESULTS"), "results JSON to write")
	gapsPath := flag.String("gaps", "", "docs/gaps.md")
	only := flag.String("only", "", "comma-separated row ids to run; the rest are NOT-RUN")
	legTimeout := flag.Duration("leg-timeout", 15*time.Minute, "bound on one leg")
	flag.Parse()
	if *matrixPath == "" || *out == "" || *agent == "" || *ctrPath == "" {
		fatal("need -matrix, -out, -agent and -ctr")
	}
	box.Ctr = *ctrPath
	rows, err := loadMatrix(*matrixPath)
	if err != nil {
		fatal(err.Error())
	}
	reg, err := gaps.Load(*gapsPath)
	if err != nil {
		fatal("gap register: " + err.Error())
	}
	runDate := time.Now().UTC()
	if v := os.Getenv("CERT_RUN_STARTED"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			runDate = t
		}
	}
	onlySet := map[string]bool{}
	for _, id := range strings.Split(*only, ",") {
		if id = strings.TrimSpace(id); id != "" {
			onlySet[id] = true
		}
	}
	start := time.Now()
	oracle := runLeg("o", *oracleAddr, *agent, *legTimeout)
	kubemq := runLeg("k", *kubemqAddr, *agent, *legTimeout)
	elapsed := time.Since(start).Milliseconds()

	res := Results{Schema: 1, Track: "migration", Clients: map[string]string{"franz-go": "v1.22.0", "kafka-clients": os.Getenv("KAFKA_VERSION")}}
	for _, r := range rows {
		row := Row{ID: r.ID, Kind: "behaviour", Title: r.Title, Provenance: r.Provenance, Source: r.Source, DurationMS: elapsed}
		if len(onlySet) > 0 && !onlySet[r.ID] {
			row.Verdict, row.Detail = "NOT-RUN", "excluded by -only"
		} else {
			judge(&row, r.ID, oracle.leg(r.ID), kubemq.leg(r.ID), reg, runDate)
		}
		logf("%-44s %-7s %s", row.ID, row.Verdict, row.Detail)
		res.Rows = append(res.Rows, row)
	}
	if err := writeJSON(*out, res); err != nil {
		fatal(err.Error())
	}
}

// judge maps two legs to a verdict. Pure, for the tests.
func judge(row *Row, base string, o, k *Leg, reg map[string]gaps.Entry, runDate time.Time) {
	row.Oracle, row.KubeMQ = o, k
	if !o.OK {
		row.Verdict, row.Detail = "VOID", "oracle leg failed: "+o.Error
		return
	}
	if g := good[base]; g != nil && !g(o.Observation) {
		row.Verdict, row.Detail = "VOID", "oracle precondition failed: Kafka did not migrate correctly: "+compact(o.Observation)+" "+strings.Join(o.Notes, "; ")
		return
	}
	if !k.OK {
		row.Verdict, row.Detail = "FAIL", "KubeMQ leg failed: "+k.Error
		return
	}
	dev, same := verify.Diff(o.Observation, k.Observation)
	if same {
		row.Verdict, row.Detail = "PASS", "observations identical: "+compact(o.Observation)
		return
	}
	row.Deviation = dev
	why := strings.Join(k.Notes, "; ")
	if ok, reason := gaps.Registered(reg, row.ID, dev, runDate); ok {
		row.Verdict, row.Detail = "GAP", dev+". "+reason+". "+why
	} else {
		row.Verdict, row.Detail = "FAIL", dev+". "+reason+". "+why
	}
}

// legResult collects one leg's per-row evidence. A step that fails marks every row that had
// not got its evidence yet.
type legResult struct {
	rows map[string]*Leg
	err  error
}

func (l *legResult) leg(id string) *Leg {
	if r, ok := l.rows[id]; ok {
		return r
	}
	msg := "the leg stopped before this row's evidence"
	if l.err != nil {
		msg += ": " + l.err.Error()
	}
	return &Leg{Error: msg}
}

func (l *legResult) set(id string, obs map[string]any, notes ...string) {
	l.rows[id] = &Leg{OK: true, Observation: obs, Notes: notes}
}

func (l *legResult) fail(id string, err error) {
	l.rows[id] = &Leg{Error: err.Error()}
}

// leg is one migration scenario against one broker under test.
type leg struct {
	ctx      context.Context
	tag      string // "o" or "k"
	prefix   string // container names
	id       string // topic and group names
	dir      string // host work dir (MirrorMaker 2 properties)
	logs     string
	agentBox *box.Box
	src, tgt mirror.Cluster
	stops    []func()
}

func runLeg(tag, bootstrap, agentBin string, timeout time.Duration) *legResult {
	res := &legResult{rows: map[string]*Leg{}}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	label := strings.ToLower(os.Getenv("TOOLS_RUN_LABEL"))
	if label == "" {
		label = "manual"
	}
	l := &leg{ctx: ctx, tag: tag, prefix: fmt.Sprintf("kcert-mig-%s-%s", label, tag),
		id: fmt.Sprintf("mig-%s%d", tag, time.Now().Unix()%100000), logs: filepath.Join(os.Getenv("CERT_RUN_DIR"), "logs")}
	defer func() {
		for i := len(l.stops) - 1; i >= 0; i-- {
			l.stops[i]()
		}
	}()
	res.err = l.run(bootstrap, agentBin, res)
	if res.err != nil {
		logf("leg %s stopped: %v", tag, res.err)
	}
	return res
}

func (l *leg) topic(name string) string { return l.id + "-" + name }
func (l *leg) group(name string) string { return l.id + "-g" + name }

func (l *leg) run(bootstrap, agentBin string, res *legResult) error {
	var err error
	if l.dir, err = os.MkdirTemp(os.Getenv("CERT_RUN_DIR"), "migration-"+l.tag+"-"); err != nil {
		return err
	}
	// MirrorMaker 2 runs as the image's own user and reads its properties from this directory.
	if err := os.Chmod(l.dir, 0o755); err != nil {
		return err
	}
	net := l.prefix + "-net"
	rmNet, err := box.Network(l.ctx, net)
	if err != nil {
		return err
	}
	l.stops = append(l.stops, rmNet)
	src, stopSrc, err := mirror.StartKafka(l.ctx, l.prefix+"-src", net, filepath.Join(l.logs, "migration-"+l.tag+"-source-kafka.log"))
	if err != nil {
		return fmt.Errorf("source Kafka: %w", err)
	}
	l.stops = append(l.stops, stopSrc)
	opts := append(box.JavaEnv(), "--network", net, "-v", agentBin+":/agent:ro", "--entrypoint", "sleep")
	if l.agentBox, err = box.Start(l.ctx, l.prefix+"-agent", opts, box.Image(), "86400"); err != nil {
		return fmt.Errorf("agent container: %w", err)
	}
	l.stops = append(l.stops, l.agentBox.Remove)
	// The source's own tools run in its container; the broker under test's in the agent box.
	l.src = src
	l.tgt = mirror.Cluster{Box: l.agentBox, Bootstrap: bootstrap, Advertise: bootstrap}

	// --- fill the source --------------------------------------------------------------------
	S := src.Advertise
	orders, compactT, txnT, retT := l.topic("orders"), l.topic("compact"), l.topic("txn"), l.topic("retbytes")
	gmid, gjava := l.group("mid"), l.group("java")
	steps := [][]string{
		{"produce", "--bootstrap", S, "--topic", orders, "--partitions", "3", "--records", strconv.Itoa(firstBatch)},
		{"produce", "--bootstrap", S, "--topic", compactT, "--partitions", "2", "--records", strconv.Itoa(compactRecords), "--compacted"},
		{"tombstone", "--bootstrap", S, "--topic", compactT, "--partitions", "2", "--keys", "key-01,key-02"},
		{"txn", "--bootstrap", S, "--topic", txnT, "--partitions", "2", "--committed", strconv.Itoa(txnCommitted), "--aborted", strconv.Itoa(txnAborted)},
		{"produce", "--bootstrap", S, "--topic", retT, "--partitions", "3", "--records", "30", "--config", "retention.bytes=" + retBytesValue},
		{"commit", "--bootstrap", S, "--group", gmid, "--topic", orders, "--offsets", "0=37,1=100,2=5"},
		{"commit", "--bootstrap", S, "--group", gjava, "--topic", orders, "--offsets", "0=50,1=20,2=99"},
	}
	for _, s := range steps {
		if _, err := l.agent(s...); err != nil {
			return fmt.Errorf("filling the source: %w", err)
		}
	}

	// --- mirror while records keep arriving -------------------------------------------------
	// The retention.bytes topic gets its own flow below: KubeMQ refuses its creation, and a flow
	// that cannot create one of its topics can stall for all of them (observed), which would
	// spoil the cutover rows.
	stopMM, err := l.mirror("fwd", "src", "tgt", src, l.tgt, "^"+regexp.QuoteMeta(l.id)+"-(orders|compact|txn)$", "^"+regexp.QuoteMeta(l.id)+"-g.*", net)
	if err != nil {
		return err
	}
	if !mirror.WaitRemote(l.ctx, l.tgt, orders, firstBatch, 3*time.Minute) {
		stopMM()
		return fmt.Errorf("MirrorMaker 2 did not copy %d records of %s within 3 minutes", firstBatch, orders)
	}
	if _, err := l.agent("produce", "--bootstrap", S, "--topic", orders, "--partitions", "3", "--from", strconv.Itoa(firstBatch), "--records", strconv.Itoa(liveBatch)); err != nil {
		stopMM()
		return fmt.Errorf("live produce: %w", err)
	}
	if _, err := l.agent("commit", "--bootstrap", S, "--group", gmid, "--topic", orders, "--offsets", "0=120,1=140,2=60"); err != nil {
		stopMM()
		return fmt.Errorf("mid-log commit: %w", err)
	}
	total := int64(firstBatch + liveBatch)
	if !mirror.WaitRemote(l.ctx, l.tgt, orders, total, 3*time.Minute) {
		stopMM()
		return fmt.Errorf("MirrorMaker 2 did not copy the live records of %s", orders)
	}
	// Topics other than orders: wait until their mirror is complete (or, for the config row,
	// give it the same time).
	mirror.WaitRemote(l.ctx, l.tgt, compactT, compactRecords+2, time.Minute)
	mirror.WaitRemote(l.ctx, l.tgt, txnT, 1, time.Minute)
	// Cutover, one group at a time: as soon as a group's offsets are translated its consumer
	// resumes on the target. The kit's brokers expire an empty group's offsets after one minute
	// (compose files, offset-retention row), so nothing may wait between translation and resume.
	srcEnd, err := l.offsets(S, gmid, orders)
	if err != nil {
		stopMM()
		return err
	}
	srcJava, err := l.offsets(S, gjava, orders)
	if err != nil {
		stopMM()
		return err
	}
	parts := func(srcCommit map[int32]int64, translated map[int]int64, end map[int32]int64, delivered map[int32][]int64) map[int32]verify.Partition {
		out := map[int32]verify.Partition{}
		for p, sc := range srcCommit {
			t := int64(-1)
			if v, ok := translated[int(p)]; ok {
				t = v
			}
			out[p] = verify.Partition{SourceCommitted: sc, Translated: t, End: end[p], Delivered: delivered[p]}
		}
		return out
	}

	midT := l.waitTranslated(gmid, orders, map[int]int64{0: 120, 1: 140, 2: 60})
	tgtEnd, err := l.offsets(l.tgt.Bootstrap, gmid, orders)
	if err != nil {
		stopMM()
		return err
	}
	offsetsIdentical := fmt.Sprint(srcEnd.End) == fmt.Sprint(tgtEnd.End)
	mid := verify.CheckResume(parts(srcEnd.Committed, midT, tgtEnd.End, nil))
	res.set(rowMidLog, map[string]any{"never_ahead": mid.NeverAhead, "some_translated": mid.Untranslated < mid.Partitions},
		fmt.Sprintf("source commits %v, translated %v (%d partitions untranslated), re-reads %d", srcEnd.Committed, midT, mid.Untranslated, mid.Rereads))
	javaT := l.waitTranslated(gjava, orders, map[int]int64{0: 50, 1: 20, 2: 99})
	// Both groups resume at once, right after translation: the kit brokers expire an empty
	// group's offsets after one minute. The committed offsets each consumer finds are recorded,
	// so an expiry shows in the evidence instead of looking like a wrong resume.
	before, _ := l.offsets(l.tgt.Bootstrap, gjava, orders)
	type franzOut struct {
		recs []verify.Rec
		err  error
	}
	franzCh := make(chan franzOut, 1)
	go func() {
		recs, err := l.consume(l.tgt.Bootstrap, gmid, orders)
		franzCh <- franzOut{recs, err}
	}()
	delivered, javaErr := l.consoleResume(gjava, orders)
	f := <-franzCh
	if f.err != nil {
		res.fail(rowResumeFranz, f.err)
	} else {
		r := verify.CheckResume(parts(srcEnd.Committed, midT, tgtEnd.End, verify.Delivered(f.recs)))
		obs := r.Observation()
		obs["offsets_identical"] = offsetsIdentical
		res.set(rowResumeFranz, obs, append(r.Problems, fmt.Sprintf("translated %v, re-reads %d", midT, r.Rereads))...)
	}
	if javaErr != nil {
		res.fail(rowResumeJava, javaErr)
	} else {
		r := verify.CheckResume(parts(srcJava.Committed, javaT, tgtEnd.End, delivered))
		obs := r.Observation()
		obs["offsets_identical"] = offsetsIdentical
		res.set(rowResumeJava, obs, append(r.Problems, fmt.Sprintf("translated %v, committed on the target just before the consumer joined %v, re-reads %d", javaT, before.Committed, r.Rereads))...)
	}
	stopMM()

	// --- rows: topic fidelity ---------------------------------------------------------------
	if s, err := l.dump(S, compactT, false); err != nil {
		res.fail(rowCompacted, fmt.Errorf("dump source: %w", err))
	} else if t, err := l.dump(l.tgt.Bootstrap, compactT, false); err != nil {
		res.fail(rowCompacted, fmt.Errorf("dump target: %w", err))
	} else {
		c := verify.CompareCompacted(s, t, []string{"key-01", "key-02"})
		obs := c.Observation()
		obs["target_cleanup_policy"] = l.tgt.CleanupPolicy(l.ctx, compactT)
		res.set(rowCompacted, obs, c.Problems...)
	}
	if t, err := l.dump(l.tgt.Bootstrap, txnT, false); err != nil {
		res.fail(rowTxn, fmt.Errorf("dump target: %w", err))
	} else {
		committed, aborted := 0, 0
		for _, r := range t {
			seq, err := strconv.Atoi(r.Headers["seq"])
			switch {
			case err != nil:
			case seq < txnCommitted:
				committed++
			default:
				aborted++
			}
		}
		res.set(rowTxn, map[string]any{"committed_records_on_target": committed, "aborted_records_on_target": aborted})
	}
	// --- row: a topic KubeMQ refuses to create with its source configs, in its own flow --------
	l.configRow(net, retT, res)

	// --- row: rollback ----------------------------------------------------------------------
	if err := l.rollback(net, gmid, orders, total, res); err != nil {
		res.fail(rowRollback, err)
	}
	return nil
}

func (l *leg) rollback(net, gmid, orders string, mirrored int64, res *legResult) error {
	T := l.tgt.Bootstrap
	if _, err := l.agent("produce", "--bootstrap", T, "--topic", orders, "--partitions", "3", "--from", strconv.Itoa(int(mirrored)), "--records", strconv.Itoa(afterCutover)); err != nil {
		return fmt.Errorf("writes after cutover: %w", err)
	}
	rb, stopRB, err := mirror.StartKafka(l.ctx, l.prefix+"-rb", net, filepath.Join(l.logs, "migration-"+l.tag+"-rollback-kafka.log"))
	if err != nil {
		return fmt.Errorf("rollback Kafka: %w", err)
	}
	l.stops = append(l.stops, stopRB)
	stopMM, err := l.mirror("rev", "tgt", "rb", l.tgt, rb, "^"+regexp.QuoteMeta(orders)+"$", "^"+regexp.QuoteMeta(gmid)+"$", net)
	if err != nil {
		return err
	}
	defer stopMM()
	total := mirrored + afterCutover
	if !mirror.WaitRemote(l.ctx, rb, orders, total, 3*time.Minute) {
		return fmt.Errorf("reverse MirrorMaker 2 did not copy %d records of %s to Kafka within 3 minutes", total, orders)
	}
	// The group, now consuming on the new cluster, commits mid-log (510 records, 170 per
	// partition); then the rollback decision: its offsets translate back and it resumes on Kafka.
	committed := map[int32]int64{0: 160, 1: 170, 2: 100}
	if _, err := l.agent("commit", "--bootstrap", T, "--group", gmid, "--topic", orders, "--offsets", "0=160,1=170,2=100"); err != nil {
		return fmt.Errorf("commit on the new cluster: %w", err)
	}
	translated := l.waitTranslatedOn(rb, gmid, orders, map[int]int64{0: 160, 1: 170, 2: 100})
	rbOff, err := l.offsets(rb.Advertise, gmid, orders)
	if err != nil {
		return err
	}
	recs, err := l.consume(rb.Advertise, gmid, orders)
	if err != nil {
		return fmt.Errorf("resume on Kafka: %w", err)
	}
	parts := map[int32]verify.Partition{}
	delivered := verify.Delivered(recs)
	for p, c := range committed {
		t := int64(-1)
		if v, ok := translated[int(p)]; ok {
			t = v
		}
		parts[p] = verify.Partition{SourceCommitted: c, Translated: t, End: rbOff.End[p], Delivered: delivered[p]}
	}
	r := verify.CheckResume(parts)
	obs := r.Observation()
	a, err1 := l.dump(T, orders, false)
	b, err2 := l.dump(rb.Advertise, orders, false)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("dump for identity: %v %v", err1, err2)
	}
	same, problems := verify.Identical(a, b)
	obs["records_identical"] = same
	res.set(rowRollback, obs, append(append(r.Problems, problems...), fmt.Sprintf("translated %v, re-reads %d", translated, r.Rereads))...)
	return nil
}

// configRow mirrors the retention.bytes topic alone and records what arrives: whether the flow
// completes, the partition count, the size bound, and whether MirrorMaker 2 had to give up on
// creating the topic.
func (l *leg) configRow(net, retT string, res *legResult) {
	logPath := filepath.Join(l.logs, "migration-"+l.tag+"-mm2-cfg.log")
	stop, err := l.mirror("cfg", "src", "tgt", l.src, l.tgt, "^"+regexp.QuoteMeta(retT)+"$", "^$", net)
	if err != nil {
		res.fail(rowConfig, err)
		return
	}
	completed := mirror.WaitRemote(l.ctx, l.tgt, retT, 30, 2*time.Minute)
	stop()
	created := l.tgt.Listed(l.ctx, retT)
	var mirrored int64
	partitions := 0
	if created {
		for _, v := range l.tgt.EndOffsets(l.ctx, retT) {
			mirrored += v
			partitions++
		}
	}
	retention := "absent"
	if created {
		if v, ok := kout.ParseConfigsDescribe(l.tgt.Tool(l.ctx, "", "kafka-configs.sh", "--describe", "--all",
			"--entity-type", "topics", "--entity-name", retT).Stdout)["retention.bytes"]; ok {
			retention = v
		}
	}
	refused := 0
	if b, err := os.ReadFile(logPath); err == nil {
		refused = strings.Count(string(b), "Could not create topic "+retT)
	}
	res.set(rowConfig, map[string]any{"mirror_completed": completed, "target_partitions": partitions, "target_retention_bytes": retention},
		fmt.Sprintf("source: 3 partitions, 30 records, retention.bytes %s; target holds %d records; MirrorMaker 2 logged %d failed creates", retBytesValue, mirrored, refused),
		"on KubeMQ the outcome varies between runs (2026-10-07, six runs on v3.6.16): the topic arrived without its size bound in four, never arrived in one, and in one (shared flow) stalled every topic of the flow; tracked as a server finding, not a gap")
}

// mirror starts one MirrorMaker 2 flow with identity topic names (a migration keeps names).
func (l *leg) mirror(name, srcAlias, tgtAlias string, src, tgt mirror.Cluster, topics, groups, net string) (func(), error) {
	_, stop, err := mirror.Start(l.ctx, mirror.Spec{Name: l.prefix + "-mm2-" + name, Network: net, ConfDir: l.dir,
		SrcAlias: srcAlias, TgtAlias: tgtAlias, Src: src, Tgt: tgt, Topics: topics, Groups: groups,
		Extra: map[string]string{"replication.policy.class": "org.apache.kafka.connect.mirror.IdentityReplicationPolicy"}},
		filepath.Join(l.logs, "migration-"+l.tag+"-mm2-"+name+".log"))
	if err != nil {
		return nil, fmt.Errorf("MirrorMaker 2 %s: %w", name, err)
	}
	return stop, nil
}

// waitTranslated waits until the group's translated offsets on the target equal want, or cover
// every partition and stop changing for 15 seconds (MirrorMaker 2 may translate below a mid-log
// commit), at most 2 minutes.
func (l *leg) waitTranslated(group, topic string, want map[int]int64) map[int]int64 {
	return l.waitTranslatedOn(l.tgt, group, topic, want)
}

func (l *leg) waitTranslatedOn(c mirror.Cluster, group, topic string, want map[int]int64) map[int]int64 {
	deadline := time.Now().Add(2 * time.Minute)
	var last map[int]int64
	stable := time.Now()
	for time.Now().Before(deadline) && l.ctx.Err() == nil {
		got := c.GroupOffsets(l.ctx, group, topic)
		if fmt.Sprint(got) == fmt.Sprint(want) {
			return got
		}
		if fmt.Sprint(got) != fmt.Sprint(last) {
			last, stable = got, time.Now()
		} else if len(got) >= len(want) && time.Since(stable) > 15*time.Second {
			return got
		}
		time.Sleep(3 * time.Second)
	}
	return last
}

func (l *leg) agent(args ...string) (string, error) {
	q := make([]string, len(args))
	for i, a := range args {
		q[i] = "'" + strings.ReplaceAll(a, "'", `'\''`) + "'"
	}
	r := l.agentBox.Sh(l.ctx, "", 3*time.Minute, "/agent "+strings.Join(q, " "))
	if r.Exit != 0 {
		return "", fmt.Errorf("agent %s: exit %d: %s", args[0], r.Exit, lastLine(r.Stderr))
	}
	return r.Stdout, nil
}

type offsets struct {
	Committed map[int32]int64 `json:"committed"`
	End       map[int32]int64 `json:"end"`
}

func (l *leg) offsets(bootstrap, group, topic string) (offsets, error) {
	var o offsets
	out, err := l.agent("offsets", "--bootstrap", bootstrap, "--group", group, "--topic", topic)
	if err == nil {
		err = json.Unmarshal([]byte(out), &o)
	}
	return o, err
}

func (l *leg) consume(bootstrap, group, topic string) ([]verify.Rec, error) {
	out, err := l.agent("consume", "--bootstrap", bootstrap, "--group", group, "--topic", topic, "--idle", "6s")
	if err != nil {
		return nil, err
	}
	var v struct {
		Records []verify.Rec `json:"records"`
	}
	return v.Records, json.Unmarshal([]byte(out), &v)
}

func (l *leg) dump(bootstrap, topic string, readCommitted bool) ([]verify.Rec, error) {
	args := []string{"dump", "--bootstrap", bootstrap, "--topic", topic}
	if readCommitted {
		args = append(args, "--read-committed")
	}
	out, err := l.agent(args...)
	if err != nil {
		return nil, err
	}
	var v struct {
		Records []verify.Rec `json:"records"`
	}
	return v.Records, json.Unmarshal([]byte(out), &v)
}

var consoleLine = regexp.MustCompile(`^Partition:(\d+)\s+Offset:(\d+)`)

// consoleResume runs Kafka's own console consumer (Java kafka-clients) as a member of group on
// the broker under test and returns the offsets it delivered per partition. It joins with the
// classic group protocol (the one allowed client deviation, docs/methodology.md) and, as the
// runbook requires of every migrated consumer, auto.offset.reset=earliest, so a partition
// MirrorMaker 2 did not translate is re-read rather than skipped.
func (l *leg) consoleResume(group, topic string) (map[int32][]int64, error) {
	r := l.tgt.Tool(l.ctx, "", "kafka-console-consumer.sh", "--topic", topic, "--group", group, "--timeout-ms", "15000",
		"--consumer-property", "group.protocol=classic", "--consumer-property", "enable.auto.commit=false",
		"--consumer-property", "auto.offset.reset=earliest",
		"--formatter-property", "print.partition=true", "--formatter-property", "print.offset=true")
	out := map[int32][]int64{}
	for _, line := range strings.Split(r.Stdout, "\n") {
		m := consoleLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		p, _ := strconv.Atoi(m[1])
		o, _ := strconv.ParseInt(m[2], 10, 64)
		out[int32(p)] = append(out[int32(p)], o)
	}
	if len(out) == 0 && !strings.Contains(r.Stderr, "Processed a total of") {
		return nil, fmt.Errorf("console consumer: exit %d: %s", r.Exit, lastLine(r.Stderr))
	}
	return out, nil
}

func loadMatrix(path string) ([]matrixRow, error) {
	var m struct {
		Track string      `yaml:"track"`
		Rows  []matrixRow `yaml:"rows"`
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if m.Track != "migration" || len(m.Rows) == 0 {
		return nil, fmt.Errorf("%s: want track migration with rows", path)
	}
	known := map[string]bool{}
	for id := range good {
		known[id] = true
	}
	seen := map[string]bool{}
	for _, r := range m.Rows {
		if !known[r.ID] || seen[r.ID] {
			return nil, fmt.Errorf("%s: row %q is unknown to this suite or duplicated", path, r.ID)
		}
		if r.Provenance != "upstream" && r.Provenance != "kubemq-authored" || r.Source == "" {
			return nil, fmt.Errorf("%s: row %s: provenance and source are mandatory", path, r.ID)
		}
		seen[r.ID] = true
	}
	return m.Rows, nil
}

func compact(o map[string]any) string {
	keys := make([]string, 0, len(o))
	for k := range o {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		b, _ := json.Marshal(o[k])
		parts[i] = k + "=" + string(b)
	}
	return strings.Join(parts, " ")
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}

func writeJSON(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func logf(format string, args ...any) { fmt.Fprintf(os.Stderr, "[migration] "+format+"\n", args...) }

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "[migration] ERROR:", msg)
	os.Exit(2)
}
