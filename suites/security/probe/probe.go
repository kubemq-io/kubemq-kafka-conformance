// Package probe holds the security track's probes (Track E). A probe runs one scenario against
// one broker's secured listeners and returns an Observation of deterministic facts (error codes,
// accepted or refused, counts), never timings or broker-assigned identifiers. The track runs it
// on the Kafka oracle first, then on KubeMQ with only the addresses changed, and compares the two
// observations with the features track's field-by-field Diff.
package probe

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	fprobe "github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features/probe"
	"github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security/tlsca"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Observation is what a probe saw (the features track's type, so Diff and gap binding are shared).
type Observation = fprobe.Observation

// Env is one broker's side of a posture. Plain and TLS are comma-separated host:port lists (one
// entry for a single node, one per broker on a cluster); clients bootstrap from the whole list,
// single-connection probes use the first entry. TLS may be empty where a deployment has no TLS
// port for the posture; rows that need it then fail on both legs alike.
type Env struct {
	Target string // "real-kafka" or "kubemq", for topic names only
	Plain  string // plain client port(s) (PLAINTEXT or SASL_PLAINTEXT)
	TLS    string // TLS client port(s) (SSL or SASL_SSL)
	Certs  *tlsca.Material
	// For container client rows: the broker's container (clients join its network namespace),
	// its plain port as seen from inside it, and the posture directory (mounted at /posture).
	Container        string
	PlainInContainer string
	PostureDir       string
}

// Probe runs one scenario. An error means it could not complete (oracle: VOID; KubeMQ: FAIL);
// a behaviour difference must be an Observation field.
type Probe func(ctx context.Context, env Env) (Observation, error)

// Registry maps matrix probe names to probes.
var Registry = map[string]Probe{}

func register(name string, p Probe) { Registry[name] = p }

func topicName(env Env, probe string) string {
	return fmt.Sprintf("kcert-sec-%s-%s-%d", probe, env.Target, time.Now().UnixNano())
}

// tlsRecorder is a kgo dialer that speaks TLS and remembers the protocol version of every
// connection it made.
type tlsRecorder struct {
	cfg      *tls.Config
	mu       sync.Mutex
	versions map[string]bool
}

func newTLSRecorder(cfg *tls.Config) *tlsRecorder {
	return &tlsRecorder{cfg: cfg, versions: map[string]bool{}}
}

func (r *tlsRecorder) dial(ctx context.Context, network, host string) (net.Conn, error) {
	cfg := r.cfg.Clone()
	if cfg.ServerName == "" {
		h, _, err := net.SplitHostPort(host)
		if err != nil {
			return nil, err
		}
		cfg.ServerName = h
	}
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: cfg}
	c, err := d.DialContext(ctx, network, host)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.versions[tls.VersionName(c.(*tls.Conn).ConnectionState().Version)] = true
	r.mu.Unlock()
	return c, nil
}

func (r *tlsRecorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for v := range r.versions {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// Outcome classifies how an attempt ended, in words both brokers can be compared on.
func Outcome(err error) string {
	switch {
	case err == nil:
		return "accepted"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return "timeout"
	case isTLSAlert(err):
		return "tls-refused"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE), errors.Is(err, net.ErrClosed):
		return "closed"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	// TLS handshakes that fail on the client side (version, certificate) all say "tls:".
	if strings.Contains(err.Error(), "tls:") {
		return "tls-refused"
	}
	return "error"
}

func isTLSAlert(err error) bool {
	var ae tls.AlertError
	if errors.As(err, &ae) {
		return true
	}
	var re *tls.RecordHeaderError
	return errors.As(err, &re)
}

// kgoTLS is a franz-go client on env.TLS presenting client cert name ("" for none).
func kgoTLS(env Env, client string, rec **tlsRecorder, opts ...kgo.Opt) (*kgo.Client, error) {
	cfg, err := env.Certs.ClientConfig(client)
	if err != nil {
		return nil, err
	}
	r := newTLSRecorder(cfg)
	if rec != nil {
		*rec = r
	}
	base := []kgo.Opt{kgo.SeedBrokers(Seeds(env.TLS)...), kgo.Dialer(r.dial), kgo.FetchMaxWait(500 * time.Millisecond)}
	return kgo.NewClient(append(base, opts...)...)
}

// Seeds splits a comma-separated host:port list.
func Seeds(list string) []string {
	var out []string
	for _, a := range strings.Split(list, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// First is the first entry of a host:port list ("" if empty).
func First(list string) string {
	if s := Seeds(list); len(s) > 0 {
		return s[0]
	}
	return ""
}
