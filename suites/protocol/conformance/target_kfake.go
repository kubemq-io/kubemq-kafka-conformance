package conformance

// target_kfake.go — the in-process franz-go/pkg/kfake Target, the second oracle.
//
// kfake is a SEPARATE Go module (github.com/twmb/franz-go/pkg/kfake, no semver tags — pinned by
// pseudo-version). It boots a real (in-memory) Kafka-protocol broker inside THIS test
// process — no container, no network hop — which is what makes the primary diff (kgo vs
// real-Kafka === kgo vs kfake) fast enough to run without podman for the inner dev loop.
//
// 1-partition defaults: the core scenario pins 1-partition topics (kills
// murmur2-vs-crc32 partition-assignment divergence and gives total order — see
// scenario_core.go), so this Target configures kfake's DefaultNumPartitions(1) to match.
//
// Port: kfake's listener is drawn from the same KUBEMQ_INPROC_PORT_RANGE pool as the in-process
// KubeMQ target (target_kubemq.go), so an in-process run keeps every listener it opens inside one
// declared block instead of an OS-assigned ephemeral port.

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/franz-go/pkg/kfake"
)

// KfakeTarget is a Target backed by an in-process franz-go/pkg/kfake cluster.
type KfakeTarget struct {
	cluster *kfake.Cluster
	addr    string
}

// NewKfakeTarget starts a fresh, single-broker, single-partition-by-default in-process kfake
// cluster and returns a Target for it. The cluster is stopped by Close.
func NewKfakeTarget() (*KfakeTarget, error) {
	// kfake binds an ephemeral loopback port of its own choosing; this repository's runner
	// owns no port range for it (the kubemq-server rig's port pool is not ported).
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.DefaultNumPartitions(1),
		kfake.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafkaconformance: start in-process kfake cluster: %w", err)
	}

	addrs := cluster.ListenAddrs()
	if len(addrs) == 0 {
		cluster.Close()
		return nil, fmt.Errorf("kafkaconformance: kfake cluster started with no listen addresses")
	}

	return &KfakeTarget{cluster: cluster, addr: addrs[0]}, nil
}

// BootstrapAddr implements Target.
func (k *KfakeTarget) BootstrapAddr() string { return k.addr }

// Name implements Target.
func (k *KfakeTarget) Name() string { return "kfake" }

// Close implements Target. Unlike KafkaTarget (which never owns the broker process), KfakeTarget
// DOES own the in-process cluster it started in NewKfakeTarget and must tear it down here — kfake
// is not externally managed infrastructure.
func (k *KfakeTarget) Close() error {
	if k.cluster != nil {
		k.cluster.Close()
	}
	return nil
}

// InjectProduceError makes this kfake broker answer the next Produce request with code on
// every partition. Test hook for the verifier mutation tests: it simulates a broker under
// test returning a wrong error code on the wire.
func (k *KfakeTarget) InjectProduceError(code int16) {
	k.cluster.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ProduceRequest)
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
}
