package probe

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() {
	register("groups-assignor-range", assignor(kgo.RangeBalancer(), "range"))
	register("groups-assignor-roundrobin", assignor(kgo.RoundRobinBalancer(), "roundrobin"))
	register("groups-assignor-sticky", assignor(kgo.StickyBalancer(), "sticky"))
	register("groups-assignor-cooperative-sticky", assignor(kgo.CooperativeStickyBalancer(), "cooperative-sticky"))
	register("groups-rebalance-join-leave-crash", rebalanceJoinLeaveCrash)
	register("groups-session-timeout-bounds", sessionTimeoutBounds)
	register("groups-rebalance-timeout-expiry", rebalanceTimeoutExpiry)
	register("groups-static-membership", staticMembership)
	register("groups-offset-reset-policies", offsetResetPolicies)
}

// member is a group consumer whose current assignment is tracked through the client's
// rebalance callbacks, and whose network can be cut to simulate a crash (no LeaveGroup).
type member struct {
	cl     *kgo.Client
	mu     sync.Mutex
	parts  map[int32]bool
	conns  []net.Conn
	dead   bool
	cancel context.CancelFunc
}

func newMember(env Env, group, topic string, balancer kgo.GroupBalancer, extra ...kgo.Opt) (*member, error) {
	m := &member{parts: map[int32]bool{}}
	set := func(ps map[string][]int32, add bool) {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, p := range ps[topic] {
			if add {
				m.parts[p] = true
			} else {
				delete(m.parts, p)
			}
		}
	}
	dialer := func(ctx context.Context, network, host string) (net.Conn, error) {
		m.mu.Lock()
		dead := m.dead
		m.mu.Unlock()
		if dead {
			return nil, fmt.Errorf("member crashed")
		}
		c, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, host)
		if err == nil {
			m.mu.Lock()
			m.conns = append(m.conns, c)
			m.mu.Unlock()
		}
		return c, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(env.Bootstrap), kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic),
		kgo.Balancers(balancer), kgo.SessionTimeout(6 * time.Second), kgo.HeartbeatInterval(500 * time.Millisecond),
		kgo.Dialer(dialer),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, ps map[string][]int32) { set(ps, true) }),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, ps map[string][]int32) { set(ps, false) }),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, ps map[string][]int32) { set(ps, false) }),
	}
	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, err
	}
	m.cl = cl
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go func() {
		for ctx.Err() == nil {
			cl.PollFetches(ctx)
		}
	}()
	return m, nil
}

func (m *member) assignment() []int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int32, 0, len(m.parts))
	for p := range m.parts {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// crash cuts the member's network without a LeaveGroup; the broker must notice through the
// session timeout.
func (m *member) crash() {
	m.mu.Lock()
	m.dead = true
	conns := m.conns
	m.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	m.cancel()
}

// discard closes a crashed member's client once the probe is done with it. Its dialer refuses,
// so no LeaveGroup reaches the broker.
func (m *member) discard() { m.cl.Close() }

func (m *member) close() {
	m.cancel()
	m.cl.Close() // sends LeaveGroup
}

// waitAssigned waits until the members' assignments together cover exactly want partitions,
// each partition once, with every member holding at least min partitions.
func waitAssigned(ctx context.Context, members []*member, want, min int, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		seen := map[int32]int{}
		ok := true
		for _, m := range members {
			a := m.assignment()
			if len(a) < min {
				ok = false
			}
			for _, p := range a {
				seen[p]++
			}
		}
		if ok && len(seen) == want {
			for _, n := range seen {
				if n != 1 {
					ok = false
				}
			}
			if ok {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func groupTopic(ctx context.Context, env Env, name string, partitions int32) (string, error) {
	cl, adm, err := adminClient(env)
	if err != nil {
		return "", err
	}
	defer cl.Close()
	topic := topicName(env, name)
	return topic, createTopic(ctx, adm, topic, partitions, nil)
}

// assignor: two members of one group on a 6-partition topic with the given client-side
// assignor. The broker relays the leader's assignment; the observation is the shape of it.
func assignor(b kgo.GroupBalancer, name string) Probe {
	return func(ctx context.Context, env Env) (Observation, error) {
		topic, err := groupTopic(ctx, env, "assign-"+name, 6)
		if err != nil {
			return nil, err
		}
		group := topic + "-g"
		a, err := newMember(env, group, topic, b)
		if err != nil {
			return nil, err
		}
		defer a.close()
		if !waitAssigned(ctx, []*member{a}, 6, 6, 30*time.Second) {
			return Observation{"single_member_got_all_6": false}, nil
		}
		bm, err := newMember(env, group, topic, b)
		if err != nil {
			return nil, err
		}
		defer bm.close()
		balanced := waitAssigned(ctx, []*member{a, bm}, 6, 3, 30*time.Second)
		counts := []int{len(a.assignment()), len(bm.assignment())}
		sort.Ints(counts)
		obs := Observation{"single_member_got_all_6": true, "two_members_balanced_3_and_3": balanced, "partition_counts": counts}
		if name == "range" {
			obs["each_member_contiguous"] = contiguous(a.assignment()) && contiguous(bm.assignment())
		}
		if name == "roundrobin" {
			obs["each_member_alternating"] = alternating(a.assignment()) && alternating(bm.assignment())
		}
		return obs, nil
	}
}

func contiguous(ps []int32) bool {
	for i := 1; i < len(ps); i++ {
		if ps[i] != ps[i-1]+1 {
			return false
		}
	}
	return len(ps) > 0
}

func alternating(ps []int32) bool {
	for i := 1; i < len(ps); i++ {
		if ps[i] != ps[i-1]+2 {
			return false
		}
	}
	return len(ps) > 0
}

// rebalanceJoinLeaveCrash: one member holds all 6 partitions; a second joins (3 and 3); it
// leaves cleanly (the first gets all 6 back well before the session timeout); a third joins
// and then crashes without leaving (the first gets all 6 back only after the 6 s session
// timeout, and within 30 s).
func rebalanceJoinLeaveCrash(ctx context.Context, env Env) (Observation, error) {
	topic, err := groupTopic(ctx, env, "rebalance", 6)
	if err != nil {
		return nil, err
	}
	group := topic + "-g"
	b := kgo.RangeBalancer()
	a, err := newMember(env, group, topic, b)
	if err != nil {
		return nil, err
	}
	defer a.close()
	obs := Observation{"first_member_got_all_6": waitAssigned(ctx, []*member{a}, 6, 6, 30*time.Second)}
	m2, err := newMember(env, group, topic, b)
	if err != nil {
		return nil, err
	}
	obs["join_rebalanced_3_and_3"] = waitAssigned(ctx, []*member{a, m2}, 6, 3, 30*time.Second)
	m2.close()
	t0 := time.Now()
	back := waitAssigned(ctx, []*member{a}, 6, 6, 30*time.Second)
	obs["leave_reassigned_all_6"] = back
	obs["leave_reassigned_within_4s"] = back && time.Since(t0) < 4*time.Second

	m3, err := newMember(env, group, topic, b)
	if err != nil {
		return nil, err
	}
	obs["second_join_rebalanced_3_and_3"] = waitAssigned(ctx, []*member{a, m3}, 6, 3, 30*time.Second)
	m3.crash()
	defer m3.discard()
	t0 = time.Now()
	back = waitAssigned(ctx, []*member{a}, 6, 6, 40*time.Second)
	took := time.Since(t0)
	obs["crash_reassigned_all_6_within_30s"] = back && took < 30*time.Second
	obs["crash_reassignment_waited_for_session_timeout"] = back && took >= 4*time.Second
	return obs, nil
}

// --- raw group protocol --------------------------------------------------------------------

// rawGroup speaks the group protocol directly. Every simulated member needs its own client:
// a broker serves one request at a time per connection, so a parked JoinGroup would otherwise
// block the other member's heartbeats.
type rawGroup struct {
	cl    *kgo.Client // the first member's connection
	env   Env
	group string
	topic string
	extra []*kgo.Client
}

func (g *rawGroup) newMemberClient() (*kgo.Client, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(g.env.Bootstrap))
	if err == nil {
		g.extra = append(g.extra, cl)
	}
	return cl, err
}

func (g *rawGroup) close() {
	g.cl.Close()
	for _, c := range g.extra {
		c.Close()
	}
}

func newRawGroup(ctx context.Context, env Env, name string) (*rawGroup, error) {
	topic, err := groupTopic(ctx, env, name, 2)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap))
	if err != nil {
		return nil, err
	}
	return &rawGroup{cl: cl, env: env, group: topic + "-g", topic: topic}, nil
}

// join sends JoinGroup, answering MEMBER_ID_REQUIRED (79) with the member id the broker gave.
func (g *rawGroup) join(ctx context.Context, cl *kgo.Client, memberID string, instance *string, sessionMs, rebalanceMs int32) (*kmsg.JoinGroupResponse, error) {
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Topics = []string{g.topic}
	for i := 0; i < 2; i++ {
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group, req.MemberID, req.InstanceID = g.group, memberID, instance
		req.SessionTimeoutMillis, req.RebalanceTimeoutMillis = sessionMs, rebalanceMs
		req.ProtocolType = "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name, p.Metadata = "range", meta.AppendTo(nil)
		req.Protocols = append(req.Protocols, p)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			return nil, err
		}
		if resp.ErrorCode == 79 && i == 0 {
			memberID = resp.MemberID
			continue
		}
		return resp, nil
	}
	return nil, fmt.Errorf("unreachable")
}

func (g *rawGroup) sync(ctx context.Context, cl *kgo.Client, jr *kmsg.JoinGroupResponse, instance *string) (int16, error) {
	req := kmsg.NewPtrSyncGroupRequest()
	req.Group, req.Generation, req.MemberID, req.InstanceID = g.group, jr.Generation, jr.MemberID, instance
	pt, pn := "consumer", "range"
	req.ProtocolType, req.Protocol = &pt, &pn
	if jr.LeaderID == jr.MemberID {
		for _, m := range jr.Members {
			a := kmsg.NewSyncGroupRequestGroupAssignment()
			a.MemberID = m.MemberID
			req.GroupAssignment = append(req.GroupAssignment, a)
		}
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, err
	}
	return resp.ErrorCode, nil
}

func (g *rawGroup) heartbeat(ctx context.Context, cl *kgo.Client, gen int32, memberID string, instance *string) (int16, error) {
	req := kmsg.NewPtrHeartbeatRequest()
	req.Group, req.Generation, req.MemberID, req.InstanceID = g.group, gen, memberID, instance
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, err
	}
	return resp.ErrorCode, nil
}

// sessionTimeoutBounds: a session timeout below group.min.session.timeout.ms (6 s) or above
// group.max.session.timeout.ms (30 min) is refused with INVALID_SESSION_TIMEOUT (26).
func sessionTimeoutBounds(ctx context.Context, env Env) (Observation, error) {
	g, err := newRawGroup(ctx, env, "session-bounds")
	if err != nil {
		return nil, err
	}
	defer g.close()
	obs := Observation{}
	for name, ms := range map[string]int32{"1s": 1000, "10s": 10000, "2h": 7200000} {
		resp, err := g.join(ctx, g.cl, "", nil, ms, 10000)
		if err != nil {
			return nil, err
		}
		obs["session_"+name+"_error_code"] = int(resp.ErrorCode)
	}
	return obs, nil
}

// rebalanceTimeoutExpiry: member 1 is stable; member 2 joins, starting a rebalance; member 1
// never rejoins. After member 1's rebalance timeout (3 s, which Java sets from
// max.poll.interval.ms) the broker completes the rebalance without it and member 1's next
// heartbeat is refused.
func rebalanceTimeoutExpiry(ctx context.Context, env Env) (Observation, error) {
	g, err := newRawGroup(ctx, env, "rebalance-timeout")
	if err != nil {
		return nil, err
	}
	defer g.close()
	c1 := g.cl
	c2, err := g.newMemberClient()
	if err != nil {
		return nil, err
	}
	j1, err := g.join(ctx, c1, "", nil, 30000, 3000)
	if err != nil {
		return nil, err
	}
	if _, err := g.sync(ctx, c1, j1, nil); err != nil {
		return nil, err
	}
	obs := Observation{"member1_join_error_code": int(j1.ErrorCode)}
	start := time.Now()
	j2, err := g.join(ctx, c2, "", nil, 30000, 3000) // blocks until the rebalance completes
	if err != nil {
		return nil, err
	}
	waited := time.Since(start)
	obs["member2_join_error_code"] = int(j2.ErrorCode)
	obs["member2_join_completed_after_member1_rebalance_timeout"] = waited >= 2500*time.Millisecond
	obs["member2_join_completed_within_15s"] = waited < 15*time.Second
	obs["member2_is_only_member"] = len(j2.Members) <= 1 && j2.LeaderID == j2.MemberID
	if _, err := g.sync(ctx, c2, j2, nil); err != nil {
		return nil, err
	}
	hb, err := g.heartbeat(ctx, c1, j1.Generation, j1.MemberID, nil)
	if err != nil {
		return nil, err
	}
	obs["member1_heartbeat_error_code"] = int(hb)
	return obs, nil
}

// staticMembership (KIP-345): two static members form a stable group. The follower
// "restarts": it rejoins with its instance id and no member id. Kafka keeps the generation (no
// rebalance), the leader's heartbeat stays OK, and the replaced member id is fenced with
// FENCED_INSTANCE_ID (82). (A restarting static leader is deliberately rebalanced by Kafka, so
// the probe restarts the follower.) The oracle must show exactly that, or the probe itself is
// wrong and the row is VOID.
func staticMembership(ctx context.Context, env Env) (Observation, error) {
	g, err := newRawGroup(ctx, env, "static")
	if err != nil {
		return nil, err
	}
	defer g.close()
	ia, ib := "instance-a", "instance-b"
	ca := g.cl
	cb, err := g.newMemberClient()
	if err != nil {
		return nil, err
	}
	ja, err := g.join(ctx, ca, "", &ia, 30000, 10000)
	if err != nil {
		return nil, err
	}
	if _, err := g.sync(ctx, ca, ja, &ia); err != nil {
		return nil, err
	}
	// b joins (starts a rebalance); a rejoins once its heartbeat reports the rebalance.
	var jb *kmsg.JoinGroupResponse
	var errB error
	done := make(chan struct{})
	go func() { jb, errB = g.join(ctx, cb, "", &ib, 30000, 10000); close(done) }()
	for i := 0; i < 100; i++ {
		code, err := g.heartbeat(ctx, ca, ja.Generation, ja.MemberID, &ia)
		if err != nil {
			return nil, err
		}
		if code == 27 { // REBALANCE_IN_PROGRESS
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	ja2, err := g.join(ctx, ca, ja.MemberID, &ia, 30000, 10000)
	if err != nil {
		return nil, err
	}
	<-done
	if errB != nil {
		return nil, errB
	}
	codes := make(chan int16, 2)
	go func() { c, _ := g.sync(ctx, cb, jb, &ib); codes <- c }()
	go func() { c, _ := g.sync(ctx, ca, ja2, &ia); codes <- c }()
	sa, sb := <-codes, <-codes
	obs := Observation{
		"rejoin_error_code": int(ja2.ErrorCode), "second_member_join_error_code": int(jb.ErrorCode),
		"sync_error_codes": []int{int(min(sa, sb)), int(max(sa, sb))}, "members_share_generation": ja2.Generation == jb.Generation,
	}
	joined := ja2.ErrorCode == 0 && jb.ErrorCode == 0 && sa == 0 && sb == 0 && ja2.Generation == jb.Generation

	// Identify leader and follower; restart the follower.
	leader, follower := ja2, jb
	leaderID, followerID := &ia, &ib
	leaderCl, followerCl := ca, cb
	if jb.MemberID == jb.LeaderID {
		leader, follower, leaderID, followerID, leaderCl, followerCl = jb, ja2, &ib, &ia, cb, ca
	}
	stableGen := follower.Generation
	restarted, err := g.newMemberClient() // the restarted process has a new connection
	if err != nil {
		return nil, err
	}
	jr, err := g.join(ctx, restarted, "", followerID, 30000, 10000)
	if err != nil {
		return nil, err
	}
	obs["restart_rejoin_error_code"] = int(jr.ErrorCode)
	obs["restart_kept_generation"] = jr.Generation == stableGen
	hbLeader, err := g.heartbeat(ctx, leaderCl, leader.Generation, leader.MemberID, leaderID)
	if err != nil {
		return nil, err
	}
	obs["leader_heartbeat_error_code"] = int(hbLeader)
	hbOld, err := g.heartbeat(ctx, followerCl, follower.Generation, follower.MemberID, followerID)
	if err != nil {
		return nil, err
	}
	obs["replaced_member_id_heartbeat_error_code"] = int(hbOld)
	if env.Target == "real-kafka" && !(joined && obs["restart_kept_generation"] == true && hbLeader == 0 && hbOld == 82) {
		return obs, fmt.Errorf("probe precondition: the oracle did not show KIP-345 static membership: %v", obs)
	}
	return obs, nil
}

// offsetResetPolicies: a new group with no committed offset starts at the earliest record
// (earliest) or sees only records produced after it joined (latest). "none" is a client-side
// policy with no broker behaviour to compare, so it is not part of this row.
func offsetResetPolicies(ctx context.Context, env Env) (Observation, error) {
	topic, err := groupTopic(ctx, env, "reset", 1)
	if err != nil {
		return nil, err
	}
	pr, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.DefaultProduceTopic(topic))
	if err != nil {
		return nil, err
	}
	defer pr.Close()
	for i := 0; i < 5; i++ {
		if err := pr.ProduceSync(ctx, &kgo.Record{Value: []byte(fmt.Sprintf("old-%d", i))}).FirstErr(); err != nil {
			return Observation{"produce_error_code": errCode(err)}, nil
		}
	}
	first := func(reset kgo.Offset, produceNew bool) (string, error) {
		assigned := make(chan struct{}, 1)
		cl, err := kgo.NewClient(kgo.SeedBrokers(env.Bootstrap), kgo.ConsumerGroup(topic+fmt.Sprintf("-%d", time.Now().UnixNano())),
			kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(reset), kgo.FetchMaxWait(300*time.Millisecond),
			kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
				select {
				case assigned <- struct{}{}:
				default:
				}
			}))
		if err != nil {
			return "", err
		}
		defer cl.Close()
		c, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		if produceNew {
			// Produce the new record only once the member is assigned and has had time to resolve
			// "latest", so the record cannot land before the reset is decided.
			go func() {
				select {
				case <-assigned:
				case <-c.Done():
					return
				}
				time.Sleep(2 * time.Second)
				_ = pr.ProduceSync(ctx, &kgo.Record{Value: []byte("new")}).FirstErr()
			}()
		}
		for c.Err() == nil {
			fs := cl.PollFetches(c)
			if err := fs.Err0(); err != nil && c.Err() == nil {
				return "error", nil
			}
			var v string
			fs.EachRecord(func(r *kgo.Record) {
				if v == "" {
					v = string(r.Value)
				}
			})
			if v != "" {
				return v, nil
			}
		}
		return "nothing", nil
	}
	earliest, err := first(kgo.NewOffset().AtStart(), false)
	if err != nil {
		return nil, err
	}
	latest, err := first(kgo.NewOffset().AtEnd(), true)
	if err != nil {
		return nil, err
	}
	return Observation{"earliest_first_record": earliest, "latest_first_record": latest}, nil
}
