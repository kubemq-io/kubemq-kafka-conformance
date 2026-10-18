package verify

import (
	"fmt"
	"sort"
)

// Rec is one record as migration-agent prints it.
type Rec struct {
	Partition int32             `json:"p"`
	Offset    int64             `json:"o"`
	Key       *string           `json:"k"`
	Value     *string           `json:"v"`
	Headers   map[string]string `json:"h,omitempty"`
	Timestamp int64             `json:"ts"`
}

// Latest is the value a compacted topic converges to per key: the last value written for the key
// in offset order, nil when the last write was a tombstone. Compaction may or may not have run on
// either side; the latest value per key is the same either way.
func Latest(recs []Rec) map[string]*string {
	sorted := append([]Rec(nil), recs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		if sorted[i].Partition != sorted[j].Partition {
			return sorted[i].Partition < sorted[j].Partition
		}
		return sorted[i].Offset < sorted[j].Offset
	})
	out := map[string]*string{}
	for _, r := range sorted {
		if r.Key == nil {
			continue
		}
		out[*r.Key] = r.Value
	}
	return out
}

// Compacted compares a compacted source topic with its mirror. deleted are the keys tombstoned on
// the source.
type Compacted struct {
	SameLatest     bool // every key's latest value is equal on both sides
	DeletedDeleted bool // every key deleted on the source reads as deleted on the target (tombstone latest or key gone)
	Problems       []string
}

func (c Compacted) Observation() map[string]any {
	return map[string]any{"same_latest_values": c.SameLatest, "deleted_keys_deleted": c.DeletedDeleted}
}

func CompareCompacted(src, tgt []Rec, deleted []string) Compacted {
	c := Compacted{SameLatest: true, DeletedDeleted: true}
	s, t := Latest(src), Latest(tgt)
	keys := map[string]bool{}
	for k := range s {
		keys[k] = true
	}
	for k := range t {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		sv, sok := s[k]
		tv, tok := t[k]
		// A tombstoned key may be gone entirely after compaction on one side: deleted == absent.
		sDel, tDel := !sok || sv == nil, !tok || tv == nil
		if sDel != tDel || (!sDel && *sv != *tv) {
			c.SameLatest = false
			c.Problems = append(c.Problems, fmt.Sprintf("key %s: source %s, target %s", k, show(sv, sok), show(tv, tok)))
		}
	}
	for _, k := range deleted {
		if tv, ok := t[k]; ok && tv != nil {
			c.DeletedDeleted = false
			c.Problems = append(c.Problems, fmt.Sprintf("key %s deleted on the source still has value %q on the target", k, *tv))
		}
	}
	return c
}

func show(v *string, present bool) string {
	switch {
	case !present:
		return "absent"
	case v == nil:
		return "deleted"
	default:
		return fmt.Sprintf("%q", *v)
	}
}

// Identical reports whether two dumps hold the same records at the same partitions and offsets,
// with the same keys, values, headers and timestamps, and explains the first differences.
func Identical(a, b []Rec) (bool, []string) {
	type pos struct {
		p int32
		o int64
	}
	idx := func(rs []Rec) map[pos]Rec {
		m := map[pos]Rec{}
		for _, r := range rs {
			m[pos{r.Partition, r.Offset}] = r
		}
		return m
	}
	am, bm := idx(a), idx(b)
	var problems []string
	add := func(format string, args ...any) {
		if len(problems) < 5 {
			problems = append(problems, fmt.Sprintf(format, args...))
		}
	}
	if len(a) != len(b) {
		add("%d records versus %d", len(a), len(b))
	}
	for k, ra := range am {
		rb, ok := bm[k]
		switch {
		case !ok:
			add("partition %d offset %d missing", k.p, k.o)
		case show(ra.Key, true) != show(rb.Key, true) || show(ra.Value, true) != show(rb.Value, true):
			add("partition %d offset %d: key/value %s=%s versus %s=%s", k.p, k.o, show(ra.Key, true), show(ra.Value, true), show(rb.Key, true), show(rb.Value, true))
		case fmt.Sprint(ra.Headers) != fmt.Sprint(rb.Headers):
			add("partition %d offset %d: headers %v versus %v", k.p, k.o, ra.Headers, rb.Headers)
		case ra.Timestamp != rb.Timestamp:
			add("partition %d offset %d: timestamp %d versus %d", k.p, k.o, ra.Timestamp, rb.Timestamp)
		}
	}
	for k := range bm {
		if _, ok := am[k]; !ok {
			add("partition %d offset %d unexpected", k.p, k.o)
		}
	}
	return len(problems) == 0, problems
}

// Delivered turns a consumer's records into per-partition delivered offsets, in delivery order.
func Delivered(recs []Rec) map[int32][]int64 {
	out := map[int32][]int64{}
	for _, r := range recs {
		out[r.Partition] = append(out[r.Partition], r.Offset)
	}
	return out
}
