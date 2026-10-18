// Confluent.Kafka (.NET) driver of the clients track (suites/clients/SCENARIO.md).
// The ledger goes to stdout, one JSON object per line; diagnostics go to stderr.
using System.Reflection;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

static class Driver
{
    const string Client = "confluent-kafka-dotnet";
    static readonly object Lock = new();
    static readonly string Bootstrap = Env("KCERT_BOOTSTRAP", "");
    static readonly string Mode = Env("KCERT_MODE", "");
    static readonly string Topic = Env("KCERT_TOPIC", "");
    static readonly string Group = Env("KCERT_GROUP", "");
    static readonly int Partitions = int.Parse(Env("KCERT_PARTITIONS", "0"));
    static readonly int Records = int.Parse(Env("KCERT_RECORDS", "0"));
    static readonly int Records2 = int.Parse(Env("KCERT_RECORDS2", "0"));
    static readonly int Keys = int.Parse(Env("KCERT_KEYS", "1"));
    static readonly TimeSpan Timeout = TimeSpan.FromSeconds(int.Parse(Env("KCERT_TIMEOUT_SEC", "60")));
    static readonly bool Admin = Env("KCERT_ADMIN", "1") != "0";

    static string Env(string k, string d) => Environment.GetEnvironmentVariable(k) ?? d;

    static void Emit(string ev, Dictionary<string, object?>? f = null)
    {
        var m = new Dictionary<string, object?> { ["ev"] = ev };
        if (f != null) foreach (var kv in f) m[kv.Key] = kv.Value;
        var line = JsonSerializer.Serialize(m);
        lock (Lock) { Console.Out.WriteLine(line); Console.Out.Flush(); }
    }

    static void Fail(string step, int code, string msg)
    {
        Emit("error", new() { ["step"] = step, ["error_code"] = code, ["error"] = msg });
        Emit("done");
        Environment.Exit(1);
    }

    // Kafka protocol error code; librdkafka's local errors (negative) map to -1.
    static int Code(Error? e) => e == null || e.Code == ErrorCode.NoError ? 0 : ((int)e.Code >= 0 ? (int)e.Code : -1);

    static string Value(int seq) => $"value-{seq:D6}";
    static string Key(int seq) => $"key-{seq % Keys:D2}";
    static int SeqOf(string? v) => v != null && v.StartsWith("value-") && int.TryParse(v[6..], out var n) ? n : -1;

    static string Version()
    {
        var asm = typeof(Library).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? "unknown";
        asm = asm.Split('+')[0];
        return $"{asm} (librdkafka {Library.VersionString})";
    }

    static int Main()
    {
        if (Bootstrap == "" || Topic == "" || Group == "" || (Mode != "scenario" && Mode != "stock-defaults"))
            Fail("inputs", -1, "KCERT_BOOTSTRAP, KCERT_TOPIC, KCERT_GROUP and KCERT_MODE are required");
        // librdkafka's documented default group.protocol is classic; the scenario sets it explicitly
        // (the allowed deviation), stock defaults leave it unset.
        Emit("start", new() { ["client"] = Client, ["version"] = Version(), ["mode"] = Mode, ["group_protocol"] = "classic" });
        try
        {
            if (Mode == "scenario") Scenario(); else Stock();
        }
        catch (KafkaException e)
        {
            Fail("driver", Code(e.Error), e.Message);
        }
        Emit("done");
        return 0;
    }

    static void Create(int parts, short rf)
    {
        using var a = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        int code = 0;
        string err = "";
        try
        {
            a.CreateTopicsAsync(new[] { new TopicSpecification { Name = Topic, NumPartitions = parts, ReplicationFactor = rf } })
                .GetAwaiter().GetResult();
        }
        catch (CreateTopicsException e)
        {
            code = Code(e.Results[0].Error);
            err = e.Results[0].Error.Reason;
        }
        Emit("create", new() { ["topic"] = Topic, ["partitions"] = parts, ["error_code"] = code, ["error"] = err });
        if (code != 0) Fail("create", code, err);
        // An application waits until the topic is usable before producing.
        var deadline = DateTime.UtcNow + Timeout;
        while (DateTime.UtcNow < deadline)
        {
            var md = a.GetMetadata(Topic, TimeSpan.FromSeconds(5));
            var t = md.Topics.FirstOrDefault(x => x.Topic == Topic);
            if (t != null && t.Error.Code == ErrorCode.NoError && t.Partitions.Count > 0 &&
                (parts < 0 || t.Partitions.Count == parts) && t.Partitions.All(p => p.Leader >= 0 && p.Error.Code == ErrorCode.NoError))
                return;
            Thread.Sleep(200);
        }
        Fail("create", -1, "topic never became ready");
    }

    static void Produce<TK>(IProducer<TK, string> p, int phase, int lo, int hi, Func<int, TK> key, bool headers)
    {
        for (int seq = lo; seq < hi; seq++)
        {
            int s = seq;
            var m = new Message<TK, string> { Key = key(seq), Value = Value(seq) };
            if (headers)
                m.Headers = new Headers { { "seq", Encoding.UTF8.GetBytes(seq.ToString()) }, { "client", Encoding.UTF8.GetBytes(Client) } };
            p.Produce(Topic, m, dr =>
            {
                if (dr.Error.IsError)
                    Emit("ack", new() { ["phase"] = phase, ["seq"] = s, ["partition"] = -1, ["offset"] = -1, ["error_code"] = Code(dr.Error), ["error"] = dr.Error.Reason });
                else
                    Emit("ack", new() { ["phase"] = phase, ["seq"] = s, ["partition"] = dr.Partition.Value, ["offset"] = dr.Offset.Value, ["error_code"] = 0 });
            });
        }
        if (p.Flush(Timeout) > 0) Fail("produce", -1, "records still unacknowledged after the deadline");
    }

    static void Recv<TK>(ConsumeResult<TK, string> r, int phase, HashSet<int> got)
    {
        int seq = SeqOf(r.Message.Value);
        got.Add(seq);
        var hs = (r.Message.Headers ?? new Headers()).Select(h => new[] { h.Key, Encoding.UTF8.GetString(h.GetValueBytes() ?? Array.Empty<byte>()) }).ToList();
        object? key = r.Message.Key is string k ? k : null;
        Emit("recv", new() { ["phase"] = phase, ["seq"] = seq, ["partition"] = r.Partition.Value, ["offset"] = r.Offset.Value,
            ["key"] = key, ["value"] = r.Message.Value, ["headers"] = hs });
    }

    // PollFor reads for d, or until got holds want records when want > 0.
    static void PollFor<TK>(IConsumer<TK, string> c, int phase, HashSet<int> got, int want, TimeSpan d)
    {
        var end = DateTime.UtcNow + d;
        while (DateTime.UtcNow < end && (want <= 0 || got.Count < want))
        {
            var r = c.Consume(TimeSpan.FromMilliseconds(200));
            if (r != null && !r.IsPartitionEOF) Recv(r, phase, got);
        }
    }

    static void Assigned(int phase, List<TopicPartition> parts) =>
        Emit("assigned", new() { ["phase"] = phase, ["partitions"] = parts.Select(p => p.Partition.Value).OrderBy(x => x).ToList() });

    static void Consume(int phase, int want, bool drain)
    {
        var cfg = new ConsumerConfig
        {
            BootstrapServers = Bootstrap, GroupId = Group,
            AutoOffsetReset = AutoOffsetReset.Earliest, EnableAutoCommit = false,
        };
        cfg.Set("group.protocol", "classic"); // the one allowed deviation
        using var c = new ConsumerBuilder<string, string>(cfg).SetPartitionsAssignedHandler((_, ps) => Assigned(phase, ps)).Build();
        c.Subscribe(Topic);
        var got = new HashSet<int>();
        PollFor(c, phase, got, want, Timeout);
        if (got.Count < want) Fail("consume", -1, $"received {got.Count} of {want} records before the deadline");
        if (drain) PollFor(c, phase, got, 0, TimeSpan.FromSeconds(2));
        int code = 0;
        string err = "";
        try { c.Commit(); }
        catch (KafkaException e) { code = Code(e.Error); err = e.Message; }
        Emit("commit", new() { ["phase"] = phase, ["error_code"] = code, ["error"] = err });
        c.Close();
    }

    static void Scenario()
    {
        if (Admin) Create(Partitions, 1);
        using var p = new ProducerBuilder<string, string>(new ProducerConfig { BootstrapServers = Bootstrap, Acks = Acks.All, EnableIdempotence = true }).Build();
        Produce(p, 1, 0, Records, Key, true);
        Consume(1, Records, false);
        Produce(p, 2, Records, Records + Records2, Key, true);
        Consume(2, Records2, true);
    }

    static void Stock()
    {
        if (Admin) Create(-1, -1);
        bool assigned = false; // an empty assignment is not readiness
        List<TopicPartition> held = new();
        using var c = new ConsumerBuilder<Null, string>(new ConsumerConfig { BootstrapServers = Bootstrap, GroupId = Group })
            .SetPartitionsAssignedHandler((_, ps) => { Assigned(1, ps); if (ps.Count > 0) { held = ps; assigned = true; } }).Build();
        c.Subscribe(Topic);
        var got = new HashSet<int>();
        var deadline = DateTime.UtcNow + Timeout;
        while (!assigned)
        {
            if (DateTime.UtcNow > deadline) Fail("consume", -1, "consumer was never assigned partitions");
            PollFor(c, 1, got, 0, TimeSpan.FromMilliseconds(200));
        }
        // Readiness barrier. librdkafka's default auto.offset.reset is latest and Position reports
        // only the last consumed offset + 1, so the barrier is the cached high watermark: librdkafka
        // learns it from the first fetch (or offset lookup) of a partition, which needs the start
        // position resolved. On the fresh topic that watermark is where the consumer starts.
        var pending = new List<TopicPartition>(held);
        while (pending.Count > 0)
        {
            if (DateTime.UtcNow > deadline) Fail("consume", -1, "start position never resolved on " + string.Join(",", pending.Select(p => p.Partition.Value)));
            PollFor(c, 1, got, 0, TimeSpan.FromMilliseconds(200));
            foreach (var tp in pending.ToList())
            {
                var wm = c.GetWatermarkOffsets(tp);
                if (wm.High != Offset.Unset && wm.High.Value >= 0)
                {
                    Emit("position", new() { ["phase"] = 1, ["partition"] = tp.Partition.Value, ["offset"] = wm.High.Value });
                    pending.Remove(tp);
                }
            }
        }
        using (var p = new ProducerBuilder<Null, string>(new ProducerConfig { BootstrapServers = Bootstrap }).Build())
            Produce(p, 1, 0, Records, _ => null!, false);
        PollFor(c, 1, got, Records, Timeout);
        if (got.Count < Records) Fail("consume", -1, $"received {got.Count} of {Records} records before the deadline");
        c.Close();
    }
}
