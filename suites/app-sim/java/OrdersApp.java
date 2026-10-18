import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OrdersApp — a deliberately ordinary Kafka application, written the way a team that already runs
 * Apache Kafka would write it: plain kafka-clients, AdminClient for topic setup, an idempotent
 * producer, a consumer group of several members, and a consume-transform-produce transactional
 * pipeline. It knows nothing about KubeMQ. Repointing it is a change of bootstrap address and
 * nothing else — that is the claim under test.
 *
 * It does not judge its own results. Every observable fact (an acknowledged send, a delivered
 * record, a partition assignment, a committed or aborted transaction, an admin call and its error)
 * is appended to a JSON-lines ledger, and ../verify.py decides. An application that graded itself
 * would pass whenever it stopped looking.
 *
 * Commands (one per process): version, admin, produce, group, eos, readback, lag. Every command
 * first prints and records the kafka-clients version actually loaded (AppInfoParser, which reads
 * the jar's own manifest), so a result can never be attributed to a client that did not produce
 * it: run.sh and verify.py compare it with the version the run asked for.
 *
 * Client 4.x: the source compiles without a deprecation warning against 4.3.1 and 4.4.0-rc1. It
 * already avoided every API 4.0 removed (sendOffsetsToTransaction(Map, String), poll(long),
 * DescribeTopicsResult.all()). Of the 4.0 default changes, idempotence and acks=all were already set
 * explicitly; linger.ms moved 0 -> 5 (set explicitly for the plain producer, default for the
 * transactional one — batching only); group.protocol stays "classic", which is what the groups here
 * exercise; keyed partitioning is still murmur2, so per-customer placement is unchanged.
 */
public class OrdersApp {

    static final String ORDERS = "orders";
    static final String ENRICHED = "orders-enriched";
    static final String DLQ = "orders-dlq";
    static final int PARTITIONS = 12;

    // ---------------------------------------------------------------- arguments and ledger

    static final Map<String, String> ARGS = new HashMap<>();

    static String arg(String name, String dflt) {
        return ARGS.getOrDefault(name, dflt);
    }

    static int argInt(String name, int dflt) {
        return Integer.parseInt(arg(name, Integer.toString(dflt)));
    }

    static BufferedWriter ledger;

    /** Appends one JSON object built from alternating key/value pairs. Thread-safe. */
    static synchronized void emit(Object... kv) {
        StringBuilder sb = new StringBuilder("{\"t\":").append(System.currentTimeMillis());
        for (int i = 0; i + 1 < kv.length; i += 2) {
            sb.append(",\"").append(kv[i]).append("\":");
            Object v = kv[i + 1];
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else if (v instanceof Collection<?> c) {
                sb.append('[');
                boolean first = true;
                for (Object o : c) {
                    if (!first) sb.append(',');
                    first = false;
                    if (o instanceof Number) sb.append(o); else sb.append('"').append(esc(String.valueOf(o))).append('"');
                }
                sb.append(']');
            } else {
                sb.append('"').append(esc(String.valueOf(v))).append('"');
            }
        }
        sb.append("}\n");
        try {
            ledger.write(sb.toString());
            ledger.flush();
        } catch (IOException e) {
            throw new RuntimeException("ledger write failed — a ledger that silently drops events would pass everything", e);
        }
    }

    static String esc(String s) {
        StringBuilder b = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> { if (c < 0x20) b.append(String.format("\\u%04x", (int) c)); else b.append(c); }
            }
        }
        return b.toString();
    }

    static final Pattern FIELD = Pattern.compile("\"(\\w+)\":(\"([^\"]*)\"|(-?\\d+))");

    /** Minimal parser for the flat JSON values this app writes itself. */
    static Map<String, String> parse(String json) {
        Map<String, String> m = new HashMap<>();
        if (json == null) return m;
        Matcher mt = FIELD.matcher(json);
        while (mt.find()) m.put(mt.group(1), mt.group(3) != null ? mt.group(3) : mt.group(4));
        return m;
    }

    static String errName(Throwable t) {
        Throwable c = t;
        while (c instanceof ExecutionException || c instanceof CompletionException) {
            if (c.getCause() == null) break;
            c = c.getCause();
        }
        return c.getClass().getSimpleName() + ": " + c.getMessage();
    }

    /**
     * The kafka-clients version actually on the classpath: {version, commit id, class that answered,
     * jar it was loaded from}. AppInfoParser moved from org.apache.kafka.common.utils (up to 4.3) to
     * org.apache.kafka.common.utils.internals (4.4), so it is looked up by name rather than imported —
     * one source then compiles and reports correctly against both. Neither present is fatal: a run
     * that cannot say which client produced it proves nothing.
     */
    static String[] loadedClientVersion() {
        for (String name : List.of("org.apache.kafka.common.utils.AppInfoParser",
                "org.apache.kafka.common.utils.internals.AppInfoParser")) {
            try {
                Class<?> c = Class.forName(name);
                String version = (String) c.getMethod("getVersion").invoke(null);
                String commit = (String) c.getMethod("getCommitId").invoke(null);
                var src = KafkaProducer.class.getProtectionDomain().getCodeSource();
                String jar = src == null ? "unknown" : Paths.get(src.getLocation().getPath()).getFileName().toString();
                return new String[]{version, commit, name, jar};
            } catch (ReflectiveOperationException ignored) {
                // try the next location
            }
        }
        throw new IllegalStateException("kafka-clients version unknown: AppInfoParser is in neither known package");
    }

    static Properties common() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, arg("bootstrap", "localhost:9092"));
        p.put(AdminClientConfig.CLIENT_ID_CONFIG, "orders-app-java");
        return p;
    }

    public static void main(String[] argv) throws Exception {
        for (int i = 0; i + 1 < argv.length; i += 2) {
            if (!argv[i].startsWith("--")) throw new IllegalArgumentException("expected --flag value pairs, got " + argv[i]);
            ARGS.put(argv[i].substring(2), argv[i + 1]);
        }
        String cmd = arg("cmd", "");
        Path out = Paths.get(arg("ledger", "/out/" + cmd + "-java.jsonl"));
        Files.createDirectories(out.getParent());
        ledger = Files.newBufferedWriter(out, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        emit("ev", "start", "app", "java", "cmd", cmd, "bootstrap", arg("bootstrap", ""));
        int rc = 0;
        try {
            String[] cv = loadedClientVersion();
            System.out.println("kafka-clients version: " + cv[0] + " (commit " + cv[1] + ", " + cv[2] + ", from " + cv[3] + ")");
            System.out.flush();
            emit("ev", "client_version", "app", "java", "cmd", cmd, "lib", "kafka-clients", "version", cv[0],
                    "commit", cv[1], "via", cv[2], "jar", cv[3]);
            switch (cmd) {
                case "version" -> { }
                case "admin" -> admin();
                case "produce" -> produce();
                case "group" -> group();
                case "eos" -> eos();
                case "readback" -> readback();
                case "lag" -> lag();
                default -> throw new IllegalArgumentException("unknown --cmd " + cmd);
            }
        } catch (Throwable t) {
            emit("ev", "fatal", "app", "java", "cmd", cmd, "error", errName(t));
            t.printStackTrace();
            rc = 1;
        } finally {
            emit("ev", "end", "app", "java", "cmd", cmd, "rc", rc);
            ledger.close();
        }
        System.exit(rc);
    }

    // ---------------------------------------------------------------- admin

    /**
     * The topic setup an existing Kafka estate carries in its deployment scripts: 12 partitions,
     * the replication factor production uses, a compacted side topic, then a config change. Every
     * call is recorded with its outcome; a refused call is data, not a crash.
     */
    static void admin() throws Exception {
        short rf = (short) argInt("rf", 3);
        try (Admin admin = Admin.create(common())) {
            try {
                DescribeClusterResult dc = admin.describeCluster();
                int brokers = dc.nodes().get(30, TimeUnit.SECONDS).size();
                emit("ev", "admin", "app", "java", "op", "describe_cluster", "ok", true, "brokers", brokers,
                        "controller", String.valueOf(dc.controller().get(30, TimeUnit.SECONDS)));
            } catch (Exception e) {
                emit("ev", "admin", "app", "java", "op", "describe_cluster", "ok", false, "error", errName(e));
            }

            List<NewTopic> topics = List.of(
                    new NewTopic(ORDERS, PARTITIONS, rf).configs(Map.of("retention.ms", "86400000")),
                    new NewTopic(ENRICHED, PARTITIONS, rf),
                    new NewTopic(DLQ, 3, rf).configs(Map.of("cleanup.policy", "compact")));
            // The requested partition count goes into the ledger with every create, so the
            // verifier can hold the later describe to it: a broker that refuses (or ignores) the
            // create and then answers Metadata with an auto-created 1-partition "phantom" leaves
            // the application running on the wrong partition count, and only a comparison with
            // what was asked for can see that.
            Map<String, Integer> requested = new HashMap<>();
            for (NewTopic nt : topics) requested.put(nt.name(), nt.numPartitions());
            var res = admin.createTopics(topics).values();
            for (var e : res.entrySet()) {
                int want = requested.get(e.getKey());
                try {
                    e.getValue().get(60, TimeUnit.SECONDS);
                    emit("ev", "admin", "app", "java", "op", "create_topic", "target", e.getKey(), "ok", true, "rf", (int) rf,
                            "requested_partitions", want);
                } catch (Exception ex) {
                    boolean exists = errName(ex).startsWith("TopicExistsException");
                    emit("ev", "admin", "app", "java", "op", "create_topic", "target", e.getKey(), "ok", exists,
                            "rf", (int) rf, "requested_partitions", want, "error", errName(ex));
                }
            }

            // A just-created topic is not instantly describable, on Apache Kafka included: the
            // create returns once the controller accepted it, and brokers learn about it a moment
            // later. Real deployment scripts retry for that window, so this does too, and records
            // how many attempts it took — a count that keeps growing is itself a finding.
            for (String t : List.of(ORDERS, ENRICHED, DLQ)) {
                String lastErr = null;
                boolean done = false;
                for (int attempt = 1; attempt <= 30 && !done; attempt++) {
                    try {
                        TopicDescription td = admin.describeTopics(List.of(t)).allTopicNames().get(30, TimeUnit.SECONDS).get(t);
                        int replicas = td.partitions().isEmpty() ? 0 : td.partitions().get(0).replicas().size();
                        emit("ev", "admin", "app", "java", "op", "describe_topic", "target", t, "ok", true,
                                "partitions", td.partitions().size(), "replicas", replicas, "attempts", attempt);
                        done = true;
                    } catch (Exception e) {
                        lastErr = errName(e);
                        Thread.sleep(500);
                    }
                }
                if (!done) emit("ev", "admin", "app", "java", "op", "describe_topic", "target", t, "ok", false, "error", lastErr);
            }

            ConfigResource cr = new ConfigResource(ConfigResource.Type.TOPIC, ORDERS);
            try {
                admin.incrementalAlterConfigs(Map.of(cr, List.of(new AlterConfigOp(
                        new ConfigEntry("retention.ms", "604800000"), AlterConfigOp.OpType.SET)))).all().get(30, TimeUnit.SECONDS);
                emit("ev", "admin", "app", "java", "op", "alter_config", "target", ORDERS, "ok", true);
            } catch (Exception e) {
                emit("ev", "admin", "app", "java", "op", "alter_config", "target", ORDERS, "ok", false, "error", errName(e));
            }
            // Read the change back. Like topic creation it propagates rather than lands, so poll
            // until the new value is visible; the verifier then checks it IS the new value, since
            // "describe succeeded" alone is satisfied by a change that was silently dropped.
            String seen = null;
            String lastErr = null;
            int attempts = 0;
            for (attempts = 1; attempts <= 30; attempts++) {
                try {
                    var cfg = admin.describeConfigs(List.of(cr)).all().get(30, TimeUnit.SECONDS).get(cr);
                    ConfigEntry ret = cfg.get("retention.ms");
                    seen = ret == null ? null : ret.value();
                    if ("604800000".equals(seen)) break;
                } catch (Exception e) {
                    lastErr = errName(e);
                }
                Thread.sleep(500);
            }
            if (seen == null && lastErr != null) {
                emit("ev", "admin", "app", "java", "op", "describe_config", "target", ORDERS, "ok", false, "error", lastErr);
            } else {
                emit("ev", "admin", "app", "java", "op", "describe_config", "target", ORDERS, "ok", true,
                        "retention_ms", seen, "expect_retention_ms", "604800000", "attempts", Math.min(attempts, 30));
            }
            // Listing is answered by whichever broker the client asks, and a broker that has not
            // yet learned about a just-created topic leaves it out. Retry through that window, as
            // a real script would, and record the attempts — the window's length is the finding.
            Set<String> names = Set.of();
            String listErr = null;
            int listAttempts;
            for (listAttempts = 1; listAttempts <= 30; listAttempts++) {
                try {
                    names = admin.listTopics().names().get(30, TimeUnit.SECONDS);
                    listErr = null;
                    if (names.containsAll(List.of(ORDERS, ENRICHED, DLQ))) break;
                } catch (Exception e) {
                    listErr = errName(e);
                }
                Thread.sleep(500);
            }
            boolean listed = names.containsAll(List.of(ORDERS, ENRICHED, DLQ));
            emit("ev", "admin", "app", "java", "op", "list_topics", "ok", listed, "count", names.size(),
                    "attempts", Math.min(listAttempts, 30), "error", listed ? null : (listErr != null ? listErr : "created topics missing from the listing after 30 attempts"));
        }
    }

    // ---------------------------------------------------------------- idempotent producer

    /**
     * An idempotent producer, acks=all, one pass per compression codec. The record key is a
     * customer id, so every customer's orders land on one partition in order — the property a
     * real order pipeline relies on and the verifier checks.
     */
    static void produce() throws Exception {
        String app = arg("app", "java");
        int count = argInt("count", 4000);
        List<String> codecs = List.of(arg("codecs", "gzip,lz4").split(","));
        int perCodec = count / codecs.size();
        String pad = "x".repeat(argInt("pad", 400));
        int seq = 0;
        for (String codec : codecs) {
            Properties p = common();
            p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            p.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, codec);
            p.put(ProducerConfig.LINGER_MS_CONFIG, "5");
            p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
            AtomicLong acked = new AtomicLong();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
                int end = seq + perCodec;
                for (; seq < end; seq++) {
                    final int s = seq;
                    String key = "cust-" + (s % 200);
                    String value = "{\"app\":\"" + app + "\",\"seq\":" + s + ",\"codec\":\"" + codec + "\",\"pad\":\"" + pad + "\"}";
                    producer.send(new ProducerRecord<>(ORDERS, key, value), (md, ex) -> {
                        if (ex == null) {
                            acked.incrementAndGet();
                            emit("ev", "produced", "app", app, "topic", ORDERS, "p", md.partition(), "o", md.offset(),
                                    "key", key, "seq", s, "codec", codec);
                        } else {
                            emit("ev", "produce_failed", "app", app, "key", key, "seq", s, "codec", codec, "error", errName(ex));
                        }
                    });
                }
                producer.flush();
            }
            emit("ev", "produce_phase", "app", app, "codec", codec, "acked", acked.get(), "sent", perCodec);
        }
    }

    // ---------------------------------------------------------------- consumer group

    static Properties consumerProps(String group, String isolation) {
        Properties p = common();
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        return p;
    }

    /** Stop when the producers have finished and nothing has arrived for idle-exit seconds. */
    static boolean finished(long lastRecordMs, long startMs) {
        long now = System.currentTimeMillis();
        if (now - startMs > argInt("timeout-sec", 300) * 1000L) return true;
        boolean producersDone = Files.exists(Paths.get(arg("done-file", "/out/produce.done")));
        return producersDone && now - lastRecordMs > argInt("idle-exit-sec", 15) * 1000L;
    }

    /**
     * Three members join one group; the third leaves partway through and a fourth joins later, so
     * the group rebalances twice while traffic flows. Offsets are committed synchronously after
     * each poll and on revoke — the at-least-once shape most real consumers have.
     */
    static void group() throws Exception {
        String group = arg("group", "orders-java");
        int members = argInt("members", 3);
        long leaveAfter = argInt("leave-after-sec", 8) * 1000L;
        long joinAfter = argInt("join-after-sec", 16) * 1000L;
        long start = System.currentTimeMillis();
        AtomicLong lastRecord = new AtomicLong(start);
        Map<String, AtomicBoolean> stops = new ConcurrentHashMap<>();
        ExecutorService pool = Executors.newCachedThreadPool();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 1; i <= members; i++) futures.add(startMember(pool, group, "m" + i, stops, lastRecord));
        boolean left = false, joined = false;
        while (!finished(lastRecord.get(), start)) {
            long el = System.currentTimeMillis() - start;
            if (!left && el > leaveAfter) {
                stops.get("m" + members).set(true);
                emit("ev", "member_leave_requested", "group", group, "member", "m" + members);
                left = true;
            }
            if (!joined && el > joinAfter) {
                futures.add(startMember(pool, group, "m" + (members + 1), stops, lastRecord));
                emit("ev", "member_join_requested", "group", group, "member", "m" + (members + 1));
                joined = true;
            }
            Thread.sleep(200);
        }
        stops.values().forEach(s -> s.set(true));
        for (Future<?> f : futures) f.get(60, TimeUnit.SECONDS);
        pool.shutdown();
    }

    static Future<?> startMember(ExecutorService pool, String group, String member, Map<String, AtomicBoolean> stops, AtomicLong lastRecord) {
        AtomicBoolean stop = new AtomicBoolean();
        stops.put(member, stop);
        return pool.submit(() -> {
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps(group, "read_uncommitted"))) {
                Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();
                c.subscribe(List.of(ORDERS), new ConsumerRebalanceListener() {
                    public void onPartitionsRevoked(Collection<TopicPartition> parts) {
                        commit(c, pending, group, member);
                        emit("ev", "revoked", "group", group, "member", member, "partitions", parts.stream().map(TopicPartition::partition).sorted().toList());
                    }
                    public void onPartitionsAssigned(Collection<TopicPartition> parts) {
                        emit("ev", "assigned", "group", group, "member", member, "partitions", parts.stream().map(TopicPartition::partition).sorted().toList());
                    }
                    public void onPartitionsLost(Collection<TopicPartition> parts) {
                        pending.clear();
                        emit("ev", "lost", "group", group, "member", member, "partitions", parts.stream().map(TopicPartition::partition).sorted().toList());
                    }
                });
                emit("ev", "member_started", "group", group, "member", member);
                while (!stop.get()) {
                    ConsumerRecords<String, String> recs;
                    try {
                        recs = c.poll(Duration.ofMillis(500));
                    } catch (Exception e) {
                        emit("ev", "poll_error", "group", group, "member", member, "error", errName(e));
                        Thread.sleep(500);
                        continue;
                    }
                    for (ConsumerRecord<String, String> r : recs) {
                        Map<String, String> v = parse(r.value());
                        emit("ev", "consumed", "group", group, "member", member, "topic", r.topic(), "p", r.partition(), "o", r.offset(),
                                "key", r.key(), "src_app", v.get("app"), "seq", v.get("seq") == null ? null : Integer.parseInt(v.get("seq")),
                                "codec", v.get("codec"));
                        pending.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                        lastRecord.set(System.currentTimeMillis());
                    }
                    if (!recs.isEmpty()) commit(c, pending, group, member);
                }
                commit(c, pending, group, member);
                emit("ev", "member_stopped", "group", group, "member", member);
            } catch (Exception e) {
                emit("ev", "member_error", "group", group, "member", member, "error", errName(e));
            }
            return null;
        });
    }

    static void commit(KafkaConsumer<String, String> c, Map<TopicPartition, OffsetAndMetadata> pending, String group, String member) {
        if (pending.isEmpty()) return;
        try {
            c.commitSync(pending, Duration.ofSeconds(30));
            for (var e : pending.entrySet())
                emit("ev", "committed", "group", group, "member", member, "p", e.getKey().partition(), "o", e.getValue().offset());
        } catch (Exception e) {
            emit("ev", "commit_failed", "group", group, "member", member, "error", errName(e));
        }
        pending.clear();
    }

    // ---------------------------------------------------------------- transactional pipeline

    /**
     * Consume-transform-produce with exactly-once semantics: read `orders` with read_committed,
     * write one enriched record per input to `orders-enriched`, and commit the input offsets inside
     * the same transaction. Every Nth transaction is ABORTED on purpose after its records were sent
     * and acknowledged, and the input is rewound and processed again — so the verifier can check
     * both halves: aborted output is invisible to a read_committed reader, and every input is
     * committed exactly once.
     */
    static void eos() throws Exception {
        String app = arg("app", "java");
        String group = arg("group", "enricher-java");
        int abortEvery = argInt("abort-every", 5);
        long start = System.currentTimeMillis();
        long lastRecord = start;

        Properties pp = common();
        pp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "enricher-" + app + "-" + arg("run", "r"));
        pp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        pp.put(ProducerConfig.ACKS_CONFIG, "all");
        pp.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

        KafkaProducer<String, String> p = newTransactionalProducer(pp);
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps(group, "read_committed"))) {
            c.subscribe(List.of(ORDERS));
            int txn = 0;
            Set<String> abortedOnce = new HashSet<>();
            while (!finished(lastRecord, start)) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
                if (recs.isEmpty()) continue;
                lastRecord = System.currentTimeMillis();
                txn++;
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                Map<TopicPartition, Long> firsts = new HashMap<>();
                List<String> srcs = new ArrayList<>();
                List<Future<org.apache.kafka.clients.producer.RecordMetadata>> sends = new ArrayList<>();
                try {
                    p.beginTransaction();
                    for (ConsumerRecord<String, String> r : recs) {
                        Map<String, String> v = parse(r.value());
                        TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                        firsts.putIfAbsent(tp, r.offset());
                        offsets.put(tp, new OffsetAndMetadata(r.offset() + 1));
                        String src = v.get("app") + ":" + v.get("seq");
                        srcs.add(src);
                        String out = "{\"app\":\"" + app + "\",\"src_app\":\"" + v.get("app") + "\",\"src_seq\":" + v.get("seq") + ",\"txn\":" + txn + "}";
                        sends.add(p.send(new ProducerRecord<>(ENRICHED, r.key(), out)));
                    }
                    p.sendOffsetsToTransaction(offsets, c.groupMetadata());
                    String batchId = firsts.toString();
                    boolean abort = abortEvery > 0 && txn % abortEvery == 0 && abortedOnce.add(batchId);
                    if (abort) {
                        p.flush();
                        int sentOk = 0;
                        for (var f : sends) { try { f.get(); sentOk++; } catch (Exception ignored) { } }
                        p.abortTransaction();
                        emit("ev", "txn_aborted", "app", app, "txn", txn, "records", srcs.size(), "sent_ok", sentOk, "srcs", srcs);
                        for (var e : firsts.entrySet()) c.seek(e.getKey(), e.getValue());
                    } else {
                        p.commitTransaction();
                        emit("ev", "txn_committed", "app", app, "txn", txn, "records", srcs.size(), "srcs", srcs);
                    }
                } catch (Exception e) {
                    // The producer's documented contract: an abortable error is aborted and the
                    // input rewound; when the abort itself is refused the producer is unusable
                    // (fenced, fatal, or a commit whose outcome is still pending), so it is
                    // replaced — the new producer's initTransactions fences the old one and
                    // settles its open transaction either way before any new work starts.
                    emit("ev", "txn_error", "app", app, "txn", txn, "error", errName(e));
                    try {
                        p.abortTransaction();
                    } catch (Exception abortFailed) {
                        emit("ev", "producer_recreated", "app", app, "txn", txn, "error", errName(abortFailed));
                        try { p.close(Duration.ofSeconds(5)); } catch (Exception ignored) { }
                        p = newTransactionalProducer(pp);
                    }
                    for (var e2 : firsts.entrySet()) {
                        try { c.seek(e2.getKey(), e2.getValue()); } catch (IllegalStateException notAssigned) { }
                    }
                } finally {
                    // Time spent inside a transaction is not idle time: a coordinator
                    // failover can hold a commit for tens of seconds (~43s measured on
                    // Apache Kafka), and counting that as silence would end the pipeline
                    // with its input rewound and unprocessed.
                    lastRecord = System.currentTimeMillis();
                }
            }
        } finally {
            p.close(Duration.ofSeconds(10));
        }
    }

    static KafkaProducer<String, String> newTransactionalProducer(Properties pp) {
        KafkaProducer<String, String> p = new KafkaProducer<>(pp);
        p.initTransactions();
        return p;
    }

    // ---------------------------------------------------------------- fresh-reader readback

    /**
     * A reader that is not a party to anything above: no group, manual assignment, from the
     * beginning to the end offsets taken at start. It is the only source the verifier trusts for
     * "what is actually in the log" — the group members cannot testify about their own gaps.
     */
    static void readback() throws Exception {
        rb(ORDERS, "read_uncommitted");
        awaitStable(ENRICHED);
        rb(ENRICHED, "read_committed");
        rb(ENRICHED, "read_uncommitted");
    }

    /**
     * Waits until no transaction is holding back what a read_committed reader may see — the
     * last stable offset has caught up with the log end on every partition — or until a deadline
     * well past the default transaction timeout, and records how long that took and what is left.
     *
     * A transactional producer that dies mid-transaction leaves its partitions frozen for
     * read_committed readers until the transaction coordinator aborts it at transaction.timeout.ms
     * (60s by default) — on Apache Kafka and here alike. Reading before that would report the
     * frozen records as missing and blame the broker for the application's crash; reading after
     * it, and finding them still frozen, is a real defect: a hanging transaction.
     */
    static void awaitStable(String topic) throws Exception {
        long deadlineMs = argInt("stable-wait-sec", 150) * 1000L;
        long start = System.currentTimeMillis();
        Properties rc = consumerProps("unused", "read_committed");
        rc.remove(ConsumerConfig.GROUP_ID_CONFIG);
        Properties ru = consumerProps("unused", "read_uncommitted");
        ru.remove(ConsumerConfig.GROUP_ID_CONFIG);
        try (KafkaConsumer<String, String> c1 = new KafkaConsumer<>(rc); KafkaConsumer<String, String> c2 = new KafkaConsumer<>(ru)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c1.partitionsFor(topic, Duration.ofSeconds(30))) tps.add(new TopicPartition(topic, pi.partition()));
            List<Integer> behind = new ArrayList<>();
            long gap = 0;
            while (true) {
                Map<TopicPartition, Long> stable = c1.endOffsets(tps, Duration.ofSeconds(30));
                Map<TopicPartition, Long> end = c2.endOffsets(tps, Duration.ofSeconds(30));
                behind.clear();
                gap = 0;
                for (TopicPartition tp : tps) {
                    long d = end.get(tp) - stable.get(tp);
                    if (d > 0) { behind.add(tp.partition()); gap += d; }
                }
                if (behind.isEmpty() || System.currentTimeMillis() - start > deadlineMs) break;
                Thread.sleep(1000);
            }
            Collections.sort(behind);
            emit("ev", "stable_wait", "topic", topic, "waited_ms", System.currentTimeMillis() - start,
                    "stable", behind.isEmpty(), "partitions_behind", behind, "records_behind", gap,
                    "deadline_ms", deadlineMs);
        }
    }

    static void rb(String topic, String isolation) throws Exception {
        Properties p = consumerProps("unused", isolation);
        p.remove(ConsumerConfig.GROUP_ID_CONFIG);
        long deadline = System.currentTimeMillis() + argInt("timeout-sec", 180) * 1000L;
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (PartitionInfo pi : c.partitionsFor(topic, Duration.ofSeconds(30))) tps.add(new TopicPartition(topic, pi.partition()));
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps, Duration.ofSeconds(30));
            emit("ev", "rb_begin", "topic", topic, "iso", isolation, "partitions", tps.size(),
                    "end_total", end.values().stream().mapToLong(Long::longValue).sum());
            long n = 0;
            while (System.currentTimeMillis() < deadline) {
                boolean done = true;
                for (TopicPartition tp : tps) if (c.position(tp) < end.get(tp)) { done = false; break; }
                if (done) break;
                for (ConsumerRecord<String, String> r : c.poll(Duration.ofMillis(500))) {
                    Map<String, String> v = parse(r.value());
                    n++;
                    emit("ev", "rb", "topic", topic, "iso", isolation, "p", r.partition(), "o", r.offset(), "key", r.key(),
                            "app", v.get("app"), "seq", v.get("seq") == null ? null : Long.parseLong(v.get("seq")),
                            "src_app", v.get("src_app"), "src_seq", v.get("src_seq") == null ? null : Long.parseLong(v.get("src_seq")),
                            "txn", v.get("txn") == null ? null : Long.parseLong(v.get("txn")));
                }
            }
            boolean complete = true;
            for (TopicPartition tp : tps) if (c.position(tp) < end.get(tp)) complete = false;
            emit("ev", "rb_end", "topic", topic, "iso", isolation, "records", n, "complete", complete);
        }
    }

    // ---------------------------------------------------------------- final lag

    /** Committed offset versus log end per partition, for every group this run used. */
    static void lag() throws Exception {
        List<String> groups = List.of(arg("groups", "orders-java,orders-python").split(","));
        try (Admin admin = Admin.create(common());
             KafkaConsumer<String, String> c = new KafkaConsumer<>(consumerProps("lag-probe", "read_uncommitted"))) {
            for (String g : groups) {
                try {
                    Map<TopicPartition, OffsetAndMetadata> committed =
                            admin.listConsumerGroupOffsets(g).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
                    Map<TopicPartition, Long> end = c.endOffsets(committed.keySet(), Duration.ofSeconds(30));
                    for (var e : committed.entrySet()) {
                        emit("ev", "lag", "group", g, "topic", e.getKey().topic(), "p", e.getKey().partition(),
                                "committed", e.getValue() == null ? -1 : e.getValue().offset(), "end", end.get(e.getKey()));
                    }
                    emit("ev", "lag_group", "group", g, "ok", true, "partitions", committed.size());
                } catch (Exception e) {
                    emit("ev", "lag_group", "group", g, "ok", false, "error", errName(e));
                }
            }
        }
    }
}
