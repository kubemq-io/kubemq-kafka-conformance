// TxnDriver is the Java kafka-clients transaction driver (suites/transactions/SCENARIOS.md).
// It runs one scenario against one broker inside a client container and prints one JSON report
// as the last line of standard output: {"client":..., "scenario":..., "facts":{...}}. It reports
// facts only; suites/transactions/verify judges them. Exit 1 means the scenario could not
// complete.
//
// Run with the JDK's source launcher (no build step, nothing written to the host):
//   java -cp '/kcert/lib/*' /kcert/java/TxnDriver.java -bootstrap H:P -scenario isolation -target kubemq
// where /kcert/lib holds kafka-clients and its runtime dependencies copied from the pinned Kafka
// image, so the client is exactly the 4.3.1 jar the oracle ships.

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TxnDriver {
    static String bootstrap, target;

    public static void main(String[] args) throws Exception {
        String scenario = null;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "-bootstrap" -> bootstrap = args[i + 1];
                case "-scenario" -> scenario = args[i + 1];
                case "-target" -> target = args[i + 1];
                default -> { }
            }
        }
        if (bootstrap == null || scenario == null) {
            System.err.println("TxnDriver: need -bootstrap and -scenario");
            System.exit(1);
        }
        Map<String, Object> facts;
        try {
            facts = switch (scenario) {
                case "isolation" -> isolation();
                case "send-offsets" -> sendOffsets();
                case "consume-transform-produce" -> cpp();
                case "fencing" -> fencing();
                case "timeout-abort" -> timeoutAbort();
                case "timeout-limit" -> timeoutLimit();
                default -> throw new IllegalArgumentException("unknown scenario " + scenario);
            };
        } catch (Exception e) {
            System.err.println("TxnDriver: " + e);
            e.printStackTrace();
            System.exit(1);
            return;
        }
        Map<String, Object> rep = new LinkedHashMap<>();
        rep.put("client", "kafka-clients " + AppInfoVersion.get());
        rep.put("scenario", scenario);
        rep.put("facts", facts);
        System.out.println(Json.enc(rep));
        System.exit(0);
    }

    // ---- helpers -----------------------------------------------------------------------------

    static final class AppInfoVersion {
        static String get() { return org.apache.kafka.common.utils.AppInfoParser.getVersion(); }
    }

    static String name(String kind) { return "kcert-txn-" + kind + "-" + target + "-" + System.nanoTime(); }

    static List<String> keys(String prefix, int from, int n) {
        List<String> out = new ArrayList<>();
        for (int i = from; i < from + n; i++) out.add(String.format("%s-%02d", prefix, i));
        return out;
    }

    static String valueFor(String key) { return "v-" + key; }

    /** The Kafka error name of a failure, unwrapping wrappers; "CLIENT:<class>" when none maps. */
    static String errName(Throwable t) {
        if (t == null) return "NONE";
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof ApiException) {
                Errors e = Errors.forException(c);
                if (e != Errors.UNKNOWN_SERVER_ERROR || c instanceof org.apache.kafka.common.errors.UnknownServerException) return e.name();
            }
        }
        // Some failures reach the application as a plain KafkaException whose message is the
        // broker error's message (initTransactions on INVALID_TRANSACTION_TIMEOUT): match it.
        for (Throwable c = t; c != null; c = c.getCause()) {
            String m = c.getMessage();
            if (m == null) continue;
            for (Errors e : Errors.values()) {
                if (e != Errors.NONE && e != Errors.UNKNOWN_SERVER_ERROR && e.message() != null && m.contains(e.message())) return e.name();
            }
        }
        Throwable root = t;
        while ((root instanceof ExecutionException || root.getClass() == KafkaException.class) && root.getCause() != null) root = root.getCause();
        return "CLIENT:" + root.getClass().getSimpleName();
    }

    interface Call { void run() throws Exception; }

    static String outcome(Call c) {
        try { c.run(); return "NONE"; } catch (Exception e) { return errName(e); }
    }

    static Admin admin() {
        Properties p = new Properties();
        p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        return Admin.create(p);
    }

    static void createTopic(Admin adm, String topic, int partitions) throws Exception {
        adm.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get(60, TimeUnit.SECONDS);
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                TopicDescription d = adm.describeTopics(List.of(topic)).allTopicNames().get().get(topic);
                if (d.partitions().size() == partitions && d.partitions().stream().allMatch(x -> x.leader() != null)) return;
            } catch (ExecutionException ignored) { }
            Thread.sleep(200);
        }
        throw new IllegalStateException("topic " + topic + " never became ready");
    }

    static KafkaProducer<String, String> producer(String tid, int txnTimeoutMs) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        if (tid != null) {
            p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, tid);
            p.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, txnTimeoutMs);
        }
        p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        p.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 20_000);
        p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        return new KafkaProducer<>(p);
    }

    static void sendSync(KafkaProducer<String, String> p, String topic, int partition, List<String> ks) throws Exception {
        List<java.util.concurrent.Future<RecordMetadata>> fs = new ArrayList<>();
        for (String k : ks) fs.add(p.send(new ProducerRecord<>(topic, partition, k, valueFor(k))));
        for (var f : fs) f.get(30, TimeUnit.SECONDS);
    }

    /** {lso, hwm} of partition 0. */
    static long[] listOffsets(Admin adm, String topic) throws Exception {
        TopicPartition tp = new TopicPartition(topic, 0);
        long lso = adm.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(IsolationLevel.READ_COMMITTED)).partitionResult(tp).get().offset();
        long hwm = adm.listOffsets(Map.of(tp, OffsetSpec.latest()), new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED)).partitionResult(tp).get().offset();
        return new long[]{lso, hwm};
    }

    /** Reads every partition from the start, group-less, until no record arrived for 4 s. */
    static List<Map<String, Object>> readAll(String topic, int partitions, String isolation) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        List<Map<String, Object>> out = new ArrayList<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int i = 0; i < partitions; i++) tps.add(new TopicPartition(topic, i));
            c.assign(tps);
            c.seekToBeginning(tps);
            long quietSince = System.currentTimeMillis();
            boolean any = false;
            while (System.currentTimeMillis() - quietSince < 4_000 || (!any && System.currentTimeMillis() - quietSince < 10_000)) {
                ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : rs) {
                    Map<String, Object> m = new LinkedHashMap<>();
                    m.put("partition", r.partition());
                    m.put("offset", r.offset());
                    m.put("key", r.key());
                    m.put("value", r.value());
                    out.add(m);
                }
                if (!rs.isEmpty()) { quietSince = System.currentTimeMillis(); any = true; }
            }
        }
        return out;
    }

    // ---- scenarios ---------------------------------------------------------------------------

    static Map<String, Object> isolation() throws Exception {
        Map<String, Object> f = new LinkedHashMap<>();
        Map<String, String> outcomes = new TreeMap<>();
        try (Admin adm = admin()) {
            String topic = name("isolation");
            createTopic(adm, topic, 1);
            try (KafkaProducer<String, String> p = producer(name("isolation-tid"), 60_000)) {
                p.initTransactions();
                p.beginTransaction();
                sendSync(p, topic, 0, keys("c", 0, 6));
                long[] open = listOffsets(adm, topic);
                f.put("open_lso", open[0]);
                f.put("open_hwm", open[1]);
                outcomes.put("commit1", outcome(p::commitTransaction));
                p.beginTransaction();
                sendSync(p, topic, 0, keys("a", 0, 4));
                outcomes.put("abort", outcome(p::abortTransaction));
                p.beginTransaction();
                sendSync(p, topic, 0, keys("c", 6, 3));
                outcomes.put("commit2", outcome(p::commitTransaction));
            }
            Thread.sleep(1000);
            long[] fin = listOffsets(adm, topic);
            f.put("final_lso", fin[0]);
            f.put("final_hwm", fin[1]);
            f.put("outcomes", outcomes);
            f.put("read_committed", readAll(topic, 1, "read_committed"));
            f.put("read_uncommitted", readAll(topic, 1, "read_uncommitted"));
        }
        return f;
    }

    /**
     * The group's committed offset for partition 0, its error name, or "none". With requireStable
     * the admin client itself retries while the broker answers UNSTABLE_OFFSET_COMMIT, until
     * timeoutMs: 15 s gives the settled offset (SCENARIOS.md), 5 s the single fetch while open.
     */
    static String groupOffset(Admin adm, String group, String topic, boolean requireStable, int timeoutMs) {
        TopicPartition tp = new TopicPartition(topic, 0);
        try {
            ListConsumerGroupOffsetsOptions o = new ListConsumerGroupOffsetsOptions().requireStable(requireStable);
            o.timeoutMs(timeoutMs);
            Map<TopicPartition, OffsetAndMetadata> m = adm.listConsumerGroupOffsets(
                    Map.of(group, new ListConsumerGroupOffsetsSpec().topicPartitions(List.of(tp))), o)
                    .partitionsToOffsetAndMetadata(group).get(timeoutMs + 5_000L, TimeUnit.MILLISECONDS);
            OffsetAndMetadata om = m.get(tp);
            return om == null ? "none" : Long.toString(om.offset());
        } catch (Exception e) {
            return errName(e);
        }
    }

    static Map<String, Object> sendOffsets() throws Exception {
        Map<String, Object> f = new LinkedHashMap<>();
        Map<String, String> outcomes = new TreeMap<>();
        try (Admin adm = admin()) {
            String in = name("so-in"), out = name("so-out"), group = name("so-group");
            createTopic(adm, in, 1);
            createTopic(adm, out, 1);
            try (KafkaProducer<String, String> plain = producer(null, 0)) {
                sendSync(plain, in, 0, keys("i", 0, 10));
            }
            TopicPartition tp = new TopicPartition(in, 0);
            try (KafkaProducer<String, String> p = producer(name("so-tid"), 60_000)) {
                p.initTransactions();
                ConsumerGroupMetadata meta = new ConsumerGroupMetadata(group);
                // txn1
                p.beginTransaction();
                sendSync(p, out, 0, keys("o", 0, 5));
                outcomes.put("txn1_send_offsets", outcome(() -> p.sendOffsetsToTransaction(Map.of(tp, new OffsetAndMetadata(5)), meta)));
                outcomes.put("txn1_end", outcome(p::commitTransaction));
                f.put("after_commit1", groupOffset(adm, group, in, true, 15_000));
                // txn2
                p.beginTransaction();
                sendSync(p, out, 0, keys("o", 5, 5));
                outcomes.put("txn2_send_offsets", outcome(() -> p.sendOffsetsToTransaction(Map.of(tp, new OffsetAndMetadata(10)), meta)));
                f.put("stable_fetch_while_open", groupOffset(adm, group, in, true, 5_000));
                outcomes.put("txn2_end", outcome(p::abortTransaction));
                f.put("after_abort", groupOffset(adm, group, in, true, 15_000));
                // txn3
                p.beginTransaction();
                sendSync(p, out, 0, keys("o", 5, 5));
                outcomes.put("txn3_send_offsets", outcome(() -> p.sendOffsetsToTransaction(Map.of(tp, new OffsetAndMetadata(10)), meta)));
                outcomes.put("txn3_end", outcome(p::commitTransaction));
                f.put("after_commit2", groupOffset(adm, group, in, true, 15_000));
            }
            f.put("outcomes", outcomes);
            f.put("read_committed", readAll(out, 1, "read_committed"));
        }
        return f;
    }

    static Map<String, Object> cpp() throws Exception {
        final int inputs = 60, parts = 3;
        Map<String, Object> f = new LinkedHashMap<>();
        String firstError = "NONE";
        int commits = 0, aborts = 0;
        try (Admin adm = admin()) {
            String in = name("cpp-in"), out = name("cpp-out"), group = name("cpp-group");
            createTopic(adm, in, parts);
            createTopic(adm, out, parts);
            try (KafkaProducer<String, String> plain = producer(null, 0)) {
                List<java.util.concurrent.Future<RecordMetadata>> fs = new ArrayList<>();
                for (int n = 0; n < inputs; n++) {
                    String k = String.format("i-%02d", n);
                    fs.add(plain.send(new ProducerRecord<>(in, n % parts, k, valueFor(k))));
                }
                for (var x : fs) x.get(30, TimeUnit.SECONDS);
            }
            Properties cp = new Properties();
            cp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            cp.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            cp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            cp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            cp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5);
            cp.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
            // Allowed deviation (docs/methodology.md): the classic group protocol.
            cp.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic");
            int committedOut = 0, txn = 0;
            try (KafkaConsumer<String, String> c = new KafkaConsumer<>(cp);
                 KafkaProducer<String, String> p = producer(name("cpp-tid"), 60_000)) {
                p.initTransactions();
                c.subscribe(List.of(in));
                long deadline = System.currentTimeMillis() + 120_000;
                while (committedOut < inputs && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> rs = c.poll(Duration.ofMillis(500));
                    if (rs.isEmpty()) continue;
                    Map<TopicPartition, OffsetAndMetadata> offs = new HashMap<>();
                    List<java.util.concurrent.Future<RecordMetadata>> fs = new ArrayList<>();
                    try {
                        p.beginTransaction();
                        for (ConsumerRecord<String, String> r : rs) {
                            String k = "o-" + r.key().substring(2);
                            fs.add(p.send(new ProducerRecord<>(out, r.partition(), k, valueFor(k))));
                            offs.put(new TopicPartition(r.topic(), r.partition()), new OffsetAndMetadata(r.offset() + 1));
                        }
                        for (var x : fs) x.get(30, TimeUnit.SECONDS);
                        p.sendOffsetsToTransaction(offs, c.groupMetadata());
                    } catch (Exception e) {
                        if (firstError.equals("NONE")) firstError = "produce:" + errName(e);
                        break;
                    }
                    txn++;
                    boolean abort = txn % 3 == 0;
                    try {
                        if (abort) {
                            p.abortTransaction();
                            aborts++;
                            // Resume from the group's committed offsets (or the start).
                            Set<TopicPartition> asg = c.assignment();
                            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(asg);
                            for (TopicPartition tp : asg) {
                                OffsetAndMetadata om = committed.get(tp);
                                if (om == null) c.seekToBeginning(List.of(tp)); else c.seek(tp, om.offset());
                            }
                        } else {
                            p.commitTransaction();
                            commits++;
                            committedOut += rs.count();
                        }
                    } catch (Exception e) {
                        if (firstError.equals("NONE")) firstError = "end:" + errName(e);
                        break;
                    }
                }
            }
            Map<String, Object> groupOffsets = new TreeMap<>();
            try {
                Map<TopicPartition, OffsetAndMetadata> m = adm.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(30, TimeUnit.SECONDS);
                for (var e : m.entrySet()) if (e.getKey().topic().equals(in)) groupOffsets.put(Integer.toString(e.getKey().partition()), e.getValue().offset());
            } catch (Exception e) {
                if (firstError.equals("NONE")) firstError = "offset fetch:" + errName(e);
            }
            f.put("commits", commits);
            f.put("aborts", aborts);
            f.put("first_error", firstError);
            f.put("group_offsets", groupOffsets);
            f.put("read_committed", readAll(out, parts, "read_committed"));
            f.put("read_uncommitted", readAll(out, parts, "read_uncommitted"));
        }
        return f;
    }

    static Map<String, Object> fencing() throws Exception {
        Map<String, Object> f = new LinkedHashMap<>();
        Map<String, String> outcomes = new TreeMap<>();
        try (Admin adm = admin()) {
            String topic = name("fence"), tid = name("fence-tid");
            createTopic(adm, topic, 1);
            KafkaProducer<String, String> a = producer(tid, 60_000);
            KafkaProducer<String, String> b = producer(tid, 60_000);
            try {
                a.initTransactions();
                a.beginTransaction();
                sendSync(a, topic, 0, keys("z", 0, 3));
                String bInit = outcome(b::initTransactions);
                outcomes.put("b_init", bInit);
                outcomes.put("a_send_after_fence", outcome(() -> sendSync(a, topic, 0, keys("z", 3, 1))));
                outcomes.put("a_commit_after_fence", outcome(a::commitTransaction));
                if (bInit.equals("NONE")) {
                    b.beginTransaction();
                    sendSync(b, topic, 0, keys("b", 0, 2));
                    outcomes.put("b_commit", outcome(b::commitTransaction));
                }
            } finally {
                a.close(Duration.ofSeconds(5));
                b.close(Duration.ofSeconds(5));
            }
            Thread.sleep(1000);
            long[] fin = listOffsets(adm, topic);
            f.put("final_lso", fin[0]);
            f.put("final_hwm", fin[1]);
            f.put("outcomes", outcomes);
            f.put("read_committed", readAll(topic, 1, "read_committed"));
            f.put("read_uncommitted", readAll(topic, 1, "read_uncommitted"));
        }
        return f;
    }

    static Map<String, Object> timeoutAbort() throws Exception {
        Map<String, Object> f = new LinkedHashMap<>();
        Map<String, String> outcomes = new TreeMap<>();
        try (Admin adm = admin()) {
            String topic = name("timeout"), tid = name("timeout-tid");
            createTopic(adm, topic, 1);
            boolean aborted = false;
            KafkaProducer<String, String> p = producer(tid, 3_000);
            try {
                p.initTransactions();
                p.beginTransaction();
                sendSync(p, topic, 0, keys("t", 0, 3));
                for (int i = 0; i < 45 && !aborted; i++) {
                    Thread.sleep(1000);
                    long[] o = listOffsets(adm, topic);
                    aborted = o[0] == o[1];
                }
                outcomes.put("late_commit", outcome(p::commitTransaction));
            } finally {
                p.close(Duration.ofSeconds(5));
            }
            try (KafkaProducer<String, String> q = producer(tid, 60_000)) {
                q.initTransactions();
                q.beginTransaction();
                String send = outcome(() -> sendSync(q, topic, 0, keys("u", 0, 1)));
                if (!send.equals("NONE")) outcomes.put("new_producer_send", send);
                outcomes.put("new_producer_commit", outcome(q::commitTransaction));
            }
            Thread.sleep(1000);
            long[] fin = listOffsets(adm, topic);
            f.put("aborted_by_broker", aborted);
            f.put("final_lso", fin[0]);
            f.put("final_hwm", fin[1]);
            f.put("outcomes", outcomes);
            f.put("read_committed", readAll(topic, 1, "read_committed"));
            f.put("read_uncommitted", readAll(topic, 1, "read_uncommitted"));
        }
        return f;
    }

    /** initTransactions() is where kafka-clients sends InitProducerId. */
    static Map<String, Object> timeoutLimit() {
        Map<String, String> outcomes = new TreeMap<>();
        for (int ms : new int[]{900_000, 900_001}) {
            String label = ms == 900_000 ? "at_max_900000" : "above_max_900001";
            KafkaProducer<String, String> p = producer(name("limit-" + label), ms);
            try {
                outcomes.put(label, outcome(p::initTransactions));
            } finally {
                p.close(Duration.ofSeconds(5));
            }
        }
        Map<String, Object> f = new LinkedHashMap<>();
        f.put("outcomes", outcomes);
        return f;
    }

    // ---- minimal JSON encoder (maps, lists, strings, numbers, booleans) -----------------------

    static final class Json {
        static String enc(Object o) {
            StringBuilder b = new StringBuilder();
            write(b, o);
            return b.toString();
        }

        @SuppressWarnings("unchecked")
        static void write(StringBuilder b, Object o) {
            if (o == null) b.append("null");
            else if (o instanceof String s) str(b, s);
            else if (o instanceof Number || o instanceof Boolean) b.append(o);
            else if (o instanceof Map<?, ?> m) {
                b.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (!first) b.append(',');
                    first = false;
                    str(b, e.getKey().toString());
                    b.append(':');
                    write(b, e.getValue());
                }
                b.append('}');
            } else if (o instanceof Collection<?> c) {
                b.append('[');
                boolean first = true;
                for (Object x : c) {
                    if (!first) b.append(',');
                    first = false;
                    write(b, x);
                }
                b.append(']');
            } else str(b, o.toString());
        }

        static void str(StringBuilder b, String s) {
            b.append('"');
            for (char ch : s.toCharArray()) {
                switch (ch) {
                    case '"' -> b.append("\\\"");
                    case '\\' -> b.append("\\\\");
                    case '\n' -> b.append("\\n");
                    case '\r' -> b.append("\\r");
                    case '\t' -> b.append("\\t");
                    default -> {
                        if (ch < 0x20) b.append(String.format("\\u%04x", (int) ch));
                        else b.append(ch);
                    }
                }
            }
            b.append('"');
        }
    }
}
