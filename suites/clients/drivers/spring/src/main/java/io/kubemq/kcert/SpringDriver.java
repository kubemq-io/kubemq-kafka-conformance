package io.kubemq.kcert;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.SendResult;

/**
 * Spring Kafka driver of the clients track (suites/clients/SCENARIO.md). It uses Spring Kafka's
 * own building blocks as an application does: KafkaAdmin for the topic, KafkaTemplate to send, a
 * KafkaMessageListenerContainer to consume. The container commits offsets itself (Spring's
 * default: auto commit off, synchronous commit after each poll's records, and on stop); the driver
 * reports that commit when the container stops cleanly, and the track's fresh reader checks the
 * committed offsets. The ledger goes to stdout.
 */
public final class SpringDriver {
    static final String CLIENT = "spring-kafka";

    static String bootstrap, mode, topic, group;
    static int partitions, records, records2, keys, timeoutSec;
    static boolean admin;

    public static void main(String[] args) {
        bootstrap = env("KCERT_BOOTSTRAP", "");
        mode = env("KCERT_MODE", "");
        topic = env("KCERT_TOPIC", "");
        group = env("KCERT_GROUP", "");
        partitions = Integer.parseInt(env("KCERT_PARTITIONS", "0"));
        records = Integer.parseInt(env("KCERT_RECORDS", "0"));
        records2 = Integer.parseInt(env("KCERT_RECORDS2", "0"));
        keys = Integer.parseInt(env("KCERT_KEYS", "1"));
        timeoutSec = Integer.parseInt(env("KCERT_TIMEOUT_SEC", "60"));
        admin = !"0".equals(env("KCERT_ADMIN", "1"));
        if (bootstrap.isEmpty() || topic.isEmpty() || group.isEmpty()
                || !(mode.equals("scenario") || mode.equals("stock-defaults"))) {
            fail("inputs", -1, "KCERT_BOOTSTRAP, KCERT_TOPIC, KCERT_GROUP and KCERT_MODE are required");
        }
        Map<String, Object> cprops = consumerProps(mode.equals("scenario"));
        cprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        String protocol = new ConsumerConfig(cprops).getString(ConsumerConfig.GROUP_PROTOCOL_CONFIG);
        String version = springVersion() + " (kafka-clients " + AppInfoParser.getVersion() + ")";
        Ledger.emit("start", "client", CLIENT, "version", version, "mode", mode, "group_protocol", protocol.toLowerCase());
        try {
            if (mode.equals("scenario")) {
                scenario();
            } else {
                stock();
            }
        } catch (Exception e) {
            fail("driver", code(e), String.valueOf(e));
        }
        Ledger.emit("done");
        System.exit(0);
    }

    static String springVersion() {
        String v = KafkaTemplate.class.getPackage().getImplementationVersion();
        return v == null ? "unknown" : v;
    }

    static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null ? def : v;
    }

    static void fail(String step, int code, String msg) {
        Ledger.emit("error", "step", step, "error_code", code, "error", msg);
        Ledger.emit("done");
        System.exit(1);
    }

    static int code(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && !(t instanceof org.apache.kafka.common.errors.ApiException)) {
            t = t.getCause();
        }
        return Errors.forException(t).code();
    }

    static String value(int seq) {
        return String.format("value-%06d", seq);
    }

    static int seqOf(String v) {
        try {
            return v != null && v.startsWith("value-") ? Integer.parseInt(v.substring(6)) : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    static Map<String, Object> consumerProps(boolean scenario) {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        if (scenario) {
            p.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "classic"); // the one allowed deviation
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        return p;
    }

    static void create(int parts, short rf) {
        KafkaAdmin adm = new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
        adm.setFatalIfBrokerNotAvailable(true);
        NewTopic nt = parts > 0 ? new NewTopic(topic, parts, rf) : new NewTopic(topic, java.util.Optional.empty(), java.util.Optional.empty());
        int code = 0;
        String err = "";
        try {
            adm.createOrModifyTopics(nt);
        } catch (Exception e) {
            code = code(e);
            err = String.valueOf(e);
        }
        Ledger.emit("create", "topic", topic, "partitions", parts > 0 ? parts : -1, "error_code", code, "error", err);
        if (code != 0) {
            fail("create", code, err);
        }
        // An application waits until the topic is usable before producing.
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (System.currentTimeMillis() < deadline) {
            Map<String, org.apache.kafka.clients.admin.TopicDescription> d = adm.describeTopics(topic);
            org.apache.kafka.clients.admin.TopicDescription td = d.get(topic);
            if (td != null && !td.partitions().isEmpty() && (parts <= 0 || td.partitions().size() == parts)
                    && td.partitions().stream().allMatch(i -> i.leader() != null && !i.leader().isEmpty())) {
                return;
            }
            sleep(200);
        }
        fail("create", -1, "topic never became ready");
    }

    static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static KafkaTemplate<String, String> template(boolean scenario) {
        Map<String, Object> p = new HashMap<>();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        if (scenario) {
            p.put(ProducerConfig.ACKS_CONFIG, "all");
            p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        }
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(p));
    }

    static void produce(KafkaTemplate<String, String> t, int phase, int lo, int hi, boolean keyed) throws Exception {
        List<java.util.concurrent.CompletableFuture<SendResult<String, String>>> fs = new ArrayList<>();
        for (int seq = lo; seq < hi; seq++) {
            ProducerRecord<String, String> r = keyed
                    ? new ProducerRecord<>(topic, String.format("key-%02d", seq % keys), value(seq))
                    : new ProducerRecord<>(topic, value(seq));
            if (keyed) {
                r.headers().add("seq", Integer.toString(seq).getBytes(StandardCharsets.UTF_8));
                r.headers().add("client", CLIENT.getBytes(StandardCharsets.UTF_8));
            }
            fs.add(t.send(r));
        }
        t.flush();
        for (int i = 0; i < fs.size(); i++) {
            int seq = lo + i;
            try {
                var md = fs.get(i).get(timeoutSec, TimeUnit.SECONDS).getRecordMetadata();
                Ledger.emit("ack", "phase", phase, "seq", seq, "partition", md.partition(), "offset", md.offset(), "error_code", 0);
            } catch (ExecutionException e) {
                Ledger.emit("ack", "phase", phase, "seq", seq, "partition", -1, "offset", -1,
                        "error_code", code(e), "error", String.valueOf(e.getCause()));
            }
        }
    }

    /** A listener container for one consume phase; records go to the ledger. */
    static KafkaMessageListenerContainer<String, String> container(int phase, boolean scenario, Set<Integer> got,
            AtomicBoolean ready) {
        Map<String, Object> cp = consumerProps(scenario);
        DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(cp, new StringDeserializer(), new StringDeserializer());
        ContainerProperties props = new ContainerProperties(topic);
        props.setGroupId(group);
        props.setConsumerRebalanceListener(new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> parts) {
                TreeSet<Integer> ps = new TreeSet<>();
                for (TopicPartition tp : parts) {
                    ps.add(tp.partition());
                }
                Ledger.emit("assigned", "phase", phase, "partitions", new ArrayList<>(ps));
                if (ps.isEmpty()) {
                    return; // an empty assignment is not readiness
                }
                if (!scenario) {
                    // The start position (default reset latest) is resolved here, in the consumer
                    // thread, before anything is sent: the stock-defaults barrier.
                    for (TopicPartition tp : parts) {
                        long at = consumer.position(tp, Duration.ofSeconds(timeoutSec));
                        Ledger.emit("position", "phase", phase, "partition", tp.partition(), "offset", at);
                    }
                }
                ready.set(true);
            }
        });
        props.setMessageListener((MessageListener<String, String>) r -> recv(r, phase, got));
        return new KafkaMessageListenerContainer<>(cf, props);
    }

    static void recv(ConsumerRecord<String, String> r, int phase, Set<Integer> got) {
        int seq = seqOf(r.value());
        got.add(seq);
        List<List<String>> hs = new ArrayList<>();
        for (Header h : r.headers()) {
            hs.add(List.of(h.key(), h.value() == null ? "" : new String(h.value(), StandardCharsets.UTF_8)));
        }
        Ledger.emit("recv", "phase", phase, "seq", seq, "partition", r.partition(), "offset", r.offset(),
                "key", r.key(), "value", r.value(), "headers", hs);
    }

    static void await(Set<Integer> got, int want, String step) {
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (got.size() < want) {
            if (System.currentTimeMillis() > deadline) {
                fail(step, -1, "received " + got.size() + " of " + want + " records before the deadline");
            }
            sleep(50);
        }
    }

    static void consume(int phase, int want, boolean drain) {
        Set<Integer> got = ConcurrentHashMap.newKeySet();
        KafkaMessageListenerContainer<String, String> c = container(phase, true, got, new AtomicBoolean());
        c.start();
        await(got, want, "consume");
        if (drain) {
            sleep(2000);
        }
        c.stop(); // commits the processed offsets (Spring's default) and leaves the group
        Ledger.emit("commit", "phase", phase, "error_code", 0, "error", "");
    }

    static void scenario() throws Exception {
        if (admin) {
            create(partitions, (short) 1);
        }
        KafkaTemplate<String, String> t = template(true);
        produce(t, 1, 0, records, true);
        consume(1, records, false);
        produce(t, 2, records, records + records2, true);
        consume(2, records2, true);
        t.destroy();
    }

    static void stock() throws Exception {
        if (admin) {
            create(0, (short) 0);
        }
        Set<Integer> got = ConcurrentHashMap.newKeySet();
        AtomicBoolean ready = new AtomicBoolean();
        KafkaMessageListenerContainer<String, String> c = container(1, false, got, ready);
        c.start();
        long deadline = System.currentTimeMillis() + timeoutSec * 1000L;
        while (!ready.get()) {
            if (System.currentTimeMillis() > deadline) {
                fail("consume", -1, "consumer was never assigned partitions");
            }
            sleep(50);
        }
        KafkaTemplate<String, String> t = template(false);
        produce(t, 1, 0, records, false);
        t.destroy();
        await(got, records, "consume");
        c.stop();
    }

    /** Minimal JSON-lines writer; the driver needs no JSON library. */
    static final class Ledger {
        static synchronized void emit(String ev, Object... kv) {
            StringBuilder b = new StringBuilder("{\"ev\":").append(json(ev));
            for (int i = 0; i + 1 < kv.length; i += 2) {
                b.append(',').append(json(kv[i])).append(':').append(json(kv[i + 1]));
            }
            System.out.println(b.append('}'));
            System.out.flush();
        }

        static String json(Object o) {
            if (o == null) {
                return "null";
            }
            if (o instanceof Number || o instanceof Boolean) {
                return o.toString();
            }
            if (o instanceof List<?> l) {
                StringBuilder b = new StringBuilder("[");
                for (int i = 0; i < l.size(); i++) {
                    b.append(i > 0 ? "," : "").append(json(l.get(i)));
                }
                return b.append(']').toString();
            }
            String s = o.toString();
            StringBuilder b = new StringBuilder("\"");
            for (char ch : s.toCharArray()) {
                switch (ch) {
                    case '"' -> b.append("\\\"");
                    case '\\' -> b.append("\\\\");
                    case '\n' -> b.append("\\n");
                    case '\r' -> b.append("\\r");
                    case '\t' -> b.append("\\t");
                    default -> {
                        if (ch < 0x20) {
                            b.append(String.format("\\u%04x", (int) ch));
                        } else {
                            b.append(ch);
                        }
                    }
                }
            }
            return b.append('"').toString();
        }
    }

    private SpringDriver() {
    }
}
