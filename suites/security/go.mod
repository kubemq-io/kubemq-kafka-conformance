module github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/security

go 1.26.0

replace (
	github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features => ../features
	github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol => ../protocol
)

require (
	github.com/IBM/sarama v1.61.0
	github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features v0.0.0-00010101000000-000000000000
	github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.22.0
	github.com/twmb/franz-go/pkg/kadm v1.19.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.31 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20250401214520-65e299d6c5c9 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.14.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)
