module github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/features

go 1.26.0

replace github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol => ../protocol

require (
	github.com/kubemq-io/kubemq-kafka-certification-eco-system/suites/protocol v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.22.0
	github.com/twmb/franz-go/pkg/kadm v1.19.0
	github.com/twmb/franz-go/pkg/kmsg v1.14.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.31 // indirect
)
