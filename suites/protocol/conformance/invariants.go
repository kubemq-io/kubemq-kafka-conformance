package conformance

// InvariantProblems returns the scenario-specific invariants a transcript violates, checked on
// raw (pre-normalization) events. These are absolute properties every correct broker must
// show, independent of the oracle diff. Ported from the kubemq-server legs
// (conformance_test.go, conformance_multipartition_kubemq_test.go,
// conformance_transactions_test.go).
func InvariantProblems(scenario string, tr Transcript) []string {
	var problems []string
	if err := CheckFreshTopicBaseOffsetZero(tr); err != nil {
		problems = append(problems, "absolute baseOffset==0 invariant failed on "+tr.Target+": "+err.Error())
	}
	switch scenario {
	case ScenarioMultiPartition().Name:
		if err := scenarioMultiPartitionCheckKeyOrder(tr.Events); err != nil {
			problems = append(problems, "per-key order invariant violated on "+tr.Target+": "+err.Error())
		}
	case ScenarioTransactions().Name:
		if err := scenarioTransactionsCheckClientFiltering(tr.Events); err != nil {
			problems = append(problems, "read_committed/read_uncommitted client-filtering invariant violated on "+tr.Target+": "+err.Error())
		}
		if err := scenarioTransactionsCheckLSOTripwire(tr.Events); err != nil {
			problems = append(problems, "ListOffsets(latest,read_committed)==LSO tripwire violated on "+tr.Target+": "+err.Error())
		}
	}
	return problems
}

// transactionsOracleInvariantProblems keeps the kubemq-server name used by its unit tests.
func transactionsOracleInvariantProblems(a, b Transcript) []string {
	name := ScenarioTransactions().Name
	return append(InvariantProblems(name, a), InvariantProblems(name, b)...)
}
