package natstransport

import "strings"

const (
	defaultPrefix = "toc"
	observeInfix  = "observe"
	diagnoseInfix = "diagnose"
)

func observationSubject(prefix, pipelineID string) string {
	return prefix + "." + observeInfix + "." + pipelineID
}

func diagnosisSubject(prefix, pipelineID string) string {
	return prefix + "." + diagnoseInfix + "." + pipelineID
}

func allObservationSubject(prefix string) string {
	return prefix + "." + observeInfix + ".*"
}

func allDiagnosisSubject(prefix string) string {
	return prefix + "." + diagnoseInfix + ".*"
}

// pipelineFromSubject extracts the last token from a NATS subject.
// For "toc.observe.myPipeline", returns "myPipeline".
func pipelineFromSubject(subject string) string {
	if i := strings.LastIndexByte(subject, '.'); i >= 0 {
		return subject[i+1:]
	}
	return subject
}
