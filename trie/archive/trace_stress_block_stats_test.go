package archive

import (
	"reflect"
	"testing"
)

func TestBlockDistributionIncludesZeroBlocksAndDeduplicatedProofBytes(t *testing.T) {
	counts := []int64{0, 0, 2, 4}
	proofBytes := []int64{0, 0, 100, 300}
	positiveCounts := []int64{2, 4}
	positiveBytes := []int64{100, 300}
	got := blockDistributionColumns(counts, proofBytes, positiveCounts, positiveBytes, 3)
	want := []string{
		"4", "1.500000", "2", "4", "4",
		"100.000000", "100", "300", "300",
		"2", "3.000000", "4", "4", "4",
		"200.000000", "300", "300", "300",
		"3", "400",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("block columns = %v, want %v", got, want)
	}
	all := blockDistributionSummary(counts, proofBytes)
	positive := blockDistributionSummary(positiveCounts, positiveBytes)
	if all["blocks"] != 4 || all["redemptions_mean"] != 1.5 || all["unique_proof_bytes_mean"] != 100.0 || all["unique_proof_bytes_total"] != int64(400) {
		t.Fatalf("all-block summary = %v", all)
	}
	if positive["blocks"] != 2 || positive["redemptions_mean"] != 3.0 || positive["unique_proof_bytes_mean"] != 200.0 {
		t.Fatalf("positive-block summary = %v", positive)
	}
}
