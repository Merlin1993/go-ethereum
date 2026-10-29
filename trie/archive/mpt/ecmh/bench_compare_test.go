package ecmh

import "testing"

var benchCommit Commitment
var benchPoint []byte

func benchmarkItems() [][]byte {
    out := make([][]byte, 100)
    for i := range out {
        item := make([]byte, 64)
        for j := range item { item[j] = byte(i*17 + j) }
        out[i] = item
    }
    return out
}

func BenchmarkCompareMapping(b *testing.B) {
    item := benchmarkItems()[0]
    b.ReportAllocs()
    for i := 0; i < b.N; i++ { benchPoint = HashToCurve(item) }
}

func BenchmarkCompareVerify100(b *testing.B) {
    items := benchmarkItems()
    commitment := Create(items)
    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        if !Verify(commitment, items) { b.Fatal("verification failed") }
    }
}

func BenchmarkCompareAdd1(b *testing.B) {
    items := benchmarkItems()
    commitment := Create(items)
    b.ReportAllocs()
    b.ResetTimer()
    for i := 0; i < b.N; i++ { benchCommit = BlindAppend(commitment, items[0]) }
}
