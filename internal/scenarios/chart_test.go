package scenarios

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWriteTimelineSVG: the chart writer must produce a standalone SVG with
// one polyline per series and the series labels in the legend.
func TestWriteTimelineSVG(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out", "chart.svg")
	observed := ChartSeries{Label: "observed rpm", Color: "#2563eb"}
	configured := ChartSeries{Label: "configured limit", Color: "#dc2626", Dashed: true}
	for i := 0; i <= 10; i++ {
		x := float64(i)
		observed.Points = append(observed.Points, ChartPoint{X: x, Y: 100 + float64(i)})
		configured.Points = append(configured.Points, ChartPoint{X: x, Y: 150})
	}
	if err := WriteTimelineSVG(path, "throughput over time", "requests/second", []ChartSeries{observed, configured}); err != nil {
		t.Fatalf("WriteTimelineSVG: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	svg := string(raw)
	for _, want := range []string{
		"<?xml version=\"1.0\"", "<svg ", "</svg>",
		"throughput over time", "requests/second",
		"observed rpm", "configured limit",
		"stroke=\"#2563eb\"", "stroke=\"#dc2626\"", "stroke-dasharray=\"6 4\"",
		"time (s)",
	} {
		if !strings.Contains(svg, want) {
			t.Fatalf("svg missing %q", want)
		}
	}
	if n := strings.Count(svg, "<polyline"); n != 2 {
		t.Fatalf("want 2 polylines, got %d", n)
	}
}

// TestWriteTimelineSVGEmptySeries: no series is a caller error, not a panic
// or an empty file.
func TestWriteTimelineSVGEmptySeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "chart.svg")
	if err := WriteTimelineSVG(path, "t", "y", nil); err == nil {
		t.Fatal("empty series must error")
	}
}
