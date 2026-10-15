// Chart rendering for the scenario runners: standalone SVG timeline charts
// (observed vs configured, over time) — no external dependencies. The PDF's
// scenario-2 evidence ("configured limits and observed throughput over
// time") is this chart.
package scenarios

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ChartPoint is one (x, y) sample; x is seconds, y a throughput value.
type ChartPoint struct {
	X float64
	Y float64
}

// ChartSeries is one labelled polyline.
type ChartSeries struct {
	Label  string
	Color  string
	Dashed bool
	Points []ChartPoint
}

// WriteTimelineSVG renders series over time into a standalone SVG file.
// Axes auto-scale from the data; the y axis is floored at zero (throughput).
func WriteTimelineSVG(path, title, yLabel string, series []ChartSeries) error {
	const (
		w, h      = 900.0, 380.0
		marginL   = 74.0
		marginR   = 24.0
		marginT   = 46.0
		marginB   = 48.0
		plotW     = w - marginL - marginR
		plotH     = h - marginT - marginB
		gridLines = 5
		xTicks    = 6
	)
	if len(series) == 0 {
		return fmt.Errorf("chart: no series to plot")
	}

	xmin, xmax := infPos, -infPos
	ymin, ymax := 0.0, -infPos
	for _, s := range series {
		for _, p := range s.Points {
			if p.X < xmin {
				xmin = p.X
			}
			if p.X > xmax {
				xmax = p.X
			}
			if p.Y > ymax {
				ymax = p.Y
			}
		}
	}
	if xmax <= xmin {
		xmax = xmin + 1
	}
	if ymax <= ymin {
		ymax = ymin + 1
	}
	ymax *= 1.06 // headroom above the tallest line

	mapX := func(x float64) float64 { return marginL + (x-xmin)/(xmax-xmin)*plotW }
	mapY := func(y float64) float64 { return marginT + (1-(y-ymin)/(ymax-ymin))*plotH }

	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	fmt.Fprintf(&b, "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\" font-family=\"system-ui,Segoe UI,Arial\" font-size=\"13\">\n", int(w), int(h), int(w), int(h))
	fmt.Fprintf(&b, "  <rect width=\"%d\" height=\"%d\" fill=\"#ffffff\"/>\n", int(w), int(h))
	fmt.Fprintf(&b, "  <text x=\"%0.f\" y=\"24\" fill=\"#111827\" font-size=\"16\" font-weight=\"600\">%s</text>\n", marginL, esc(title))

	// horizontal grid + y tick labels
	for i := 0; i <= gridLines; i++ {
		frac := float64(i) / gridLines
		y := marginT + frac*plotH
		val := ymax - frac*(ymax-ymin)
		fmt.Fprintf(&b, "  <line x1=\"%0.f\" y1=\"%0.f\" x2=\"%0.f\" y2=\"%0.f\" stroke=\"#e5e7eb\" stroke-width=\"1\"/>\n", marginL, y, marginL+plotW, y)
		fmt.Fprintf(&b, "  <text x=\"%0.f\" y=\"%0.f\" fill=\"#6b7280\" text-anchor=\"end\">%0.f</text>\n", marginL-6, y+4, val)
	}
	// vertical ticks + x labels
	for i := 0; i <= xTicks; i++ {
		frac := float64(i) / xTicks
		x := marginL + frac*plotW
		val := xmin + frac*(xmax-xmin)
		fmt.Fprintf(&b, "  <line x1=\"%0.f\" y1=\"%0.f\" x2=\"%0.f\" y2=\"%0.f\" stroke=\"#f3f4f6\" stroke-width=\"1\"/>\n", x, marginT, x, marginT+plotH)
		fmt.Fprintf(&b, "  <text x=\"%0.f\" y=\"%0.f\" fill=\"#6b7280\" text-anchor=\"middle\">%0.fs</text>\n", x, marginT+plotH+18, val)
	}
	// axes
	fmt.Fprintf(&b, "  <line x1=\"%0.f\" y1=\"%0.f\" x2=\"%0.f\" y2=\"%0.f\" stroke=\"#9ca3af\" stroke-width=\"1.5\"/>\n", marginL, marginT, marginL, marginT+plotH)
	fmt.Fprintf(&b, "  <line x1=\"%0.f\" y1=\"%0.f\" x2=\"%0.f\" y2=\"%0.f\" stroke=\"#9ca3af\" stroke-width=\"1.5\"/>\n", marginL, marginT+plotH, marginL+plotW, marginT+plotH)
	fmt.Fprintf(&b, "  <text x=\"%0.f\" y=\"%0.f\" fill=\"#374151\" text-anchor=\"middle\">time (s)</text>\n", marginL+plotW/2, h-8)
	fmt.Fprintf(&b, "  <text x=\"%0.f\" y=\"38\" fill=\"#374151\">%s</text>\n", marginL-58, esc(yLabel))

	// series polylines + legend
	legendY := 44.0
	for _, s := range series {
		if len(s.Points) == 0 {
			continue
		}
		var pts strings.Builder
		for i, p := range s.Points {
			if i > 0 {
				pts.WriteByte(' ')
			}
			fmt.Fprintf(&pts, "%0.f,%0.f", mapX(p.X), mapY(p.Y))
		}
		dash := ""
		if s.Dashed {
			dash = " stroke-dasharray=\"6 4\""
		}
		fmt.Fprintf(&b, "  <polyline points=\"%s\" fill=\"none\" stroke=\"%s\" stroke-width=\"2\"%s/>\n", pts.String(), s.Color, dash)

		lx := marginL + plotW - 240
		fmt.Fprintf(&b, "  <line x1=\"%0.f\" y1=\"%0.f\" x2=\"%0.f\" y2=\"%0.f\" stroke=\"%s\" stroke-width=\"2\"%s/>\n", lx, legendY, lx+26, legendY, s.Color, dash)
		fmt.Fprintf(&b, "  <text x=\"%0.f\" y=\"%0.f\" fill=\"#374151\">%s</text>\n", lx+32, legendY+4, esc(s.Label))
		legendY += 20
	}

	b.WriteString("</svg>\n")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

const (
	infPos = 1e18
	infNeg = -1e18
)

func esc(s string) string {
	r := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")
	return r.Replace(s)
}
