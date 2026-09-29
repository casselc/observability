package stpa

import (
	"fmt"
	"html"
	"strings"
)

// A tiny element tree, emitted two ways: as a standalone SVG file for the repository (colours
// through CSS classes and variables, light and dark) and as the PRD widget (JSX in SVG, the
// document's theme tokens, a data-claude-text-id on every text).

type attr struct{ k, v string }

type el struct {
	tag    string
	attrs  []attr
	kids   []*el
	text   string
	textID string // PRD only
	anchor string // PRD only: data-claude-anchor on a group
}

func mk(tag string, kv ...string) *el {
	e := &el{tag: tag}
	for i := 0; i+1 < len(kv); i += 2 {
		e.attrs = append(e.attrs, attr{kv[i], kv[i+1]})
	}
	return e
}

func (e *el) add(k ...*el) *el { e.kids = append(e.kids, k...); return e }

// Colour roles. In attributes a role is written "@ink" etc.
var roles = []string{"edge", "tint", "ink", "quiet", "accent"}

// PRD theme tokens, as the PRD's own widgets use them.
var prdToken = map[string]string{
	"edge": "var(--cds-chart-axis)", "tint": "var(--cds-chart-reference-tint)",
	"ink": "var(--cds-text-primary)", "quiet": "var(--cds-text-secondary)",
	"accent": "var(--cds-chart-categorical-1)",
}

const repoStyle = `svg{--bg:#ffffff;--edge:#6e7781;--tint:#eef1f4;--ink:#1f2328;--quiet:#59636e;--accent:#0969da}
@media (prefers-color-scheme:dark){svg{--bg:#0d1117;--edge:#8b949e;--tint:#1c2128;--ink:#e6edf3;--quiet:#9198a1;--accent:#4493f8}}
.bg{fill:var(--bg)}
.f-edge{fill:var(--edge)}.f-tint{fill:var(--tint)}.f-ink{fill:var(--ink)}.f-quiet{fill:var(--quiet)}.f-accent{fill:var(--accent)}.f-none{fill:none}
.s-edge{stroke:var(--edge)}.s-accent{stroke:var(--accent)}`

var jsxName = map[string]string{
	"stroke-width": "strokeWidth", "stroke-dasharray": "strokeDasharray", "marker-end": "markerEnd",
	"font-size": "fontSize", "font-weight": "fontWeight", "text-anchor": "textAnchor", "fill-opacity": "fillOpacity",
	"aria-label": "aria-label", "class": "className",
}

func (e *el) writeSVG(b *strings.Builder, ind string) {
	b.WriteString(ind + "<" + e.tag)
	var classes []string
	for _, a := range e.attrs {
		if strings.HasPrefix(a.v, "@") {
			prefix := "f-"
			if a.k == "stroke" {
				prefix = "s-"
			}
			classes = append(classes, prefix+a.v[1:])
			continue
		}
		if a.k == "fill" && a.v == "none" {
			classes = append(classes, "f-none")
			continue
		}
		fmt.Fprintf(b, ` %s="%s"`, a.k, html.EscapeString(a.v))
	}
	if len(classes) > 0 {
		fmt.Fprintf(b, ` class="%s"`, strings.Join(classes, " "))
	}
	switch {
	case e.text != "":
		b.WriteString(">" + html.EscapeString(e.text) + "</" + e.tag + ">\n")
	case len(e.kids) == 0:
		b.WriteString("/>\n")
	default:
		b.WriteString(">\n")
		for _, k := range e.kids {
			k.writeSVG(b, ind+"  ")
		}
		b.WriteString(ind + "</" + e.tag + ">\n")
	}
}

func jsxText(s string) string {
	// Braces and angle brackets are the only characters JSX text cannot hold literally.
	r := strings.NewReplacer("{", "{'{'}", "}", "{'}'}", "<", "{'<'}", ">", "{'>'}")
	return r.Replace(s)
}

func (e *el) writeJSX(b *strings.Builder) {
	b.WriteString("<" + e.tag)
	if e.anchor != "" {
		fmt.Fprintf(b, " data-claude-anchor='%s'", e.anchor)
	}
	if e.textID != "" {
		fmt.Fprintf(b, " data-claude-text-id='%s'", e.textID)
	}
	for _, a := range e.attrs {
		k := a.k
		if n, ok := jsxName[k]; ok {
			k = n
		}
		if strings.HasPrefix(a.v, "@") {
			fmt.Fprintf(b, " %s={%s}", k, a.v[1:])
			continue
		}
		fmt.Fprintf(b, " %s='%s'", k, strings.ReplaceAll(a.v, "'", "&apos;"))
	}
	switch {
	case e.text != "":
		b.WriteString(">" + jsxText(e.text) + "</" + e.tag + ">")
	case len(e.kids) == 0:
		b.WriteString("/>")
	default:
		b.WriteString(">")
		for _, k := range e.kids {
			k.writeJSX(b)
		}
		b.WriteString("</" + e.tag + ">")
	}
}

func itoa(i int) string { return fmt.Sprint(i) }

func pathD(pts [][2]int) string {
	var b strings.Builder
	for i, p := range pts {
		if i == 0 {
			fmt.Fprintf(&b, "M%d %d", p[0], p[1])
			continue
		}
		prev := pts[i-1]
		switch {
		case p[0] == prev[0]:
			fmt.Fprintf(&b, "V%d", p[1])
		case p[1] == prev[1]:
			fmt.Fprintf(&b, "H%d", p[0])
		default:
			fmt.Fprintf(&b, "L%d %d", p[0], p[1])
		}
	}
	return b.String()
}

// short id for text ids: the record id's hex part is stable across relabels.
func tid(r *Record, suffix string) string {
	return r.ID + "-" + suffix
}

// diagram builds the element tree of a placed view. markerID must be unique in the PRD page.
func (L *Layout) diagram(markerID string) *el {
	v := L.View
	svg := mk("svg", "viewBox", fmt.Sprintf("0 0 %d %d", canvasW, L.Height), "role", "img", "aria-label", v.Title, "font-size", "13")
	defs := mk("defs").add(mk("marker", "id", markerID, "viewBox", "0 0 10 10", "refX", "9", "refY", "5",
		"markerWidth", "6", "markerHeight", "6", "orient", "auto-start-reverse").add(mk("path", "d", "M0 0L10 5L0 10z", "fill", "@edge")))
	svg.add(defs)
	title := mk("text", "x", "24", "y", "34", "font-size", "15", "font-weight", "600", "fill", "@ink")
	title.text, title.textID = v.Title, "title"
	svg.add(title)
	ctl := mk("g", "fill", "none", "stroke", "@edge", "stroke-width", "1.25")
	ctl.anchor = "control-actions"
	fb := mk("g", "fill", "none", "stroke", "@edge", "stroke-width", "1.25", "stroke-dasharray", "5 4")
	fb.anchor = "feedback"
	var labels []*el
	for _, e := range L.Edges {
		pe := mk("path", "d", pathD(e.Path), "marker-end", "url(#"+markerID+")")
		if e.Kind == "action" {
			ctl.add(pe)
		} else {
			fb.add(pe)
		}
		if e.Label == "" {
			continue
		}
		t := mk("text", "x", itoa(e.LabelX), "y", itoa(e.LabelY))
		if e.Anchor != "start" {
			t.attrs = append(t.attrs, attr{"text-anchor", e.Anchor})
		}
		t.attrs = append(t.attrs, attr{"font-size", "11.5"}, attr{"fill", "@quiet"})
		t.text, t.textID = e.Label, "lbl-"+e.Records[0].ID
		labels = append(labels, t)
	}
	svg.add(ctl, fb)
	svg.add(labels...)
	for _, b := range L.Boxes {
		g := mk("g")
		g.anchor = b.Rec.ID
		fill, stroke, sw := "none", "@edge", "1.25"
		rect := mk("rect", "x", itoa(b.X), "y", itoa(b.Y), "width", itoa(b.W), "height", itoa(b.H), "rx", "8")
		switch {
		case b.Human:
			fill, stroke, sw = "@accent", "@accent", "2"
			rect.attrs = append(rect.attrs, attr{"fill", fill}, attr{"fill-opacity", "0.12"})
		case b.Process:
			rect.attrs = append(rect.attrs, attr{"fill", "@tint"})
		default:
			rect.attrs = append(rect.attrs, attr{"fill", fill})
		}
		rect.attrs = append(rect.attrs, attr{"stroke", stroke}, attr{"stroke-width", sw})
		g.add(rect)
		name := mk("text", "x", itoa(b.X+12), "y", itoa(b.Y+24), "font-weight", "600", "fill", "@ink")
		name.text, name.textID = b.Rec.S("title"), tid(b.Rec, "name")
		g.add(name)
		for i, line := range b.Lines {
			y := b.Y + 44 + 16*i
			if len(b.Lines) == 1 {
				y = b.Y + 42
			}
			colour := "@quiet"
			if b.Human {
				colour = "@ink"
			}
			t := mk("text", "x", itoa(b.X+12), "y", itoa(y), "font-size", "11.5", "fill", colour)
			t.text, t.textID = line, tid(b.Rec, fmt.Sprintf("l%d", i+1))
			g.add(t)
		}
		svg.add(g)
	}
	key := mk("g")
	key.anchor = "key"
	x, ky := 24, L.KeyY
	item := func(sym *el, symW int, label, id string) {
		key.add(sym)
		t := mk("text", "x", itoa(x+symW+8), "y", itoa(ky+4), "font-size", "11.5", "fill", "@quiet")
		t.text, t.textID = label, id
		key.add(t)
		x = x + symW + 8 + textW(label) + 36
	}
	if L.HasHuman {
		item(mk("rect", "x", itoa(x), "y", itoa(ky-8), "width", "24", "height", "16", "rx", "4", "fill", "@accent", "fill-opacity", "0.12", "stroke", "@accent", "stroke-width", "2"), 24, "people", "key-human")
	}
	item(mk("line", "x1", itoa(x), "x2", itoa(x+32), "y1", itoa(ky), "y2", itoa(ky), "stroke", "@edge", "stroke-width", "1.25"), 32, "control action", "key-control")
	item(mk("line", "x1", itoa(x), "x2", itoa(x+32), "y1", itoa(ky), "y2", itoa(ky), "stroke", "@edge", "stroke-width", "1.25", "stroke-dasharray", "5 4"), 32, "feedback", "key-feedback")
	if L.HasProcess {
		item(mk("rect", "x", itoa(x), "y", itoa(ky-8), "width", "24", "height", "16", "rx", "4", "fill", "@tint", "stroke", "@edge", "stroke-width", "1.25"), 24, "controlled process (store)", "key-process")
	}
	svg.add(key)
	return svg
}

// SVG renders the view as a standalone SVG file: GitHub shows it as an image, light or dark
// from the reader's colour scheme; its own background keeps it legible on either page theme.
func (L *Layout) SVG() string {
	root := L.diagram("arrow")
	root.attrs = append([]attr{{"xmlns", "http://www.w3.org/2000/svg"}, {"width", itoa(canvasW)}, {"height", itoa(L.Height)}}, root.attrs...)
	root.attrs = append(root.attrs, attr{"font-family", "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"})
	style := &el{tag: "style", text: "\n" + repoStyle + "\n"}
	bg := mk("rect", "width", "100%", "height", "100%", "rx", "12", "class", "bg")
	root.kids = append([]*el{style, bg}, root.kids...)
	var b strings.Builder
	fmt.Fprintf(&b, "<!-- Generated by otel-chdb/stpa/tools from the records (view %s: %s). Do not edit. -->\n", L.View.ID, L.View.Name)
	root.writeSVG(&b, "")
	// The style text must not be HTML-escaped beyond what XML needs; it holds no < or &.
	return strings.Replace(b.String(), "&#39;", "'", -1)
}

// PRD renders the view as the PRD's widget code: JSX in SVG, the document's theme tokens,
// one data-claude-text-id per text so the document can address each string.
func (L *Layout) PRD() string {
	var b strings.Builder
	b.WriteString("export default () => { const edge = 'var(--cds-chart-axis)', tint = 'var(--cds-chart-reference-tint)', accent = 'var(--cds-chart-categorical-1)', ink = 'var(--cds-text-primary)', quiet = 'var(--cds-text-secondary)'; return ")
	L.diagram(L.View.Name + "-arrow").writeJSX(&b)
	b.WriteString("; };\n")
	return b.String()
}
