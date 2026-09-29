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
	"font-size": "fontSize", "font-weight": "fontWeight", "text-anchor": "textAnchor", "fill-opacity": "fillOpacity", "font-style": "fontStyle",
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
	// Braces and angle brackets are the only characters JSX text cannot hold literally. They
	// are written as HTML entities, not as {'<'} expressions: the PRD takes a text that carries
	// a text id only as literal text (an expression is refused, rule text-literal).
	r := strings.NewReplacer("&", "&amp;", "{", "&#123;", "}", "&#125;", "<", "&lt;", ">", "&gt;")
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

// pathID is an entry's path as a text id: stable while the structure keeps the link's node
// names and the entry's key.
func pathID(e *Entry) string {
	return strings.NewReplacer("->", "--", "/", "-").Replace(e.Path())
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
	svg := mk("svg", "viewBox", fmt.Sprintf("0 0 %d %d", L.W, L.Height), "role", "img", "aria-label", v.Title, "font-size", "13")
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
		if e.Text == "" {
			continue
		}
		t := mk("text", "x", itoa(e.LabelX), "y", itoa(e.LabelY))
		if e.Anchor != "start" {
			t.attrs = append(t.attrs, attr{"text-anchor", e.Anchor})
		}
		t.attrs = append(t.attrs, attr{"font-size", "11.5"}, attr{"fill", "@quiet"})
		t.text, t.textID = e.Text, "lbl-"+pathID(e.Entries[0])
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
		if b.Node != nil && b.Node.Controller && L.View.Internals != "" {
			g.add(L.compartments(b)...)
		}
		svg.add(g)
	}
	key := mk("g")
	key.anchor = "key"
	x, ky := 24, L.KeyY
	lines := 1
	item := func(sym func(x, y int) *el, symW int, label, id string) {
		if w := symW + 8 + textW(label); x > 24 && x+w > L.W-24 {
			x, ky = 24, ky+22
			lines++
		}
		key.add(sym(x, ky))
		t := mk("text", "x", itoa(x+symW+8), "y", itoa(ky+4), "font-size", "11.5", "fill", "@quiet")
		t.text, t.textID = label, id
		key.add(t)
		x = x + symW + 8 + textW(label) + 36
	}
	if L.HasHuman {
		item(func(x, ky int) *el {
			return mk("rect", "x", itoa(x), "y", itoa(ky-8), "width", "24", "height", "16", "rx", "4", "fill", "@accent", "fill-opacity", "0.12", "stroke", "@accent", "stroke-width", "2")
		}, 24, "people", "key-human")
	}
	item(func(x, ky int) *el {
		return mk("line", "x1", itoa(x), "x2", itoa(x+32), "y1", itoa(ky), "y2", itoa(ky), "stroke", "@edge", "stroke-width", "1.25")
	}, 32, "control action", "key-control")
	item(func(x, ky int) *el {
		return mk("line", "x1", itoa(x), "x2", itoa(x+32), "y1", itoa(ky), "y2", itoa(ky), "stroke", "@edge", "stroke-width", "1.25", "stroke-dasharray", "5 4")
	}, 32, "feedback", "key-feedback")
	if L.HasProcess {
		item(func(x, ky int) *el {
			return mk("rect", "x", itoa(x), "y", itoa(ky-8), "width", "24", "height", "16", "rx", "4", "fill", "@tint", "stroke", "@edge", "stroke-width", "1.25")
		}, 24, "controlled process (store)", "key-process")
	}
	if L.View.Internals != "" {
		item(func(x, ky int) *el {
			return mk("g").add(mk("rect", "x", itoa(x), "y", itoa(ky-8), "width", "11", "height", "16", "rx", "3", "fill", "@tint"),
				mk("rect", "x", itoa(x+13), "y", itoa(ky-8), "width", "11", "height", "16", "rx", "3", "fill", "@tint"))
		}, 24, "control algorithm | process model (a detail diagram per controller)", "key-internals")
	}
	if lines > 1 {
		L.Height += 22 * (lines - 1)
		svg.attrs[0].v = fmt.Sprintf("0 0 %d %d", L.W, L.Height)
	}
	svg.add(key)
	return svg
}

// compartments draws a controller box's two compartments at its bottom: the control algorithm
// (left, where control leaves) and the process model (right, where feedback arrives). With
// Internals "headers" they carry only their name and size (the detail diagram has the rest);
// with "full", the rules' names and the variables' names.
func (L *Layout) compartments(b *Box) []*el {
	c := b.Rec
	rules, vars := algoOf(c), pmOf(c)
	var sh int
	if L.View.Internals == "headers" {
		sh = stripHeaders
	} else {
		sh = 26 + 13*maxi(len(rules), len(vars))
	}
	top := b.y2() - sh
	mid := b.X + b.W/2
	var out []*el
	text := func(x, y int, s, id string, attrs ...string) *el {
		t := mk("text", append([]string{"x", itoa(x), "y", itoa(y)}, attrs...)...)
		t.text, t.textID = s, tid(c, id)
		return t
	}
	for _, half := range []struct{ x1, x2 int }{{b.X + 5, mid - 2}, {mid + 2, b.x2() - 5}} {
		out = append(out, mk("rect", "x", itoa(half.x1), "y", itoa(top), "width", itoa(half.x2-half.x1), "height", itoa(sh-5), "rx", "5", "fill", "@tint"))
	}
	if L.View.Internals == "headers" {
		// The compartment names when both fit their half; in a narrow box only the sizes,
		// "4 rules | 8 variables" (the key names the compartments).
		if float64(len("PROCESS MODEL"))*stripCapW <= float64(b.x2()-19-mid) {
			out = append(out, text(b.X+11, top+16, "ALGORITHM", "ca", "font-size", stripFont, "font-weight", "600", "fill", "@quiet"))
			out = append(out, text(mid+8, top+16, "PROCESS MODEL", "pm", "font-size", stripFont, "font-weight", "600", "fill", "@quiet"))
			out = append(out, text(b.X+11, top+32, plural(len(rules), "rule"), "ca-n", "font-size", stripFont, "fill", "@ink"))
			out = append(out, text(mid+8, top+32, plural(len(vars), "variable"), "pm-n", "font-size", stripFont, "fill", "@ink"))
			return out
		}
		out = append(out, text(b.X+11, top+24, plural(len(rules), "rule"), "ca-n", "font-size", stripFont, "fill", "@ink"))
		out = append(out, text(mid+8, top+24, plural(len(vars), "variable"), "pm-n", "font-size", stripFont, "fill", "@ink"))
		return out
	}
	out = append(out, text(b.X+11, top+13, "ALGORITHM", "ca", "font-size", "9.5", "font-weight", "600", "fill", "@quiet"))
	out = append(out, text(mid+8, top+13, "PROCESS MODEL", "pm", "font-size", "9.5", "font-weight", "600", "fill", "@quiet"))
	room := int(float64(mid-b.X-16) / 5.7)
	for i, r := range rules {
		out = append(out, text(b.X+11, top+27+13*i, clipTo("▸ "+fmt.Sprint(r["name"]), room), "ca-"+ruleSlug(fmt.Sprint(r["name"])), "font-size", "10.5", "fill", "@ink"))
	}
	for i, v := range vars {
		out = append(out, text(mid+8, top+27+13*i, clipTo(fmt.Sprint(v["name"]), room), fmt.Sprintf("pm%d", i+1), "font-size", "10.5", "fill", "@ink"))
	}
	return out
}

// The "headers" strip: its labels are read in the PRD, where a 760-wide widget shows at about
// 0.88 of its size (a 672 px column), so 12 lands at 10.5 px, the PRD's minimum.
const (
	stripFont    = "12"
	stripHeaders = 44
	stripCapW    = 8.4 // average advance of a 12 px semibold capital
)

func plural(n int, w string) string {
	if n == 1 {
		return "1 " + w
	}
	return fmt.Sprintf("%d %ss", n, w)
}

func clipTo(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// SVG renders the view as a standalone SVG file: GitHub shows it as an image, light or dark
// from the reader's colour scheme; its own background keeps it legible on either page theme.
func (L *Layout) SVG() string {
	root := L.diagram("arrow")
	root.attrs = append([]attr{{"xmlns", "http://www.w3.org/2000/svg"}, {"width", itoa(L.W)}, {"height", itoa(L.Height)}}, root.attrs...)
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

// prdPrelude opens a PRD widget module: the colour roles as the document's theme tokens.
const prdPrelude = "export default () => { const edge = 'var(--cds-chart-axis)', tint = 'var(--cds-chart-reference-tint)', accent = 'var(--cds-chart-categorical-1)', ink = 'var(--cds-text-primary)', quiet = 'var(--cds-text-secondary)'; return "

// PRD renders the view as the PRD's widget code: JSX in SVG, the document's theme tokens,
// one data-claude-text-id per text so the document can address each string.
func (L *Layout) PRD() string {
	var b strings.Builder
	b.WriteString(prdPrelude)
	L.diagram(L.View.Name + "-arrow").writeJSX(&b)
	b.WriteString("; };\n")
	return b.String()
}
