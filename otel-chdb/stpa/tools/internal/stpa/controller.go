package stpa

import (
	"fmt"
	"sort"
	"strings"
)

// A controller's detail diagram, in the STPA Handbook's form: the controller is a box with two
// compartments, its control algorithm (one row per rule: its name, what it decides; the actions
// it issues; then its condition) and its process model (one row per variable: its name, the feedback that updates
// it, its meaning). Control arrows start at the rule that issues them and run down the left
// side to the controlled node; feedback arrows rise on the right side and end at each
// variable they update. The controllers above it, if any, are drawn above with their links.
// Everything comes from the controller record and the structure.

const (
	dW       = 760
	laneGap  = 12
	rowPad   = 8
	lineH    = 15
	headH    = 22
	nodeH    = 44
	nodeGapY = 48
)

// wrapWords splits s into lines of at most width pixels of the quiet type, at spaces.
func wrapWords(s string, width int) []string { return wrapChars(s, int(float64(width)/charW)) }

// nameCharW is the average advance of a 13 px semibold name.
const nameCharW = 7.6

// wrapChars splits s into lines of at most max characters, at spaces; a word longer than a
// line stays whole on a line of its own.
func wrapChars(s string, max int) []string {
	var lines []string
	for len([]rune(s)) > max {
		r := []rune(s)
		cut := strings.LastIndex(string(r[:max+1]), " ")
		if cut <= 0 {
			if cut = strings.Index(s, " "); cut <= 0 {
				break
			}
		}
		lines = append(lines, strings.TrimSpace(s[:cut]))
		s = strings.TrimSpace(s[cut:])
	}
	if s != "" {
		lines = append(lines, s)
	}
	return lines
}

type detailRow struct {
	y, h  int
	lines []*el
}

// textRow is a compartment row's text, laid out once; rows are then stacked in whichever
// order crosses least.
type textRow struct {
	id    string // text id of the head; the other lines' ids extend it (PRD)
	head  string
	sub   string   // a secondary line under the head (a rule's issued actions), or ""
	lines []string // quiet lines
	extra []string // italic lines
	h     int
}

func (t textRow) nLines() int {
	n := 1 + len(t.lines) + len(t.extra)
	if t.sub != "" {
		n++
	}
	return n
}

func (t textRow) at(x, y int) detailRow {
	r := detailRow{y: y, h: t.h}
	mkT := func(yy int, s, id string, attrs ...string) *el {
		e := mk("text", append([]string{"x", itoa(x), "y", itoa(yy)}, attrs...)...)
		e.text, e.textID = s, id
		return e
	}
	r.lines = append(r.lines, mkT(y+12, t.head, t.id, "font-weight", "600", "font-size", "12", "fill", "@ink"))
	k := 1
	if t.sub != "" {
		r.lines = append(r.lines, mkT(y+12+lineH, t.sub, t.id+"-s", "font-size", "11.5", "fill", "@ink"))
		k++
	}
	for j, l := range t.lines {
		r.lines = append(r.lines, mkT(y+12+lineH*(k+j), l, fmt.Sprintf("%s-l%d", t.id, j+1), "font-size", "11.5", "fill", "@quiet"))
	}
	for j, l := range t.extra {
		r.lines = append(r.lines, mkT(y+12+lineH*(k+len(t.lines)+j), l, fmt.Sprintf("%s-x%d", t.id, j+1), "font-size", "11.5", "font-style", "italic", "fill", "@quiet"))
	}
	return r
}

type ctlPath struct {
	rule   int
	target *Node
	labels []string
}

// detailPlan is one arrangement of a detail diagram: the order of the rules, of the feedback
// entries (and so of the variables), and of the lower nodes; and the paths that follow.
type detailPlan struct {
	ruleOrder, fbOrder []int
	nodeOrder          []*Node
	caRows, pmRows     map[int]int // rule / variable index -> row y
	boxBottom          int
	nodesTop           int
	lowerBoxes         map[*Node][4]int
	ctl, fb            [][][2]int // polylines; fb includes the branches into the variables
	crossings          int
}

func permutations(n int) [][]int {
	if n == 0 {
		return [][]int{{}}
	}
	var out [][]int
	for _, p := range permutations(n - 1) {
		for i := len(p); i >= 0; i-- {
			q := append(append(append([]int{}, p[:i]...), n-1), p[i:]...)
			out = append(out, q)
		}
	}
	return out
}

// crossings counts the points where a horizontal segment of one path crosses a vertical
// segment of another, strictly inside both.
func crossings(paths [][][2]int) int {
	type seg struct {
		a, b [2]int
		p    int
	}
	var hs, vs []seg
	for i, pl := range paths {
		for j := 1; j < len(pl); j++ {
			s := seg{pl[j-1], pl[j], i}
			if s.a[1] == s.b[1] && s.a[0] != s.b[0] {
				hs = append(hs, s)
			} else if s.a[0] == s.b[0] && s.a[1] != s.b[1] {
				vs = append(vs, s)
			}
		}
	}
	n := 0
	for _, h := range hs {
		for _, v := range vs {
			if h.p == v.p {
				continue
			}
			x, y := v.a[0], h.a[1]
			if x > mini(h.a[0], h.b[0]) && x < maxi(h.a[0], h.b[0]) && y > mini(v.a[1], v.b[1]) && y < maxi(v.a[1], v.b[1]) {
				n++
			}
		}
	}
	return n
}

// DetailDiagram builds the detail diagram of one controller node.
func (p *Project) DetailDiagram(n *Node, markerID string) (*el, int) {
	s := p.Structure
	c := n.Rec
	var lowers, uppers []*Node
	var down, up []*Link
	for _, l := range s.Links {
		if l.Upper == n {
			down = append(down, l)
			lowers = append(lowers, l.Lower)
		}
		if l.Lower == n {
			up = append(up, l)
			uppers = append(uppers, l.Upper)
		}
	}
	rules := algoOf(c)
	vars := pmOf(c)
	var cps []ctlPath
	for i, r := range rules {
		seen := map[*Node]int{}
		for _, path := range strList(r["issues"]) {
			e := s.Entries[path]
			if e == nil {
				continue
			}
			if j, ok := seen[e.Link.Lower]; ok {
				cps[j].labels = append(cps[j].labels, e.Label)
				continue
			}
			seen[e.Link.Lower] = len(cps)
			cps = append(cps, ctlPath{i, e.Link.Lower, []string{e.Label}})
		}
	}
	var fbs []*Entry
	for _, l := range down {
		fbs = append(fbs, l.Feedback...)
	}
	nLeft, nRight := len(cps), len(fbs)
	boxL := 24 + laneGap*(nLeft+1)
	boxR := dW - 24 - laneGap*(nRight+1)
	mid := (boxL + boxR) / 2
	caL, caR := boxL+12, mid-8
	pmL, pmR := mid+8, boxR-12

	// Text of every row, independent of the order.
	ruleText := make([]textRow, len(rules))
	for i, r := range rules {
		var acts []string
		for _, cp := range cps {
			// Two links may carry the same label (config to the edge and to the entity
			// controllers): the line names it once, the arrows show where it goes.
			if l := strings.Join(cp.labels, ", "); cp.rule == i && !contains(acts, l) {
				acts = append(acts, l)
			}
		}
		t := textRow{id: tid(c, "rule-"+ruleSlug(fmt.Sprint(r["name"]))), head: fmt.Sprint(r["name"]), sub: "▸ " + strings.Join(acts, "; "),
			lines: wrapWords("when "+fmt.Sprint(r["when"]), caR-caL)}
		t.h = lineH*t.nLines() + rowPad
		ruleText[i] = t
	}
	varText := make([]textRow, len(vars))
	for i, v := range vars {
		var from []string
		for _, path := range strList(v["updated_by"]) {
			if e := s.Entries[path]; e != nil {
				from = append(from, e.Label)
			}
		}
		t := textRow{id: tid(c, "pm-"+fmt.Sprint(v["name"])), head: fmt.Sprint(v["name"]), lines: wrapWords(fmt.Sprint(v["meaning"]), pmR-pmL)}
		if len(from) > 0 {
			t.head += "  ◂ " + strings.Join(from, "; ")
		} else if src, _ := v["source"].(string); src != "" {
			t.extra = wrapWords("no feedback: "+src, pmR-pmL)
		}
		t.h = lineH*t.nLines() + rowPad
		varText[i] = t
	}

	// Fixed geometry above the rows.
	y := 58
	var upperBoxes map[*Node][4]int
	if len(uppers) > 0 {
		upperBoxes = map[*Node][4]int{}
		w := (dW - 48 - 26*(len(uppers)-1)) / len(uppers)
		if w > 260 {
			w = 260
		}
		total := w*len(uppers) + 26*(len(uppers)-1)
		x := (dW - total) / 2
		for _, u := range uppers {
			upperBoxes[u] = [4]int{x, y, w, nodeH - 8}
			x += w + 26
		}
		y += nodeH - 8 + nodeGapY
	}
	boxTop := y
	titleY := boxTop + 24
	desc := wrapWords(c.S("description"), boxR-boxL-24)
	headY := titleY + 8 + lineH*len(desc) + 10
	rowsTop := headY + headH
	caH, pmH := 0, 0
	for _, t := range ruleText {
		caH += t.h
	}
	for _, t := range varText {
		pmH += t.h
	}
	boxBottom := rowsTop + maxi(caH, pmH) + 6
	level := func(i int) int { return boxBottom + 18 + 12*i }
	nodesTop := level(nLeft+nRight) + 22

	plan := func(ruleOrder, fbOrder []int, nodeOrder []*Node) *detailPlan {
		d := &detailPlan{ruleOrder: ruleOrder, fbOrder: fbOrder, nodeOrder: nodeOrder, caRows: map[int]int{}, pmRows: map[int]int{},
			boxBottom: boxBottom, nodesTop: nodesTop, lowerBoxes: map[*Node][4]int{}}
		ry := rowsTop
		for _, i := range ruleOrder {
			d.caRows[i] = ry
			ry += ruleText[i].h
		}
		// Variables grouped by their first feedback in fbOrder; those without feedback last.
		rank := map[string]int{}
		for k, i := range fbOrder {
			rank[fbs[i].Path()] = k
		}
		vorder := make([]int, len(vars))
		first := make([]int, len(vars))
		for i, v := range vars {
			vorder[i] = i
			first[i] = 1 << 20
			for _, path := range strList(v["updated_by"]) {
				if k, ok := rank[path]; ok && k < first[i] {
					first[i] = k
				}
			}
		}
		sort.SliceStable(vorder, func(a, b int) bool { return first[vorder[a]] < first[vorder[b]] })
		ry = rowsTop
		for _, i := range vorder {
			d.pmRows[i] = ry
			ry += varText[i].h
		}
		if len(nodeOrder) > 0 {
			gap := 26
			if len(nodeOrder) >= 5 {
				gap = 16 // five names side by side need the room
			}
			w := (boxR - boxL - gap*(len(nodeOrder)-1)) / len(nodeOrder)
			x := boxL
			for _, lo := range nodeOrder {
				d.lowerBoxes[lo] = [4]int{x, nodesTop, w, nodeH}
				x += w + gap
			}
		}
		// Control lanes: the lowest rule takes the innermost lane and the level nearest the
		// box; on each node the innermost lane takes the rightmost of its ports.
		pos := map[int]int{}
		for k, i := range ruleOrder {
			pos[i] = k
		}
		corder := make([]int, len(cps))
		for i := range corder {
			corder[i] = i
		}
		sort.SliceStable(corder, func(a, b int) bool { return pos[cps[corder[a]].rule] > pos[cps[corder[b]].rule] })
		lane := map[int]int{}
		for k, i := range corder {
			lane[i] = k
		}
		byNode := map[*Node][]int{}
		for _, i := range corder {
			byNode[cps[i].target] = append(byNode[cps[i].target], i)
		}
		for i, cp := range cps {
			b := d.lowerBoxes[cp.target]
			ports := byNode[cp.target]
			pi := 0
			for j, x := range ports {
				if x == i {
					pi = len(ports) - 1 - j
				}
			}
			px := b[0] + (b[2]/2)*(pi+1)/(len(ports)+1)
			k := lane[i]
			lx := boxL - laneGap*(k+1)
			ry := d.caRows[cp.rule] + 8
			d.ctl = append(d.ctl, [][2]int{{boxL, ry}, {lx, ry}, {lx, level(k)}, {px, level(k)}, {px, b[1]}})
		}
		// Feedback lanes: the feedback whose topmost variable is lowest takes the innermost
		// lane; on each node the innermost lane takes the leftmost of its ports.
		top := make([]int, len(fbs))
		for i, e := range fbs {
			top[i] = 1 << 20
			for vi, v := range vars {
				if contains(strList(v["updated_by"]), e.Path()) {
					top[i] = mini(top[i], d.pmRows[vi])
				}
			}
		}
		forder := make([]int, len(fbs))
		for i := range forder {
			forder[i] = i
		}
		sort.SliceStable(forder, func(a, b int) bool { return top[forder[a]] > top[forder[b]] })
		fbNode := map[*Node][]int{}
		for _, i := range forder {
			fbNode[fbs[i].Link.Lower] = append(fbNode[fbs[i].Link.Lower], i)
		}
		for k, i := range forder {
			e := fbs[i]
			b := d.lowerBoxes[e.Link.Lower]
			ports := fbNode[e.Link.Lower]
			pi := 0
			for j, x := range ports {
				if x == i {
					pi = j
				}
			}
			px := b[0] + b[2]/2 + (b[2]/2)*(pi+1)/(len(ports)+1)
			lx := boxR + laneGap*(k+1)
			lv := level(nLeft + k)
			topY := lv
			var branches [][][2]int
			for vi, v := range vars {
				if contains(strList(v["updated_by"]), e.Path()) {
					ry := d.pmRows[vi] + 8
					topY = mini(topY, ry)
					branches = append(branches, [][2]int{{lx, ry}, {boxR, ry}})
				}
			}
			d.fb = append(d.fb, [][2]int{{px, b[1]}, {px, lv}, {lx, lv}, {lx, topY}})
			d.fb = append(d.fb, branches...)
		}
		d.crossings = crossings(append(append([][][2]int{}, d.ctl...), d.fb...))
		return d
	}
	// Try every order of the rules, the feedback and the lower nodes (a handful each), and keep
	// the first with the fewest crossings: deterministic, and identity first on a tie.
	var best *detailPlan
	for _, no := range permutations(len(lowers)) {
		nodes := make([]*Node, len(no))
		for i, j := range no {
			nodes[i] = lowers[j]
		}
		for _, ro := range permutations(len(rules)) {
			for _, fo := range permutations(len(fbs)) {
				d := plan(ro, fo, nodes)
				if best == nil || d.crossings < best.crossings {
					best = d
				}
			}
		}
	}
	height := nodesTop + nodeH + 64
	if len(lowers) == 0 {
		height = boxBottom + 64
	}

	// Draw.
	// Every text carries its own text id (the PRD addresses each string): record ids, the rule's
	// position in its record, the variable's name, the entry's path.
	text := func(x, y int, s, id string, attrs ...string) *el {
		t := mk("text", append([]string{"x", itoa(x), "y", itoa(y)}, attrs...)...)
		t.text, t.textID = s, id
		return t
	}
	var svgKids []*el
	defs := mk("defs").add(mk("marker", "id", markerID, "viewBox", "0 0 10 10", "refX", "9", "refY", "5",
		"markerWidth", "6", "markerHeight", "6", "orient", "auto-start-reverse").add(mk("path", "d", "M0 0L10 5L0 10z", "fill", "@edge")))
	svgKids = append(svgKids, defs)
	title := text(24, 34, c.S("title")+": control algorithm and process model", "title", "font-size", "15", "font-weight", "600", "fill", "@ink")
	svgKids = append(svgKids, title)
	ctl := mk("g", "fill", "none", "stroke", "@edge", "stroke-width", "1.25")
	ctl.anchor = "control-actions"
	fb := mk("g", "fill", "none", "stroke", "@edge", "stroke-width", "1.25", "stroke-dasharray", "5 4")
	fb.anchor = "feedback"
	var labels []*el
	for _, l := range up {
		b := upperBoxes[l.Upper]
		cx := b[0] + b[2]/2
		if len(l.Control) > 0 {
			ctl.add(mk("path", "d", pathD([][2]int{{cx - 14, b[1] + b[3]}, {cx - 14, boxTop}}), "marker-end", "url(#"+markerID+")"))
			labels = append(labels, text(cx-22, b[1]+b[3]+nodeGapY/2+4, joinLabels(l.Control), "lbl-"+pathID(l.Control[0]), "text-anchor", "end", "font-size", "11.5", "fill", "@quiet"))
		}
		if len(l.Feedback) > 0 {
			fb.add(mk("path", "d", pathD([][2]int{{cx + 14, boxTop}, {cx + 14, b[1] + b[3]}}), "marker-end", "url(#"+markerID+")"))
			labels = append(labels, text(cx+22, b[1]+b[3]+nodeGapY/2+4, joinLabels(l.Feedback), "lbl-"+pathID(l.Feedback[0]), "font-size", "11.5", "fill", "@quiet"))
		}
	}
	for _, pl := range best.ctl {
		ctl.add(mk("path", "d", pathD(pl), "marker-end", "url(#"+markerID+")"))
	}
	for _, pl := range best.fb {
		pe := mk("path", "d", pathD(pl))
		if pl[len(pl)-1][0] == boxR { // a branch into a variable
			pe.attrs = append(pe.attrs, attr{"marker-end", "url(#" + markerID + ")"})
		}
		fb.add(pe)
	}
	svgKids = append(svgKids, ctl, fb)
	svgKids = append(svgKids, labels...)
	g := mk("g")
	g.anchor = c.ID
	rect := mk("rect", "x", itoa(boxL), "y", itoa(boxTop), "width", itoa(boxR-boxL), "height", itoa(boxBottom-boxTop), "rx", "8")
	if c.S("component_type") == "human" {
		rect.attrs = append(rect.attrs, attr{"fill", "@accent"}, attr{"fill-opacity", "0.12"}, attr{"stroke", "@accent"}, attr{"stroke-width", "2"})
	} else {
		rect.attrs = append(rect.attrs, attr{"fill", "none"}, attr{"stroke", "@edge"}, attr{"stroke-width", "1.25"})
	}
	g.add(rect)
	g.add(text(boxL+12, titleY, c.S("title"), tid(c, "name"), "font-weight", "600", "fill", "@ink"))
	for i, line := range desc {
		g.add(text(boxL+12, titleY+lineH*(i+1)+2, line, tid(c, fmt.Sprintf("l%d", i+1)), "font-size", "11.5", "fill", "@quiet"))
	}
	for _, comp := range []struct {
		x1, x2   int
		name, id string
	}{{boxL + 6, mid - 2, "Control algorithm", "ca"}, {mid + 2, boxR - 6, "Process model", "pm"}} {
		g.add(mk("rect", "x", itoa(comp.x1), "y", itoa(headY-4), "width", itoa(comp.x2-comp.x1), "height", itoa(boxBottom-headY-2), "rx", "6",
			"fill", "@tint", "stroke", "none"))
		g.add(text(comp.x1+6, headY+12, strings.ToUpper(comp.name), tid(c, comp.id), "font-size", "10.5", "font-weight", "600", "fill", "@quiet"))
	}
	for _, i := range best.ruleOrder {
		g.add(ruleText[i].at(caL, best.caRows[i]).lines...)
	}
	for i := range vars {
		g.add(varText[i].at(pmL, best.pmRows[i]).lines...)
	}
	svgKids = append(svgKids, g)
	nodeBox := func(nd *Node, b [4]int) *el {
		ng := mk("g")
		ng.anchor = nd.ID
		r := mk("rect", "x", itoa(b[0]), "y", itoa(b[1]), "width", itoa(b[2]), "height", itoa(b[3]), "rx", "8")
		switch {
		case nd.Rec != nil && nd.Rec.S("component_type") == "human":
			r.attrs = append(r.attrs, attr{"fill", "@accent"}, attr{"fill-opacity", "0.12"}, attr{"stroke", "@accent"}, attr{"stroke-width", "2"})
		case !nd.Controller:
			r.attrs = append(r.attrs, attr{"fill", "@tint"}, attr{"stroke", "@edge"}, attr{"stroke-width", "1.25"})
		default:
			r.attrs = append(r.attrs, attr{"fill", "none"}, attr{"stroke", "@edge"}, attr{"stroke-width", "1.25"})
		}
		ng.add(r)
		t := ""
		if nd.Rec != nil {
			t = nd.Rec.S("title")
		}
		// A name wider than its box (five nodes side by side) takes two lines.
		lines := []string{t}
		if float64(len([]rune(t)))*nameCharW > float64(b[2]-24) {
			lines = wrapChars(t, int(float64(b[2]-24)/nameCharW))
		}
		for i, line := range lines {
			id := tid(nd.Rec, "name")
			if i > 0 {
				id += fmt.Sprintf("%d", i+1)
			}
			ng.add(text(b[0]+12, b[1]+b[3]/2+5+16*i-8*(len(lines)-1), line, id, "font-weight", "600", "fill", "@ink"))
		}
		return ng
	}
	for _, u := range uppers {
		svgKids = append(svgKids, nodeBox(u, upperBoxes[u]))
	}
	for _, lo := range best.nodeOrder {
		svgKids = append(svgKids, nodeBox(lo, best.lowerBoxes[lo]))
	}
	ky := height - 24
	key := mk("g")
	key.anchor = "key"
	x := 24
	item := func(sym *el, symW int, label, id string) {
		key.add(sym)
		key.add(text(x+symW+8, ky+4, label, id, "font-size", "11.5", "fill", "@quiet"))
		x += symW + 8 + textW(label) + 32
	}
	item(mk("line", "x1", itoa(x), "x2", itoa(x+32), "y1", itoa(ky), "y2", itoa(ky), "stroke", "@edge", "stroke-width", "1.25"), 32, "control action, from the rule that issues it", "key-control")
	item(mk("line", "x1", itoa(x), "x2", itoa(x+32), "y1", itoa(ky), "y2", itoa(ky), "stroke", "@edge", "stroke-width", "1.25", "stroke-dasharray", "5 4"), 32, "feedback, into each variable it updates", "key-feedback")
	svgKids = append(svgKids, key)
	svg := mk("svg", "viewBox", fmt.Sprintf("0 0 %d %d", dW, height), "role", "img", "aria-label", c.S("title")+": control algorithm and process model", "font-size", "13")
	svg.add(svgKids...)
	return svg, height
}

func joinLabels(es []*Entry) string {
	var ls []string
	for _, e := range es {
		ls = append(ls, e.Label)
	}
	sort.Strings(ls)
	return strings.Join(ls, ", ")
}

// DetailSVG renders a controller's detail diagram as a standalone SVG (light and dark).
func (p *Project) DetailSVG(n *Node) string {
	root, h := p.DetailDiagram(n, "arrow")
	root.attrs = append([]attr{{"xmlns", "http://www.w3.org/2000/svg"}, {"width", itoa(dW)}, {"height", itoa(h)}}, root.attrs...)
	root.attrs = append(root.attrs, attr{"font-family", "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"})
	style := &el{tag: "style", text: "\n" + repoStyle + "\n"}
	bg := mk("rect", "width", "100%", "height", "100%", "rx", "12", "class", "bg")
	root.kids = append([]*el{style, bg}, root.kids...)
	var b strings.Builder
	fmt.Fprintf(&b, "<!-- Generated by otel-chdb/stpa/tools from %s and structure.yaml (controller %s). Do not edit. -->\n", n.Rec.Path, n.Name)
	root.writeSVG(&b, "")
	return strings.Replace(b.String(), "&#39;", "'", -1)
}

// DetailPRD renders a controller's detail diagram as the PRD's widget code, like the overview's
// (L.PRD): JSX in SVG, the document's theme tokens, a data-claude-text-id on every text.
func (p *Project) DetailPRD(n *Node) string {
	root, _ := p.DetailDiagram(n, "controller-"+n.Name+"-arrow")
	var b strings.Builder
	b.WriteString(prdPrelude)
	root.writeJSX(&b)
	b.WriteString("; };\n")
	return b.String()
}

// DetailMermaid renders a controller's detail as Mermaid: the controller a subgraph with two
// subgraphs (its rules and its variables), control from each rule to the node it acts on,
// feedback from each node to each variable it updates.
func (p *Project) DetailMermaid(n *Node) string {
	s := p.Structure
	c := n.Rec
	var b strings.Builder
	b.WriteString("---\nconfig:\n  layout: elk\n---\nflowchart TB\n")
	fmt.Fprintf(&b, "  %%%% Generated by otel-chdb/stpa/tools from %s and structure.yaml. Do not edit.\n", c.Path)
	q := func(t string) string { return strings.ReplaceAll(t, `"`, "#quot;") }
	fmt.Fprintf(&b, "  subgraph C[\"%s\"]\n", q(c.S("title")))
	b.WriteString("    subgraph CA[\"Control algorithm\"]\n")
	for i, r := range algoOf(c) {
		var acts []string
		for _, path := range strList(r["issues"]) {
			if e := s.Entries[path]; e != nil {
				acts = append(acts, e.Label)
			}
		}
		fmt.Fprintf(&b, "      r%d[\"<b>%s</b><br/>▸ %s<br/>when %s\"]\n", i+1, q(fmt.Sprint(r["name"])), q(strings.Join(acts, ", ")), q(fmt.Sprint(r["when"])))
	}
	b.WriteString("    end\n    subgraph PM[\"Process model\"]\n")
	for i, v := range pmOf(c) {
		fmt.Fprintf(&b, "      v%d[\"<b>%s</b>\"]\n", i+1, q(fmt.Sprint(v["name"])))
	}
	b.WriteString("    end\n  end\n")
	nodeKey := map[*Node]string{}
	var fbs []string
	for _, l := range s.Links {
		if l.Upper != n {
			continue
		}
		k := "n" + Hex(l.Lower.ID)
		if nodeKey[l.Lower] == "" {
			nodeKey[l.Lower] = k
			shape := "[\"%s\"]"
			if !l.Lower.Controller {
				shape = "[(\"%s\")]"
			}
			fmt.Fprintf(&b, "  %s"+shape+"\n", k, q(l.Lower.Rec.S("title")))
		}
		for i, r := range algoOf(c) {
			var acts []string
			for _, path := range strList(r["issues"]) {
				if e := s.Entries[path]; e != nil && e.Link == l {
					acts = append(acts, e.Label)
				}
			}
			if len(acts) > 0 {
				fmt.Fprintf(&b, "  r%d -->|%s| %s\n", i+1, q(strings.Join(acts, ", ")), k)
			}
		}
		for _, e := range l.Feedback {
			for i, v := range pmOf(c) {
				if contains(strList(v["updated_by"]), e.Path()) {
					id := fmt.Sprintf("fb%d", len(fbs)+1)
					fbs = append(fbs, id)
					fmt.Fprintf(&b, "  v%d %s@<-.->|%s| %s\n", i+1, id, q(e.Label), k)
				}
			}
		}
	}
	if len(fbs) > 0 {
		b.WriteString("  classDef fb stroke:#888,marker-end:none\n")
		fmt.Fprintf(&b, "  class %s fb\n", strings.Join(fbs, ","))
	}
	return b.String()
}
