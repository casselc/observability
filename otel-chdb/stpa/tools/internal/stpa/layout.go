package stpa

import (
	"sort"
	"strings"
)

// Geometry of the PRD control-structure style: a 760-wide canvas, a 3-column grid 212 wide
// with 26 between columns, boxes with a bold name and up to three quiet lines, rows 56 apart,
// a lane for long feedback loops on the right, and a key at the bottom.
const (
	canvasW   = 760
	left      = 24
	right     = 712
	colGap    = 26
	firstRowY = 72
	rowGap    = 56
	laneX     = 730
	laneStep  = 10
	pairSep   = 40
	charW     = 6.2 // average advance of the 11.5 px quiet type
	maxLines  = 3
)

// Box is a placed component.
type Box struct {
	Rec        *Record
	Node       *Node
	Row        int
	X, Y, W, H int
	Lines      []string
	Human      bool
	Process    bool
}

func (b *Box) cx() int { return b.X + b.W/2 }
func (b *Box) cy() int { return b.Y + b.H/2 }
func (b *Box) x2() int { return b.X + b.W }
func (b *Box) y2() int { return b.Y + b.H }

// Edge is one drawn line: every record of one kind between one ordered pair of components.
type Edge struct {
	Kind     string // action | feedback
	From, To *Box
	Entries  []*Entry
	Label    string // the members' labels, joined
	Text     string // what the drawing writes beside the edge (Label, or moved to a neighbour)
	Path     [][2]int
	LabelX   int
	LabelY   int
	Anchor   string // start | end
}

// Layout is a placed control-structure view.
type Layout struct {
	vsegs                [][3]int // x, y1, y2 of vertical segments already routed
	View                 *View
	p                    *Project
	W                    int // canvas width
	Boxes                []*Box
	Edges                []*Edge
	Height               int
	KeyY                 int
	HasHuman, HasProcess bool
}

func colWidth(cols, width int) int {
	return (width - (canvasW - right) - left - (cols-1)*colGap) / cols
}

// wrap splits a description into lines that fit a box: first at "; " boundaries, joining
// segments while they fit, then at spaces. A trailing comma at a line end is dropped.
func wrap(s string, width int) []string {
	if s == "" {
		return nil
	}
	max := int(float64(width-24) / charW)
	var lines []string
	cur := ""
	for _, seg := range strings.Split(s, "; ") {
		if cur != "" && len(cur)+2+len(seg) <= max {
			cur += "; " + seg
			continue
		}
		if cur != "" {
			lines = append(lines, cur)
			cur = ""
		}
		for len(seg) > max {
			cut := strings.LastIndex(seg[:max+1], " ")
			if cut <= 0 {
				cut = max
			}
			lines = append(lines, strings.TrimSuffix(seg[:cut], ","))
			seg = strings.TrimSpace(seg[cut:])
		}
		cur = seg
	}
	if cur != "" {
		lines = append(lines, cur)
	}
	if len(lines) > maxLines {
		lines = append(lines[:maxLines-1], strings.Join(lines[maxLines-1:], " "))
	}
	return lines
}

func textW(s string) int { return int(float64(len([]rune(s)))*charW + 0.5) }

// Place lays a control-structure view out.
func (p *Project) Place(v *View) *Layout {
	L := &Layout{View: v, p: p}
	L.W = v.Width
	if L.W == 0 {
		L.W = canvasW
	}
	cw := colWidth(v.Columns, L.W)
	y := firstRowY
	byName := map[string]*Box{}
	for ri, row := range v.Rows {
		h := 0
		var rowBoxes []*Box
		for _, c := range row {
			n := p.Structure.Nodes[c.Ref]
			if n == nil || n.Rec == nil {
				continue // reported by the view check
			}
			r := n.Rec
			b := &Box{Rec: r, Node: n, Row: ri, X: left + c.Col*(cw+colGap), Y: y, W: c.Span*cw + (c.Span-1)*colGap}
			if c.Detail == nil || *c.Detail {
				b.Lines = wrap(r.S("description"), b.W)
			}
			b.Human = r.S("component_type") == "human"
			b.Process = !n.Controller
			L.HasHuman = L.HasHuman || b.Human
			L.HasProcess = L.HasProcess || b.Process
			b.H = 40 + 16*len(b.Lines) + p.stripH(v, n)
			if b.H > h {
				h = b.H
			}
			rowBoxes = append(rowBoxes, b)
			byName[n.Name] = b
		}
		for _, b := range rowBoxes {
			b.H = h
		}
		L.Boxes = append(L.Boxes, rowBoxes...)
		y += h + rowGap
	}
	bottom := y - rowGap
	// One drawn edge per link and direction: its control entries down, its feedback up.
	for _, l := range p.Structure.Links {
		up, lo := byName[l.Upper.Name], byName[l.Lower.Name]
		if up == nil || lo == nil {
			continue
		}
		for _, kind := range []string{"control", "feedback"} {
			es := l.Control
			f, t := up, lo
			if kind == "feedback" {
				es, f, t = l.Feedback, lo, up
			}
			if len(es) == 0 {
				continue
			}
			e := &Edge{Kind: map[string]string{"control": "action", "feedback": "feedback"}[kind], From: f, To: t, Entries: es}
			var labels []string
			for _, x := range es {
				labels = append(labels, x.Label)
			}
			sort.Strings(labels)
			e.Label = strings.Join(labels, ", ")
			e.Text = e.Label
			L.Edges = append(L.Edges, e)
		}
	}
	// Deterministic edge order: by the controller's position, then the counterpart's, actions first.
	pos := func(b *Box) int { return b.Row*100 + b.X }
	ctl := func(e *Edge) (*Box, *Box) {
		if e.Kind == "action" {
			return e.From, e.To
		}
		return e.To, e.From
	}
	sort.SliceStable(L.Edges, func(i, j int) bool {
		ci, pi := ctl(L.Edges[i])
		cj, pj := ctl(L.Edges[j])
		if pos(ci) != pos(cj) {
			return pos(ci) < pos(cj)
		}
		if pos(pi) != pos(pj) {
			return pos(pi) < pos(pj)
		}
		return L.Edges[i].Kind < L.Edges[j].Kind
	})
	L.route()
	L.KeyY = bottom + 28
	L.Height = L.KeyY + 24
	return L
}

// blocked reports whether a vertical segment at x between y1 and y2 (or a horizontal one)
// crosses any box other than the given ones.
func (L *Layout) blockedV(x, y1, y2 int, skip ...*Box) bool {
	if y1 > y2 {
		y1, y2 = y2, y1
	}
	for _, b := range L.Boxes {
		if containsBox(skip, b) {
			continue
		}
		if x >= b.X-4 && x <= b.x2()+4 && y2 > b.Y && y1 < b.y2() {
			return true
		}
	}
	return false
}

func (L *Layout) blockedH(y, x1, x2 int, skip ...*Box) bool {
	if x1 > x2 {
		x1, x2 = x2, x1
	}
	for _, b := range L.Boxes {
		if containsBox(skip, b) {
			continue
		}
		if y >= b.Y-4 && y <= b.y2()+4 && x2 > b.X && x1 < b.x2() {
			return true
		}
	}
	return false
}

func containsBox(bs []*Box, b *Box) bool {
	for _, x := range bs {
		if x == b {
			return true
		}
	}
	return false
}

func (L *Layout) leftmost(b *Box) bool {
	for _, o := range L.Boxes {
		if o != b && o.Row == b.Row && o.X < b.X {
			return false
		}
	}
	return true
}

func (L *Layout) rightmost(b *Box) bool {
	for _, o := range L.Boxes {
		if o != b && o.Row == b.Row && o.X > b.X {
			return false
		}
	}
	return true
}

// gapBelow is the y of the middle of the gap under row r (above row r+1).
func (L *Layout) rowBottom(r int) int {
	for _, b := range L.Boxes {
		if b.Row == r {
			return b.y2()
		}
	}
	return 0
}

func (L *Layout) rowTop(r int) int {
	for _, b := range L.Boxes {
		if b.Row == r {
			return b.Y
		}
	}
	return 0
}

func (L *Layout) route() {
	// Edges between the same two boxes are drawn side by side: control left, feedback right.
	type pair struct{ a, b *Box }
	bundles := map[pair][]*Edge{}
	var porder []pair
	for _, e := range L.Edges {
		a, b := e.From, e.To
		if pos2(a) > pos2(b) {
			a, b = b, a
		}
		k := pair{a, b}
		if bundles[k] == nil {
			porder = append(porder, k)
		}
		bundles[k] = append(bundles[k], e)
	}
	lane := 0
	sidePorts := map[*Box]int{}
	port := func(b *Box) int {
		n := sidePorts[b]
		sidePorts[b]++
		off := []int{0, 16, -16, 32, -32}
		return b.cy() + off[n%len(off)]
	}
	for _, k := range porder {
		es := bundles[k]
		sort.SliceStable(es, func(i, j int) bool { return es[i].Kind == "action" && es[j].Kind != "action" })
		a, b := k.a, k.b
		lo, hi := maxi(a.X, b.X), mini(a.x2(), b.x2())
		n := len(es)
		if a.Row != b.Row && hi-lo >= pairSep*(n-1)+20 {
			cx := (lo + hi) / 2
			// Straight verticals, if the corridor is clear.
			ok := true
			for i := range es {
				x := cx + (2*i-(n-1))*pairSep/2 + L.halfShift(es, n)
				if L.blockedV(x, a.y2(), b.Y, a, b) {
					ok = false
				}
			}
			if ok {
				for i, e := range es {
					x := cx + (2*i-(n-1))*pairSep/2 + L.halfShift(es, n)
					f, t := e.From, e.To
					if f.Y < t.Y {
						e.Path = [][2]int{{x, f.y2()}, {x, t.Y}}
					} else {
						e.Path = [][2]int{{x, f.Y}, {x, t.y2()}}
					}
					L.noteV(e.Path)
					// Label in the first gap after the upper box.
					upper := f
					if t.Y < f.Y {
						upper = t
					}
					gy := (upper.y2()+L.rowTop(upper.Row+1))/2 + 4
					e.LabelY = gy
					if n > 1 && i == 0 {
						e.LabelX, e.Anchor = x-8, "end"
					} else {
						e.LabelX, e.Anchor = x+8, "start"
					}
				}
				// A left label that would leave the canvas joins the right one: "control; feedback".
				if n == 2 && es[0].LabelX-textW(es[0].Text) < 4 {
					// No room on the left (the canvas edge): both labels on the right, stacked,
					// control above feedback.
					es[0].LabelX, es[0].Anchor = es[1].LabelX, "start"
					es[0].LabelY = es[1].LabelY - 7
					es[1].LabelY += 7
				}
				continue
			}
		}
		for _, e := range es {
			L.routeOne(e, port, &lane)
			L.noteV(e.Path)
		}
	}
	L.spreadLabels()
}

// halfShift moves a lone control edge under the control-algorithm half of a controller box,
// and a lone feedback edge under its process-model half, when the view draws compartments.
func (L *Layout) halfShift(es []*Edge, n int) int {
	if L.View.Internals == "" || n != 1 {
		return 0
	}
	if es[0].Kind == "action" {
		return -pairSep / 2
	}
	return pairSep / 2
}

// stripH is the height of a controller box's compartment strip in a view that draws one.
func (p *Project) stripH(v *View, n *Node) int {
	if !n.Controller {
		return 0
	}
	switch v.Internals {
	case "headers":
		return 38
	case "full":
		return 26 + 13*maxi(len(algoOf(n.Rec)), len(pmOf(n.Rec)))
	}
	return 0
}

func pos2(b *Box) int { return b.Row*10000 + b.X }

// freeX returns x, or the nearest offset of it, where a vertical segment from y1 to y2 does
// not run along one already routed.
func (L *Layout) freeX(x, y1, y2 int) int {
	if y1 > y2 {
		y1, y2 = y2, y1
	}
	for _, d := range []int{0, 20, -20, 40, -40} {
		ok := true
		for _, s := range L.vsegs {
			if absi(s[0]-(x+d)) < 8 && y2 > s[1] && y1 < s[2] {
				ok = false
				break
			}
		}
		if ok {
			return x + d
		}
	}
	return x
}

func (L *Layout) noteV(pts [][2]int) {
	for i := 1; i < len(pts); i++ {
		if pts[i][0] == pts[i-1][0] {
			L.vsegs = append(L.vsegs, [3]int{pts[i][0], mini(pts[i][1], pts[i-1][1]), maxi(pts[i][1], pts[i-1][1])})
		}
	}
}

// spreadLabels moves a label that overlaps one placed before it down (or up) a line.
func (L *Layout) spreadLabels() {
	type rect struct{ x1, y1, x2, y2 int }
	var placed []rect
	box := func(e *Edge, dy int) rect {
		w := textW(e.Text)
		x1 := e.LabelX
		switch e.Anchor {
		case "end":
			x1 -= w
		case "middle":
			x1 -= w / 2
		}
		return rect{x1, e.LabelY + dy - 10, x1 + w, e.LabelY + dy + 3}
	}
	hitsBox := func(r rect) bool {
		for _, b := range L.Boxes {
			if r.x1 < b.x2() && b.X < r.x2 && r.y1 < b.y2() && b.Y < r.y2 {
				return true
			}
		}
		return false
	}
	hitsLine := func(r rect, own *Edge) bool {
		for _, o := range L.Edges {
			if o == own {
				continue
			}
			for i := 1; i < len(o.Path); i++ {
				a, b := o.Path[i-1], o.Path[i]
				if a[0] == b[0] && a[0] > r.x1-3 && a[0] < r.x2+3 && mini(a[1], b[1]) < r.y2 && maxi(a[1], b[1]) > r.y1 {
					return true
				}
				if a[1] == b[1] && a[1] > r.y1-2 && a[1] < r.y2+2 && mini(a[0], b[0]) < r.x2 && maxi(a[0], b[0]) > r.x1 {
					return true
				}
			}
		}
		return false
	}
	for _, e := range L.Edges {
		if e.Text == "" {
			continue
		}
		chosen, ok := 0, false
		for pass := 0; pass < 2 && !ok; pass++ {
			for _, dy := range []int{0, -14, 14, -24, 24} {
				r := box(e, dy)
				clash := hitsBox(r) || (pass == 0 && hitsLine(r, e))
				for _, q := range placed {
					if r.x1 < q.x2+6 && q.x1 < r.x2+6 && r.y1 < q.y2 && q.y1 < r.y2 {
						clash = true
						break
					}
				}
				if !clash {
					chosen, ok = dy, true
					break
				}
			}
		}
		e.LabelY += chosen
		placed = append(placed, box(e, 0))
	}
}

func (L *Layout) routeOne(e *Edge, port func(*Box) int, lane *int) {
	f, t := e.From, e.To
	down := f.Y < t.Y
	// L-route 1: vertical out of f at its centre, then horizontal into t's side.
	x := f.cx()
	if t.x2() < f.X || t.X > f.x2() {
		ty := port(t)
		sideX := t.x2()
		if t.X > f.x2() {
			sideX = t.X
		}
		y0 := f.y2()
		if !down {
			y0 = f.Y
		}
		x = L.freeX(x, y0, ty)
		if !L.blockedV(x, y0, ty, f, t) && !L.blockedH(ty, x, sideX, f, t) {
			e.Path = [][2]int{{x, y0}, {x, ty}, {sideX, ty}}
			e.LabelX, e.LabelY, e.Anchor = x+8, (y0+ty)/2+4, "start"
			return
		}
		// L-route 2: horizontal out of f's side, then vertical into t at its centre.
		fy := port(f)
		fx := f.x2()
		if t.x2() < f.X {
			fx = f.X
		}
		ty2 := t.Y
		if !down {
			ty2 = t.y2()
		}
		tx := L.freeX(t.cx(), fy, ty2)
		if !L.blockedH(fy, fx, tx, f, t) && !L.blockedV(tx, fy, ty2, f, t) {
			e.Path = [][2]int{{fx, fy}, {tx, fy}, {tx, ty2}}
			e.LabelX, e.LabelY, e.Anchor = (fx+tx)/2, fy-6, "middle"
			return
		}
	}
	// Lane on the left, when both ends are the leftmost boxes of their rows: out of f's left
	// side, along the canvas edge, into t's left side.
	if L.leftmost(f) && L.leftmost(t) {
		lx := left - 12
		fy, ty := port(f), port(t)
		e.Path = [][2]int{{f.X, fy}, {lx, fy}, {lx, ty}, {t.X, ty}}
		mid := (fy + ty) / 2
		best, bestD := mid, 1<<30
		for r := 0; L.rowTop(r+1) != 0; r++ {
			g := (L.rowBottom(r) + L.rowTop(r+1)) / 2
			if g > mini(fy, ty) && g < maxi(fy, ty) {
				if d := absi(g - mid); d < bestD {
					best, bestD = g, d
				}
			}
		}
		e.LabelX, e.LabelY, e.Anchor = lx+8, best+4, "start"
		return
	}
	// Lane on the right: out of f, up or down the lane, into t.
	lx := L.W - (canvasW - laneX) + *lane*laneStep
	*lane++
	var pts [][2]int
	if L.rightmost(f) {
		y := port(f)
		pts = append(pts, [2]int{f.x2(), y}, [2]int{lx, y})
	} else {
		gy := (f.y2() + L.rowTop(f.Row+1)) / 2
		if !down {
			gy = (f.Y + L.rowBottom(f.Row-1)) / 2
		}
		sy := f.y2()
		if !down {
			sy = f.Y
		}
		pts = append(pts, [2]int{f.cx(), sy}, [2]int{f.cx(), gy}, [2]int{lx, gy})
	}
	if L.rightmost(t) {
		y := port(t)
		pts = append(pts, [2]int{lx, y}, [2]int{t.x2(), y})
	} else {
		gy := (t.Y + L.rowBottom(t.Row-1)) / 2
		ey := t.Y
		if !down {
			gy = (t.y2() + L.rowTop(t.Row+1)) / 2
			ey = t.y2()
		}
		pts = append(pts, [2]int{lx, gy}, [2]int{t.cx(), gy}, [2]int{t.cx(), ey})
	}
	e.Path = pts
	// Label above the lane's longest horizontal run, if it has room; else beside the lane, in
	// the row gap nearest the lane's middle.
	bestRun, runX, runY := 0, 0, 0
	for i := 1; i < len(pts); i++ {
		if pts[i][1] == pts[i-1][1] {
			if w := absi(pts[i][0] - pts[i-1][0]); w > bestRun {
				bestRun, runX, runY = w, mini(pts[i][0], pts[i-1][0]), pts[i][1]
			}
		}
	}
	if bestRun >= textW(e.Text)+24 {
		e.LabelX, e.LabelY, e.Anchor = runX+12, runY-6, "start"
		return
	}
	y1, y2 := pts[1][1], pts[len(pts)-2][1]
	mid := (y1 + y2) / 2
	best, bestD := mid, 1<<30
	for r := 0; L.rowTop(r+1) != 0; r++ {
		g := (L.rowBottom(r) + L.rowTop(r+1)) / 2
		if g > mini(y1, y2) && g < maxi(y1, y2) {
			if d := absi(g - mid); d < bestD {
				best, bestD = g, d
			}
		}
	}
	e.LabelX, e.LabelY, e.Anchor = lx-10, best+4, "end"
}

func mini(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxi(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func absi(a int) int {
	if a < 0 {
		return -a
	}
	return a
}
