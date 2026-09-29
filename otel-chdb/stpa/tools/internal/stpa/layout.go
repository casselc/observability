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
	Records  []*Record
	Label    string
	Path     [][2]int
	LabelX   int
	LabelY   int
	Anchor   string // start | end
}

// Layout is a placed control-structure view.
type Layout struct {
	vsegs                [][3]int // x, y1, y2 of vertical segments already routed
	View                 *View
	Boxes                []*Box
	Edges                []*Edge
	Height               int
	KeyY                 int
	HasHuman, HasProcess bool
}

func colWidth(cols int) int { return (right - left - (cols-1)*colGap) / cols }

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
	L := &Layout{View: v}
	cw := colWidth(v.Columns)
	y := firstRowY
	byID := map[string]*Box{}
	for ri, row := range v.Rows {
		h := 0
		var rowBoxes []*Box
		for _, c := range row {
			r := p.Records[c.Ref]
			b := &Box{Rec: r, Row: ri, X: left + c.Col*(cw+colGap), Y: y, W: c.Span*cw + (c.Span-1)*colGap}
			if c.Detail == nil || *c.Detail {
				b.Lines = wrap(r.S("description"), b.W)
			}
			b.Human = r.S("component_type") == "human"
			b.Process = !p.IsController(r)
			L.HasHuman = L.HasHuman || b.Human
			L.HasProcess = L.HasProcess || b.Process
			b.H = 40 + 16*len(b.Lines)
			if b.H > h {
				h = b.H
			}
			rowBoxes = append(rowBoxes, b)
			byID[r.ID] = b
		}
		for _, b := range rowBoxes {
			b.H = h
		}
		L.Boxes = append(L.Boxes, rowBoxes...)
		y += h + rowGap
	}
	bottom := y - rowGap
	// Group the view's actions and feedback by (kind, from, to).
	type key struct{ kind, from, to string }
	groups := map[key]*Edge{}
	var order []key
	for _, kind := range []string{"action", "feedback"} {
		for _, r := range p.Of(kind) {
			f, t := byID[r.S("from")], byID[r.S("to")]
			if f == nil || t == nil {
				continue
			}
			k := key{kind, f.Rec.ID, t.Rec.ID}
			if groups[k] == nil {
				groups[k] = &Edge{Kind: kind, From: f, To: t}
				order = append(order, k)
			}
			groups[k].Records = append(groups[k].Records, r)
		}
	}
	for _, k := range order {
		e := groups[k]
		var labels []string
		for _, r := range e.Records {
			labels = append(labels, r.S("label"))
		}
		sort.Strings(labels)
		e.Label = strings.Join(labels, ", ")
		L.Edges = append(L.Edges, e)
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
				x := cx + (2*i-(n-1))*pairSep/2
				if L.blockedV(x, a.y2(), b.Y, a, b) {
					ok = false
				}
			}
			if ok {
				for i, e := range es {
					x := cx + (2*i-(n-1))*pairSep/2
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
				if n == 2 && es[0].LabelX-textW(es[0].Label) < 4 {
					es[1].Label = es[0].Label + "; " + es[1].Label
					es[0].Label = ""
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
		w := textW(e.Label)
		x1 := e.LabelX
		switch e.Anchor {
		case "end":
			x1 -= w
		case "middle":
			x1 -= w / 2
		}
		return rect{x1, e.LabelY + dy - 10, x1 + w, e.LabelY + dy + 3}
	}
	for _, e := range L.Edges {
		if e.Label == "" {
			continue
		}
		for _, dy := range []int{0, 14, -14, 28} {
			r := box(e, dy)
			clash := false
			for _, q := range placed {
				if r.x1 < q.x2+6 && q.x1 < r.x2+6 && r.y1 < q.y2 && q.y1 < r.y2 {
					clash = true
					break
				}
			}
			if !clash {
				e.LabelY += dy
				placed = append(placed, r)
				break
			}
		}
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
	// Lane on the right: out of f, up or down the lane, into t.
	lx := laneX + *lane*laneStep
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
	// Label beside the lane, in the row gap nearest the lane's middle.
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
