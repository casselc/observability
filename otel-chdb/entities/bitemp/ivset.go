package bitemp

import "sort"

// iv is a valid-time interval [a, b).
type iv struct{ a, b Time }

// ivset is a set of disjoint, non-adjacent intervals in ascending order.
type ivset []iv

// add merges x into the set.
func (s *ivset) add(x iv) {
	if x.a >= x.b {
		return
	}
	v := *s
	i := sort.Search(len(v), func(i int) bool { return v[i].b >= x.a }) // first that touches or follows x
	j := i
	for j < len(v) && v[j].a <= x.b {
		x.a, x.b = min(x.a, v[j].a), max(x.b, v[j].b)
		j++
	}
	out := append(v[:i:i], x)
	*s = append(out, v[j:]...)
}

// covers reports whether x lies inside one interval of the set.
func (s ivset) covers(x iv) bool {
	if x.a >= x.b {
		return true
	}
	i := sort.Search(len(s), func(i int) bool { return s[i].b > x.a })
	return i < len(s) && s[i].a <= x.a && x.b <= s[i].b
}

// subtractFrom returns xs minus the set (xs ascending, disjoint).
func (s ivset) subtractFrom(xs []iv) []iv {
	var out []iv
	for _, x := range xs {
		cur := x.a
		i := sort.Search(len(s), func(i int) bool { return s[i].b > x.a })
		for ; i < len(s) && s[i].a < x.b; i++ {
			if s[i].a > cur {
				out = append(out, iv{cur, s[i].a})
			}
			cur = max(cur, s[i].b)
		}
		if cur < x.b {
			out = append(out, iv{cur, x.b})
		}
	}
	return out
}

// subtract returns s minus t.
func (s ivset) subtract(t ivset) []iv { return t.subtractFrom(s) }

// intersect returns the parts of x inside the set.
func (s ivset) intersect(x iv) []iv {
	var out []iv
	i := sort.Search(len(s), func(i int) bool { return s[i].b > x.a })
	for ; i < len(s) && s[i].a < x.b; i++ {
		if a, b := max(s[i].a, x.a), min(s[i].b, x.b); a < b {
			out = append(out, iv{a, b})
		}
	}
	return out
}
