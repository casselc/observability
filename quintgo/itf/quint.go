package itf

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"unicode"
)

// Quint renders the value as a Quint literal expression (the same text as
// String): Set(...), Map(k -> v), { f: v }, (a, b), [a, b], Tag / Tag(v).
func (v Value) Quint() string { return v.String() }

// MarshalJSON encodes the value in ITF.
func (v Value) MarshalJSON() ([]byte, error) { return json.Marshal(v.itf()) }

func (v Value) itf() any {
	switch v.Kind {
	case Int:
		return map[string]string{"#bigint": v.I.String()}
	case Bool:
		return v.B
	case Str:
		return v.S
	case List, Set, Tuple:
		es := make([]any, len(v.Elems))
		for i, e := range v.Elems {
			es[i] = e.itf()
		}
		switch v.Kind {
		case Set:
			return map[string]any{"#set": es}
		case Tuple:
			return map[string]any{"#tup": es}
		}
		return es
	case Map:
		ps := make([]any, len(v.Pairs))
		for i, p := range v.Pairs {
			ps[i] = []any{p[0].itf(), p[1].itf()}
		}
		return map[string]any{"#map": ps}
	case Record:
		m := map[string]any{}
		for k, f := range v.Fields {
			m[k] = f.itf()
		}
		return m
	case Variant:
		inner := Value{Kind: Tuple}
		if v.Inner != nil {
			inner = *v.Inner
		}
		return map[string]any{"tag": v.Tag, "value": inner.itf()}
	}
	return nil
}

// Some wraps a value as Quint's Some(v), as mbt::nondetPicks holds picks.
func Some(v Value) Value { return Value{Kind: Variant, Tag: "Some", Inner: &v} }

// FromGo converts an observed scalar (string, int64, int, bool) to a Value.
func FromGo(x any) (Value, error) {
	switch v := x.(type) {
	case string:
		return Value{Kind: Str, S: v}, nil
	case bool:
		return Value{Kind: Bool, B: v}, nil
	case int64:
		return Value{Kind: Int, I: big.NewInt(v)}, nil
	case int:
		return Value{Kind: Int, I: big.NewInt(int64(v))}, nil
	case float64:
		return Value{Kind: Int, I: big.NewInt(int64(v))}, nil
	}
	return Value{}, fmt.Errorf("unsupported value %T", x)
}

// ParseQuint parses a Quint literal: integers, strings, booleans, records,
// Set(...), List(...) / [...], Map(k -> v, ...), tuples, and variants
// (an upper-case constructor, bare or applied to one argument). Anything
// else, such as a call of a user operator, is an error: such expressions
// need Quint to evaluate them.
func ParseQuint(src string) (Value, error) {
	p := &qparser{s: src}
	v, err := p.expr()
	if err != nil {
		return Value{}, err
	}
	p.ws()
	if p.i < len(p.s) {
		return Value{}, p.errf("unexpected %q", p.s[p.i:])
	}
	return v, nil
}

type qparser struct {
	s string
	i int
}

func (p *qparser) errf(f string, a ...any) error {
	return fmt.Errorf("quint literal %q at %d: %s", p.s, p.i, fmt.Sprintf(f, a...))
}

func (p *qparser) ws() {
	for p.i < len(p.s) && unicode.IsSpace(rune(p.s[p.i])) {
		p.i++
	}
}

func (p *qparser) eat(tok string) bool {
	p.ws()
	if strings.HasPrefix(p.s[p.i:], tok) {
		p.i += len(tok)
		return true
	}
	return false
}

func (p *qparser) ident() string {
	p.ws()
	j := p.i
	for j < len(p.s) && (p.s[j] == '_' || unicode.IsLetter(rune(p.s[j])) || (j > p.i && unicode.IsDigit(rune(p.s[j])))) {
		j++
	}
	id := p.s[p.i:j]
	p.i = j
	return id
}

// list parses "e, e, ...)" (or with the given closer) after the opener.
func (p *qparser) list(closer string) ([]Value, error) {
	var out []Value
	if p.eat(closer) {
		return out, nil
	}
	for {
		v, err := p.expr()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
		if p.eat(closer) {
			return out, nil
		}
		if !p.eat(",") {
			return nil, p.errf("expected , or %s", closer)
		}
		if p.eat(closer) { // trailing comma
			return out, nil
		}
	}
}

func (p *qparser) expr() (Value, error) {
	p.ws()
	if p.i >= len(p.s) {
		return Value{}, p.errf("unexpected end")
	}
	c := p.s[p.i]
	switch {
	case c == '-' || (c >= '0' && c <= '9'):
		j := p.i + 1
		for j < len(p.s) && (p.s[j] >= '0' && p.s[j] <= '9' || p.s[j] == '_') {
			j++
		}
		n, ok := new(big.Int).SetString(strings.ReplaceAll(p.s[p.i:j], "_", ""), 10)
		if !ok {
			return Value{}, p.errf("bad integer")
		}
		p.i = j
		return Value{Kind: Int, I: n}, nil
	case c == '"':
		j := p.i + 1
		for j < len(p.s) && p.s[j] != '"' {
			if p.s[j] == '\\' {
				j++
			}
			j++
		}
		if j >= len(p.s) {
			return Value{}, p.errf("unterminated string")
		}
		str, err := strconv.Unquote(p.s[p.i : j+1])
		if err != nil {
			return Value{}, p.errf("bad string")
		}
		p.i = j + 1
		return Value{Kind: Str, S: str}, nil
	case c == '{':
		p.i++
		v := Value{Kind: Record, Fields: map[string]Value{}}
		if p.eat("}") {
			return v, nil
		}
		for {
			name := p.ident()
			if name == "" || !p.eat(":") {
				return Value{}, p.errf("expected field: value")
			}
			f, err := p.expr()
			if err != nil {
				return Value{}, err
			}
			v.Fields[name] = f
			if p.eat("}") {
				return v, nil
			}
			if !p.eat(",") {
				return Value{}, p.errf("expected , or }")
			}
			if p.eat("}") {
				return v, nil
			}
		}
	case c == '[':
		p.i++
		es, err := p.list("]")
		return Value{Kind: List, Elems: es}, err
	case c == '(':
		p.i++
		es, err := p.list(")")
		if err != nil {
			return Value{}, err
		}
		if len(es) == 1 {
			return es[0], nil // parenthesised expression
		}
		return Value{Kind: Tuple, Elems: es}, nil
	}
	id := p.ident()
	switch id {
	case "":
		return Value{}, p.errf("unexpected %q", string(c))
	case "true", "false":
		return Value{Kind: Bool, B: id == "true"}, nil
	case "Set", "List":
		if !p.eat("(") {
			return Value{}, p.errf("expected ( after %s", id)
		}
		es, err := p.list(")")
		k := Set
		if id == "List" {
			k = List
		}
		return Value{Kind: k, Elems: es}, err
	case "Map":
		if !p.eat("(") {
			return Value{}, p.errf("expected ( after Map")
		}
		v := Value{Kind: Map}
		if p.eat(")") {
			return v, nil
		}
		for {
			k, err := p.expr()
			if err != nil {
				return Value{}, err
			}
			if !p.eat("->") {
				return Value{}, p.errf("expected ->")
			}
			x, err := p.expr()
			if err != nil {
				return Value{}, err
			}
			v.Pairs = append(v.Pairs, [2]Value{k, x})
			if p.eat(")") {
				return v, nil
			}
			if !p.eat(",") {
				return Value{}, p.errf("expected , or )")
			}
		}
	}
	if !unicode.IsUpper(rune(id[0])) {
		return Value{}, p.errf("%s is not a literal (only constructors, Set, List, Map and records are); evaluate it in Quint or write the literal", id)
	}
	inner := Value{Kind: Tuple}
	if p.eat("(") {
		es, err := p.list(")")
		if err != nil {
			return Value{}, err
		}
		if len(es) != 1 {
			inner = Value{Kind: Tuple, Elems: es}
		} else {
			inner = es[0]
		}
	}
	return Value{Kind: Variant, Tag: id, Inner: &inner}, nil
}
