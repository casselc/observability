package binding

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// InstanceConstants reads the constant overrides of an instance module, i.e.
// a module of the form
//
//	module currentDesign {
//	  import edgePublish(PAYLOADS = Set(1, 2), ...).*
//	}
//
// and returns the instantiated module's name and "NAME = expr" overrides.
// (A generated module cannot `import currentDesign.*`: Quint does not
// re-export what an instance module imports.)
func InstanceConstants(spec, instance string) (module string, constants []string, err error) {
	src, err := os.ReadFile(spec)
	if err != nil {
		return "", nil, err
	}
	s := string(src)
	re := regexp.MustCompile(`(?s)module\s+` + regexp.QuoteMeta(instance) + `\s*\{\s*import\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
	loc := re.FindStringSubmatchIndex(s)
	if loc == nil {
		return "", nil, fmt.Errorf("%s: no module %s of the form `module %s { import M(...).* }`", spec, instance, instance)
	}
	module = s[loc[2]:loc[3]]
	i, depth, start := loc[1], 1, loc[1]
	var parts []string
	for ; i < len(s) && depth > 0; i++ {
		switch s[i] {
		case '(', '{', '[':
			depth++
		case ')', '}', ']':
			depth--
		case ',':
			if depth == 1 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return "", nil, fmt.Errorf("%s: unbalanced import in module %s", spec, instance)
	}
	parts = append(parts, s[start:i-1])
	for _, p := range parts {
		if p = strings.Join(strings.Fields(p), " "); p != "" {
			constants = append(constants, p)
		}
	}
	return module, constants, nil
}
