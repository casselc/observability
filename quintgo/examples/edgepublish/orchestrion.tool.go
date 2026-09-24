// This file is how Orchestrion learns which aspects to weave: only quintgo's
// //quint:action and //quint:thread (no dd-trace-go integrations).

//go:build tools

package tools

import (
	_ "github.com/DataDog/orchestrion"
	_ "github.com/chucklehead-dev/oscope/spike/quintgo/qobs"
)
