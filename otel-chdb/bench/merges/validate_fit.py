#!/usr/bin/env python3
"""How far can M(N) = a + b ln N be extrapolated? Fit on 50 <= N <= N_end/k,
predict M and R at N_end, compare with what was measured.  validate_fit.py results/<run> ..."""
import math, sys
from analyze import TABLE_OF, load
from fit import curve, lsq

for d in sys.argv[1:]:
    args, run, pl, ql, ins, samples = load(d)
    for name in args["tables"].split(","):
        pts = curve(pl, TABLE_OF[name])
        n_end, R_end, M_end = pts[-1]
        for k in (4, 8):
            use = [(math.log(n), r, m) for n, r, m in pts if 50 <= n <= n_end / k]
            if len(use) < 10:
                continue
            a, b = lsq([u[0] for u in use], [u[1] for u in use])
            am, bm = lsq([u[0] for u in use], [u[2] for u in use])
            x = math.log(n_end)
            print(f"{d.split('/')[-1]:>16} {name:>22} fit N<={n_end // k:>5}, predict N={n_end}: "
                  f"R {a + b * x:.2f} vs {R_end:.2f} measured; M {am + bm * x:.2f} vs {M_end:.2f} µs ({(am + bm * x) / M_end - 1:+.0%})")
