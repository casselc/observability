import json, sys, statistics as st, collections
rows = collections.defaultdict(list); loads=[]
for line in open(sys.argv[1]):
    d = json.loads(line)
    if 'load' in d: loads.append(float(d['load'])); continue
    rows[(d['Dest'], d['Signal'], d['Impl'])].append(d)
def f(vals, fmt='%.0f'):
    m = st.median(vals)
    return (fmt % m) + (' [' + fmt % min(vals) + '–' + fmt % max(vals) + ']' if len(vals) > 1 else '')
print('load avg during runs: median %.1f, range %.1f–%.1f' % (st.median(loads), min(loads), max(loads)))
print('| dest | signal | impl | runs | ms/batch (median of per-run medians) | k rows/s | CPU ms/batch | Go allocs/batch | Go MB alloc/batch | peak RSS MB | ready ms | object KB |')
print('|'+'---|'*12)
for k in sorted(rows):
    r = rows[k]
    ob = [x['ObjectBytes']/1024 for x in r if x.get('ObjectBytes')]
    print('| %s | %s | %s | %d | %s | %s | %s | %s | %s | %s | %s | %s |' % (k[0], k[1], k[2], len(r),
        f([x['MedianMS'] for x in r], '%.1f'), f([x['RowsPerSec']/1000 for x in r]), f([x['CPUMSPerBatch'] for x in r]),
        f([x['GoAllocsPerBatch'] for x in r]), f([x['GoBytesPerBatch']/1e6 for x in r], '%.1f'), f([x['MaxRSSMB'] for x in r]),
        f([x['ReadyMS'] for x in r]), f(ob) if ob else '–'))
