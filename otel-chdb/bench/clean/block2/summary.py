#!/usr/bin/env python3
"""Block 2 per table: the per-repetition fits (fit.json, from analyze.py
--json), median [min–max] over the 5 repetitions, next to the earlier
loaded-box values (metrics-layout/README.md: marginal µs/point per type and
fixed ms per object; bench/central/REPORT.md: ~5 µs per span all-in).

  summary.py > summary.md
"""
import json, os, statistics as st

HERE = os.path.dirname(os.path.abspath(__file__))
EARLIER_U = {"otel_metrics_number_points": "0.72 sum / 0.64 gauge", "otel_metrics_histogram_points": "1.8",
             "otel_metrics_exponential_histogram_points": "2.7", "otel_metrics_summary_points": "1.3", "otel_metrics_series": "13.2 (0.11 per point at 1/120)",
             "otel_metrics_sum": "7.85 (blend)", "otel_metrics_gauge": "7.85 (blend)", "otel_metrics_histogram": "7.85 (blend)",
             "otel_metrics_exponential_histogram": "7.85 (blend)", "otel_metrics_summary": "7.85 (blend)", "otel_traces": "~5 all-in", "otel_logs": "–"}
EARLIER_F = {"otel_metrics_number_points": "16.3 (B)", "otel_metrics_histogram_points": "16.3 (B)", "otel_metrics_exponential_histogram_points": "16.3 (B)",
             "otel_metrics_summary_points": "16.3 (B)", "otel_metrics_series": "16.3 (B)", "otel_metrics_sum": "18.3 (A)", "otel_metrics_gauge": "18.3 (A)",
             "otel_metrics_histogram": "18.3 (A)", "otel_metrics_exponential_histogram": "18.3 (A)", "otel_metrics_summary": "18.3 (A)",
             "otel_traces": "–", "otel_logs": "–"}


def mm(xs, f="{:.2f}"):
    xs = sorted(xs)
    return f"{f.format(st.median(xs))} [{f.format(xs[0])}–{f.format(xs[-1])}]"


def main():
    fit = json.load(open(os.path.join(HERE, "fit.json")))
    print("| table | marginal µs/row, clean | earlier µs/row | fixed ms per 1-object statement (F_stmt + F_obj), clean | earlier ms | "
          "fixed ms per object at 32 per statement (F_stmt/32 + F_obj) |")
    print("|---|---|---|---|---|---|")
    for t in sorted(fit):
        pr = fit[t]["per_rep"]
        print(f"| {t} | {mm([p['u_row_us'] for p in pr])} | {EARLIER_U.get(t, '–')} | {mm([p['F_stmt_ms'] + p['F_obj_ms'] for p in pr], '{:.1f}')} "
              f"| {EARLIER_F.get(t, '–')} | {mm([p['F_stmt_ms'] / 32 + p['F_obj_ms'] for p in pr])} |")


if __name__ == "__main__":
    main()
