using System.Globalization;
using System.Text.Json;
using Oscope.Forwarder.Core;

namespace Oscope.Forwarder;

/// <summary>
/// The counters as an OTLP/JSON logs request (research/entra-ingress.md §4.4): sent
/// through the same queue as the tools' requests, so drops appear in the person's own
/// namespace (H-E4 counted, TM-E1 shown). Counters are cumulative since start, so a lost
/// report is superseded by the next; a crash's loss shows as the last report's held
/// entries with no later report (nothing is persisted to count it, D37).
/// </summary>
public static class CountersReport
{
    public const string EventName = "oscope.forwarder.counters";

    public static byte[] Json(CountersSnapshot s, IReadOnlyDictionary<string, long> tokenFailures, DateTimeOffset at)
    {
        ArgumentNullException.ThrowIfNull(s);
        ArgumentNullException.ThrowIfNull(tokenFailures);
        var attrs = new List<(string Key, long Value)>
        {
            ("accepted", s.Accepted), ("committed", s.Committed), ("committed_after_unknown", s.CommittedAfterUnknown),
            ("attempts", s.Attempts), ("unknown_outcomes", s.UnknownOutcomes), ("held_entries", s.HeldEntries),
            ("held_bytes", s.HeldBytes), ("oldest_age_s", (long)s.OldestAge.TotalSeconds), ("clock_regressions", s.ClockRegressions),
        };
        attrs.AddRange(s.Refused.Select(kv => ("refused." + kv.Key.ToString().ToLowerInvariant(), kv.Value)));
        attrs.AddRange(s.Dropped.Select(kv => ("dropped." + kv.Key, kv.Value)));
        attrs.AddRange(s.DroppedMaybeLanded.Select(kv => ("dropped_maybe_landed." + kv.Key, kv.Value)));
        attrs.AddRange(tokenFailures.Select(kv => ("token_failures." + kv.Key.ToLowerInvariant(), kv.Value)));
        using var ms = new MemoryStream();
        using (var w = new Utf8JsonWriter(ms))
        {
            w.WriteStartObject();
            w.WriteStartArray("resourceLogs");
            w.WriteStartObject();
            w.WriteStartObject("resource");
            w.WriteStartArray("attributes");
            Attr(w, "service.name", "oscope-forwarder");
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteStartArray("scopeLogs");
            w.WriteStartObject();
            w.WriteStartObject("scope");
            w.WriteString("name", "oscope.forwarder");
            w.WriteEndObject();
            w.WriteStartArray("logRecords");
            w.WriteStartObject();
            var nanos = (at.ToUnixTimeMilliseconds() * 1_000_000L).ToString(CultureInfo.InvariantCulture);
            w.WriteString("timeUnixNano", nanos);
            w.WriteString("observedTimeUnixNano", nanos);
            w.WriteString("eventName", EventName);
            w.WriteStartObject("body");
            w.WriteString("stringValue", EventName);
            w.WriteEndObject();
            w.WriteStartArray("attributes");
            foreach (var (k, v) in attrs.OrderBy(a => a.Key, StringComparer.Ordinal))
            {
                w.WriteStartObject();
                w.WriteString("key", "oscope.forwarder." + k);
                w.WriteStartObject("value");
                w.WriteString("intValue", v.ToString(CultureInfo.InvariantCulture));
                w.WriteEndObject();
                w.WriteEndObject();
            }
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
            w.WriteEndArray();
            w.WriteEndObject();
        }
        return ms.ToArray();
    }

    private static void Attr(Utf8JsonWriter w, string key, string value)
    {
        w.WriteStartObject();
        w.WriteString("key", key);
        w.WriteStartObject("value");
        w.WriteString("stringValue", value);
        w.WriteEndObject();
        w.WriteEndObject();
    }
}
