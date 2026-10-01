using CsCheck;
using Oscope.Forwarder.Core;
using Xunit;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// Stateful, model-based tests of <see cref="ForwarderCore"/> (VERIFICATION.md §1 "SM"):
/// CsCheck draws a sequence of operations (a tool's request, an attempt, an attempt's
/// outcome — committed, refused, 401, 429, a lost answer —, time passing, a stop), runs it
/// against the core and the reference model <see cref="QueueModel"/>, and checks after
/// every step that they agree and that the invariants hold. Swarm testing: each case
/// switches some outcome kinds off, so long runs of one fault are reached. A failure
/// shrinks to a short story.
/// </summary>
public class CoreModelTests
{
    internal abstract record Op;
    internal sealed record OfferOp(int Size, int Who) : Op;
    internal sealed record NextOp : Op;
    internal sealed record CompleteOp(int Pick, OutcomeKind Kind, int RetryAfterMs) : Op;
    internal sealed record AdvanceOp(int Ms) : Op;
    internal sealed record ShutdownOp : Op;
    internal sealed record AbandonOp : Op;

    internal static readonly ForwarderOptions Small = new ForwarderOptions
    {
        MaxQueueBytes = 1000, MaxEntries = 6, MaxEntryBytes = 400, MaxAttempts = 5, MaxAge = TimeSpan.FromSeconds(12),
        BackoffBase = TimeSpan.FromMilliseconds(100), BackoffCap = TimeSpan.FromSeconds(2), MaxRetryAfter = TimeSpan.FromSeconds(3),
        MaxInFlight = 2,
    }.Validate();

    internal static readonly AccountKey Alice = new("11111111-1111-4111-8111-111111111111", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa");
    internal static readonly AccountKey Bob = new("11111111-1111-4111-8111-111111111111", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb");

    private static AccountKey? Who(int w) => w switch { 0 or 1 => Alice, 2 => Bob, _ => null };

    private static Gen<Op> GenOp(bool shutdowns)
    {
        var gens = new List<(int, IGen<Op>)>
        {
            (5, Gen.Select(Gen.Int[0, 450], Gen.Int[0, 3], (s, w) => (Op)new OfferOp(s, w))),
            (5, Gen.Const((Op)new NextOp())),
            (5, Gen.Select(Gen.Int[0, 7], Gen.Enum<OutcomeKind>(), Gen.Int[0, 4000], (p, k, r) => (Op)new CompleteOp(p, k, r))),
            (3, Gen.Select(Gen.Int[0, 5000], ms => (Op)new AdvanceOp(ms))),
        };
        if (shutdowns)
        {
            gens.Add((1, Gen.Const((Op)new ShutdownOp())));
            gens.Add((1, Gen.Const((Op)new AbandonOp())));
        }
        return Gen.Frequency<Op>(gens.ToArray());
    }

    /// <summary>A case: which outcome kinds are switched on (swarm), and the operations.</summary>
    private static Gen<(int Mask, Op[] Ops)> GenCase(bool shutdowns) =>
        Gen.Select(Gen.Int[1, 255], GenOp(shutdowns).Array[1, 80], (m, ops) => (m, ops));

    /// <summary>Runs one case; throws at the first step where the core and the model disagree or an invariant breaks.</summary>
    internal static void Run(int mask, Op[] ops)
    {
        var core = new ForwarderCore(Small, () => 0.5);
        var model = new QueueModel(Small, 0.5);
        var now = TimeSpan.Zero;
        var bodies = new Dictionary<long, byte[]>();   // the array each entry was accepted with
        var copies = new Dictionary<long, byte[]>();   // and a copy of its contents then
        var accounts = new Dictionary<long, AccountKey>();
        var resolved = new HashSet<long>();
        for (var step = 0; step < ops.Length; step++)
        {
            var op = ops[step];
            void Fail(string what) => throw new InvalidOperationException($"step {step} {op}: {what}");
            switch (op)
            {
                case OfferOp(var size, var w):
                {
                    var body = new byte[size];
                    new Random(step * 7919 + size).NextBytes(body);
                    var a = core.Offer(now, Who(w), new ForwardRequest("/v1/traces", "application/x-protobuf", null, body));
                    var m = model.Offer(now, Who(w), size);
                    if (a != m) Fail($"admission {a} != model {m}");
                    if (a.Accepted)
                    {
                        bodies[a.Id] = body;
                        copies[a.Id] = (byte[])body.Clone();
                        accounts[a.Id] = Who(w)!.Value;
                    }
                    break;
                }
                case NextOp:
                {
                    var a = core.Next(now);
                    var m = model.Next(now);
                    if ((a is null) != (m is null)) Fail($"attempt {a} != model {m?.Id}");
                    if (a is null || m is null) break;
                    if (a.Id != m.Id || a.Number != m.Attempts || a.ForceRefresh != m.Force)
                        Fail($"attempt {a.Id}#{a.Number} force={a.ForceRefresh} != model {m.Id}#{m.Attempts} force={m.Force}");
                    if (resolved.Contains(a.Id)) Fail($"entry {a.Id} attempted after it was resolved");
                    // LS-E3: a retry is a copy — the same array, with the same contents.
                    if (!ReferenceEquals(a.Request.Body, bodies[a.Id]) || !a.Request.Body.AsSpan().SequenceEqual(copies[a.Id]))
                        Fail($"entry {a.Id}: the attempt's bytes are not the accepted bytes");
                    // R-E6, LS-E5: only under the account it was accepted under.
                    if (a.Account != accounts[a.Id]) Fail($"entry {a.Id}: sent as {a.Account}, accepted as {accounts[a.Id]}");
                    if (a.Number > Small.MaxAttempts) Fail($"entry {a.Id}: attempt {a.Number} over the bound");
                    break;
                }
                case CompleteOp(var pick, var kind, var ra):
                {
                    var flying = model.Held.Where(e => e.InFlight).OrderBy(e => e.Id).ToList();
                    if (flying.Count == 0) break;
                    var id = flying[pick % flying.Count].Id;
                    if ((mask & (1 << (int)kind)) == 0) kind = OutcomeKind.Committed;
                    var o = new Outcome(kind, ra > 3500 ? null : TimeSpan.FromMilliseconds(ra));
                    core.Complete(now, id, o);
                    model.Complete(now, id, o);
                    break;
                }
                case AdvanceOp(var ms):
                    now += TimeSpan.FromMilliseconds(ms);
                    core.Tick(now);
                    model.Tick(now);
                    break;
                case ShutdownOp:
                    core.BeginShutdown(now);
                    model.BeginShutdown();
                    break;
                case AbandonOp:
                    core.Abandon(now);
                    model.Abandon();
                    break;
            }
            foreach (var id in bodies.Keys)
                if (!model.Held.Any(e => e.Id == id)) resolved.Add(id);
            Check(core, model, Fail);
        }
    }

    private static void Check(ForwarderCore core, QueueModel model, Action<string> fail)
    {
        var s = core.Snapshot();
        // H-E4: every acknowledged entry is committed, counted as dropped, or held.
        if (!s.Balances) fail($"ledger: accepted {s.Accepted} != committed {s.Committed} + dropped {s.DroppedTotal} + held {s.HeldEntries}");
        // R-E7: bounded memory, in the queue and in flight together.
        if (s.HeldBytes > Small.MaxQueueBytes || s.HeldEntries > Small.MaxEntries || s.InFlight > Small.MaxInFlight)
            fail($"bounds: {s.HeldBytes} bytes, {s.HeldEntries} entries, {s.InFlight} in flight");
        if (s.Accepted != model.Accepted || s.Committed != model.Committed || s.CommittedAfterUnknown != model.CommittedAfterUnknown
            || s.Attempts != model.Attempts || s.UnknownOutcomes != model.Unknown || s.HeldEntries != model.Held.Count
            || s.HeldBytes != model.HeldBytes || s.InFlight != model.InFlight)
            fail($"counters differ from the model: {s}");
        if (!Same(s.Refused, model.Refused) || !Same(s.Dropped, model.Dropped) || !Same(s.DroppedMaybeLanded, model.DroppedMaybe))
            fail($"refusal or drop counters differ: core {Show(s.Refused)} {Show(s.Dropped)} {Show(s.DroppedMaybeLanded)}; " +
                 $"model {Show(model.Refused)} {Show(model.Dropped)} {Show(model.DroppedMaybe)}");
        if (s.ClockRegressions != 0) fail("the test's clock never goes back");
    }

    private static bool Same<K>(IReadOnlyDictionary<K, long> a, IReadOnlyDictionary<K, long> b) where K : notnull =>
        a.Count == b.Count && a.All(kv => b.TryGetValue(kv.Key, out var v) && v == kv.Value);

    private static string Show<K>(IReadOnlyDictionary<K, long> d) where K : notnull =>
        "{" + string.Join(",", d.OrderBy(kv => kv.Key.ToString(), StringComparer.Ordinal).Select(kv => $"{kv.Key}={kv.Value}")) + "}";

    private static string Print((int Mask, Op[] Ops) c) => $"mask={c.Mask} ops=[{string.Join("; ", c.Ops.Select(o => o.ToString()))}]";

    [Fact]
    public void The_core_agrees_with_the_reference_model_under_every_fault_order()
    {
        OscopeTrace.Covers("SM", "H-E4 H-E6 R-E6 R-E7 LS-E3 LS-E5 UCA-E6 UCA-E7 CAST-39 CAST-50");
        GenCase(shutdowns: false).Sample(c => Run(c.Mask, c.Ops), iter: 3000, print: Print);
    }

    [Fact]
    public void Stops_and_abandons_count_every_held_entry()
    {
        OscopeTrace.Covers("SM", "H-E4 R-E7 UCA-E7");
        GenCase(shutdowns: true).Sample(c => Run(c.Mask, c.Ops), iter: 2000, print: Print);
    }
}
