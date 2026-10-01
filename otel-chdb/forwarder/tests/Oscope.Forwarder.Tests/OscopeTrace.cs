using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;

namespace Oscope.Forwarder.Tests;

/// <summary>
/// The .NET twin of tracetag.Covers (Go), oscope_trace::covers (Rust) and covers(t, …)
/// (node:test): first in a test, one line naming the VERIFICATION.md §1 technique and the
/// STPA/CAST ids it verifies:
/// <code>OscopeTrace.Covers("SM", "H-E4 R-E7 CAST-39");</code>
/// With OSCOPE_TRACE_OUT set it appends a claim (ids, technique, class, method, source
/// file) to "{OSCOPE_TRACE_OUT}.dotnet-claims.jsonl"; xUnit does not hand a test its own
/// outcome, so ci/trace/dotnet_trx.py joins the claims with the run's TRX results and
/// writes the records (same fields as the other helpers) with the outcome the runner saw.
/// A claim with no result is recorded as failed (CAST rows 41, 43, 53: an unrun test is
/// not evidence). Unset, it does nothing.
/// </summary>
public static class OscopeTrace
{
    public const string Env = "OSCOPE_TRACE_OUT";
    private static readonly Lock Gate = new();

    [MethodImpl(MethodImplOptions.NoInlining)]
    public static void Covers(string technique, string ids, [CallerMemberName] string func = "", [CallerFilePath] string file = "")
    {
        var outPath = Environment.GetEnvironmentVariable(Env);
        if (string.IsNullOrEmpty(outPath)) return;
        var line = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["ids"] = ids.Split([' ', ','], StringSplitOptions.RemoveEmptyEntries),
            ["technique"] = technique,
            ["class"] = TestClass() ?? "",
            ["func"] = func,
            ["file"] = RepoPath(file),
        });
        lock (Gate)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(outPath))!);
            File.AppendAllText(outPath + ".dotnet-claims.jsonl", line + "\n");
        }
    }

    /// <summary>The calling test's class (an async test's frame is its state machine's; its parent is the class).</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string? TestClass()
    {
        var t = new StackFrame(2, false).GetMethod()?.DeclaringType;
        while (t is not null && t.Name.StartsWith('<') && t.DeclaringType is not null) t = t.DeclaringType;
        return t?.FullName;
    }

    /// <summary>Relative to the repository root (the directory holding otel-chdb/), with forward slashes.</summary>
    internal static string RepoPath(string file)
    {
        var f = file.Replace('\\', '/');
        var i = f.LastIndexOf("/otel-chdb/", StringComparison.Ordinal);
        return i >= 0 ? f[(i + 1)..] : f;
    }
}
