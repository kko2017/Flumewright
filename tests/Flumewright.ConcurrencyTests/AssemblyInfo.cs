// Flumewright.ConcurrencyTests is a COYOTE-ONLY assembly.
// It is listed in coyote.json and is binary-rewritten by Microsoft.Coyote.
// Rewriting affects EVERY Task in the assembly: do NOT add ordinary xUnit
// tests that spawn their own Tasks here — their Tasks would be hijacked by
// Coyote's scheduler. Ordinary concurrency tests (e.g. the start-gate herd
// tests) belong in Flumewright.UnitTests, which is NOT rewritten.
//
// NOTE: the Coyote rewrite is NOT run on ordinary build. To run these tests
// with real systematic exploration you must rewrite first, then test WITHOUT
// rebuilding (a rebuild would overwrite the rewritten assembly):
//   dotnet build -c Release tests/Flumewright.ConcurrencyTests
//   dotnet tool restore
//   dotnet build tests/Flumewright.ConcurrencyTests -t:CoyoteRewriteTarget -c Release --no-build
//   dotnet test tests/Flumewright.ConcurrencyTests -c Release --no-build
// A plain `dotnet test` here will NOT be rewritten and will report 1 iteration
// (the FIX-016 trap) — always verify the explored-iteration count (~100).
