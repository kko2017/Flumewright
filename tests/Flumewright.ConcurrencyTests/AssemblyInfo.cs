// Flumewright.ConcurrencyTests is a COYOTE-ONLY assembly.
// It is listed in coyote.json and is binary-rewritten by Microsoft.Coyote.
// Rewriting affects EVERY Task in the assembly: do NOT add ordinary xUnit
// tests that spawn their own Tasks here — their Tasks would be hijacked by
// Coyote's scheduler. Ordinary concurrency tests (e.g. the start-gate herd
// tests) belong in Flumewright.UnitTests, which is NOT rewritten.
