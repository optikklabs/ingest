# CLAUDE.md

**CRITICAL RULE**: You MUST refer to and update `CODE_INDEX.md` after every architectural or structural task to ensure the codebase index remains accurate.

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.


## 5. Engineering Principles
- No God files/functions/variables: each file/function should own a single responsibility 
- DRY: Single authoritative representation for each piece of knowledge.
- SRP: A class/package should have one reason to change.
- OCP: Open for extension, closed for modification.
- LSP: Subtypes must be substitutable for their base types.
- ISP: Many small, focused interfaces over one general-purpose interface.
- DIP: Depend on abstractions, not concretions.


## 6. Code Comments
- Comments should be of single line and not more 80 characters, concise explainable

---

## 7. Architecture First

Always optimize for long-term maintainability over short-term convenience.

Before introducing new code, evaluate whether it:

- fits the existing architecture
- respects package boundaries
- introduces unnecessary coupling
- belongs in the correct layer
- increases future maintenance cost

Challenge existing designs instead of assuming they are correct.

If a simpler architecture exists, recommend it.

---

## 8. Package Ownership

Every package should have a single responsibility.

Business logic should never leak into:

- HTTP handlers
- database repositories
- configuration
- transport models

Prefer the following flow:

```
HTTP Handler
    ↓
Service
    ↓
Repository
    ↓
Database
```

Repositories fetch data.

Services implement business logic.

Handlers translate HTTP requests and responses.

---

## 9. Database & Query Performance

This service is query-heavy.

Assume every endpoint may execute against billions of telemetry records.

Always evaluate:

- query complexity
- ClickHouse execution cost
- unnecessary allocations
- repeated database calls
- N+1 queries
- missing LIMITs
- unnecessary scans
- excessive joins
- unnecessary deserialization

Never sacrifice query performance for cleaner-looking code.

---

## 10. API Design

APIs should be:

- consistent
- predictable
- versioned
- backwards compatible

Avoid:

- inconsistent response shapes
- hidden breaking changes
- ambiguous field names
- leaking database models directly to clients

DTOs should represent the API contract, not database schemas.

---

## 11. Scalability & Production Readiness

Assume this service will eventually support:

- thousands of organizations
- millions of API requests
- billions of telemetry rows
- concurrent dashboard queries
- alert evaluations
- background jobs
- enterprise customers

Prefer designs that minimize:

- lock contention
- memory allocations
- unnecessary copies
- synchronous bottlenecks
- repeated parsing
- repeated query planning

Think about production behaviour, not just correctness.

---

## 12. Go Best Practices

Write idiomatic Go.

Prefer:

- small packages
- small interfaces
- composition over inheritance
- explicit dependencies
- context propagation
- error wrapping where useful
- early returns
- zero-value friendly types

Avoid:

- global mutable state
- massive interfaces
- utility packages containing unrelated code
- reflection unless absolutely necessary
- premature abstractions
- unnecessary goroutines

Review every implementation before considering it complete.

Ask yourself:

- Is this the simplest correct solution?
- Is this idiomatic Go?
- Will this still scale in two years?
- Would another senior Go engineer approve this?
- Is every abstraction justified?
- Does this improve or degrade maintainability?

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.