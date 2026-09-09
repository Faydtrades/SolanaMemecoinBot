# Codex Model Routing and Scope Policy v1 — LOCKED OWNER POLICY

Status: **LOCKED OWNER POLICY**

Applies to future Codex/CHIEF/worker planning, implementation and review for this project unless the owner explicitly overrides it.

## 1. Core objective

Optimize for the fastest path to a correct, high-quality finished product — not for minimum quota consumption.

Higher model intelligence is used to improve correctness, design judgment, root-cause analysis, first-pass quality and review depth. It does **not** authorize broader scope, more abstraction, more code, more frameworks or more tests than the production transition actually requires.

Guiding rule:

> **Use more intelligence, not more complexity.**

## 2. Quality-first, scope-disciplined engineering

All Codex tasks must preserve these principles:

- **Quality first.** Do not choose a weaker model/reasoning tier merely to conserve quota on meaningful engineering work.
- **Scope discipline always.** Solve the smallest complete problem that closes the required production transition.
- **Reuse before build.** Inspect relevant existing mechanisms first; do not duplicate persistence, safety, wrappers, providers, frameworks or lifecycle machinery that already satisfies the need.
- **No speculative engineering.** Do not add abstractions, wrappers, framework layers, generic extension points or "future-proofing" without a concrete required consumer.
- **No artificial breadth.** Do not inspect or change unrelated subsystems merely because they are adjacent in the repository.
- **Do not duplicate safety.** Existing adequate safety mechanisms are reused. New safety is added only when a real uncovered invariant requires it. Do not create safety mechanisms that conflict with the intended product lifecycle.
- **Higher reasoning does not imply larger diffs.** A stronger model should be used to identify the minimum correct change and the exact stopping point.
- **Escalate intelligence before scope.** If a task proves harder than expected, first improve model/reasoning and root-cause understanding; do not automatically broaden architecture or rewrite surrounding systems.
- **Evidence must be useful.** Avoid duplicate reads, repeated tests, full regressions without cross-surface justification, or parallel agents performing substantially the same work.
- **Prompts stay calibrated.** Give CHIEF/worker the context, invariants, boundaries and acceptance criteria needed for the task, but do not inflate prompts with unrelated hypotheses, exhaustive repository tours or unnecessary framework instructions.

## 3. Available model/reasoning pool

The working pool is:

- Sol High
- Sol Extra High
- Sol Ultra
- Astra Medium
- Astra High
- Astra Extra High
- Astra Ultra
- Luna Medium

Routing is based on expected quality gain and downstream risk, not a quota-minimization ladder.

## 4. Default routing

### Astra Extra High — normal engineering baseline

Use **Astra Extra High by default** for meaningful coding/building where the architecture and contract are already sufficiently defined.

Typical examples:

- bounded production implementation;
- cross-file integration inside a known work package;
- substantive fixtures/tests;
- nontrivial bug fixes;
- implementation corrections after CHIEF review;
- stateful code where mistakes could create later rework.

### Astra Ultra — low threshold for critical work

Use **Astra Ultra by default** when correctness depends materially on deeper reasoning, cross-component consequences or difficult state/lifecycle semantics.

The threshold for Ultra is intentionally low.

Typical examples:

- production/closure architecture;
- architecture review/freeze;
- source-of-truth and identity/lineage design;
- durable state machines;
- settlement/accounting/position truth;
- risk/capital/funding semantics;
- signing/send authority;
- unknown/partial/finality handling;
- crash/reboot/network recovery;
- unattended runtime composition;
- difficult root-cause debugging;
- package-level SYSTEM TRANSITION PASS;
- integrated full-stack acceptance design/review;
- final production-readiness review;
- any task where a subtle local decision could create a hidden downstream production gap.

Use Astra Extra High instead of Ultra when the task is important but the additional Ultra reasoning is unlikely to produce a meaningful quality or rework reduction.

### Astra High / Astra Medium

Use these when the task is genuinely simpler and the contract is already explicit.

- **Astra High:** straightforward bounded engineering where Extra High/Ultra has little expected marginal benefit.
- **Astra Medium:** mechanical code/test/document changes that still benefit from repository understanding but carry low architectural/state risk.

### Sol High / Extra High / Ultra

Sol is available as both an implementation model and, especially, an independent reasoning perspective.

Use Sol when:

- a strong independent second opinion is valuable;
- architecture/review benefits from model diversity;
- the task profile is better suited to Sol;
- Astra-level capability would add little but Sol can complete the bounded work cleanly.

**Sol Extra High/Ultra** are particularly suitable for independent critical review rather than duplicating the same implementation work without purpose.

**Sol High** is acceptable for simple, well-bounded technical work where stronger models offer no meaningful gain.

### Luna Medium

Reserve **Luna Medium** for genuinely mechanical/clerical work, such as:

- simple documentation formatting;
- log/result organization;
- command transcription;
- small deterministic text/config changes;
- other low-risk tasks where deeper reasoning would not improve the result.

Do not use Luna Medium for production-critical design, state logic, safety, recovery, accounting, signing or substantive implementation.

## 5. CHIEF / worker routing

Default expectations:

- Critical architecture/design CHIEF work: **Astra Ultra**.
- Independent critical architecture review: **Astra Ultra**, with Sol Extra High/Ultra available when a genuinely independent second perspective is useful.
- Normal consequential worker implementation: **Astra Extra High**.
- High-risk worker implementation involving state/risk/recovery/signing/settlement or difficult cross-component integration: **Astra Ultra**.
- Local CHIEF review: **Astra Extra High** unless risk/complexity warrants Ultra.
- Package/freeze/SYSTEM TRANSITION review: **Astra Ultra** by default.

Model selection may move downward when the marginal quality benefit is genuinely negligible; it must not move downward merely because quota is being consumed.

## 6. Quota and efficiency policy

Do **not** instruct CHIEF or workers to be "stingy" with quota or to choose weaker reasoning to save allowance.

Efficiency means:

- avoid rereading large material without a reason;
- avoid duplicate agents doing the same job;
- avoid broad regressions after every micro-change;
- avoid oversized prompts;
- avoid irrelevant repository inspection;
- avoid unnecessary abstractions/frameworks;
- stop when the required transition and acceptance evidence are complete.

It does **not** mean sacrificing model capability on critical work.

## 7. First 50% calibration checkpoint

When the owner reaches approximately **50% of the first Pro $200 weekly Codex quota**, pause only for a planning calibration — not a hard project gate.

Review actual:

- project/work-package progress;
- Ultra vs Extra High consumption;
- correction-loop frequency;
- rework avoided or incurred;
- architecture/review quality;
- expected remaining work.

Then choose one of three directions:

1. More quota headroom than expected -> increase Ultra use where it may improve quality/speed.
2. Consumption roughly on plan -> continue current routing.
3. Consumption materially ahead of useful progress -> move clearly straightforward work from Ultra to Extra High/High, while keeping architecture, state, safety, recovery and critical review at the required high tier.

The checkpoint exists to calibrate routing from evidence, not to pre-emptively reduce quality.

## 8. Relationship to project acceptance discipline

This model policy does not replace project engineering controls.

Existing rules such as bounded tasks, LOCAL PASS, SYSTEM TRANSITION PASS, lifecycle ownership, fail-closed unknown state and explicit human real-capital gates remain authoritative.

Stronger models are used to execute those controls better; they are not a substitute for them.

## 9. Standing default

Unless the owner explicitly changes this policy, all future prompts and routing decisions should assume:

- **Astra is the standard coding/building family**;
- **Astra Extra High is the normal meaningful-engineering baseline**;
- **Astra Ultra has a low threshold and is the default for critical/cross-component work**;
- lower tiers are used only where stronger intelligence is unlikely to improve the result;
- quota conservation never justifies weakening critical engineering;
- scope remains narrow, evidence-driven and production-transition-focused.