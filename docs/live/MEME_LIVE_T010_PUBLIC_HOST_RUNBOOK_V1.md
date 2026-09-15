# T010 reviewed public DRY unattended host

Implementation status: `IMPLEMENTED_PENDING_PROJECT_REVIEW`.

This is engineering for a separately authorized T010 observation. No T010 has
been launched by its implementation or self-tests. Current external clock,
capacity evidence, owner approvals and project acceptance remain prerequisites;
this document does not supply them. The accepted collector and LIVE stores are
never host setup targets.

## Exact inputs and prior preparation

The entry is `scripts/live_t010_public_host_v0_1.py`. Use a separately reviewed
normal external Windows terminal and the reviewed interpreter/checkout. The
host neither initializes stores nor installs policies, arms grants, resets
Operations, installs a service or changes Task Scheduler or boot configuration.

An explicit JSON request has exactly these fields:

- `schema`: `MEME_LIVE_T010_PUBLIC_HOST_REQUEST_V1`.
- `profile`, `dossier`, `package`, `qualification`, `facts`, `handoff`,
  `resource_envelope`: each an
  absolute local JSON `path` and exact file `sha256`.
- `health`: explicit `startup_us`, `progress_us`, `termination_us` as accepted
  finite `HealthProfile` values.
- `journal_root`: an existing, separate local directory for this one run.
- `execution_review_reference`: the actual separate T010 execution review.

The exact PUBLIC_ENVIRONMENT_REVIEWED supported profile, original public
qualification evidence and accepted preflight package must agree. `facts` must
be the qualification's exact reviewed facts reference. Its externally reviewed
real public clock must match the package ClockPolicy before ownership is
acquired. Endpoint names, stored wall time and caller flags are not clock
qualification. JSON cannot select Python modules, callbacks or execution ports.
The constructed child contains ReviewedPublicFacts, the profile's public driver,
OperationsSupervisor and the original DRY RuntimeComposition. It cannot sign,
send, broadcast, create fills/inventory, or issue capital authority.

The handoff record has exactly `schema`, `profile_content_digest`,
`package_sha256`, `qualification_sha256`, `stores`, `operations_control` and
`review_reference`. Schema is
`MEME_LIVE_CURRENT_PUBLIC_DRY_HOST_HANDOFF_V1`. `stores` is the five exact current
`{kind,path,sha256}` records; `operations_control` is the complete current
Operations snapshot. The Operations record/file must still equal the original
qualified Operations store: no fresh generation, budget or store substitution.
This explicit current handoff allows separately approved policy/grant controls
to have been installed externally in Ledger after qualification. The host does
not install them. The child verifies the exact package policy/grant and every
original qualified terminal receipt and simulation identity before any step.
The current handoff is checked only at the initial launch; later original owner
mutations must not be compared with obsolete whole-file hashes.

Both policy and grant must have explicit current windows no longer than 14 hours,
with at least 12 hours remaining at the original host launch. The grant end must
not exceed the policy end. No window is issued or extended by the host.

## Commands

These examples are templates; substitute reviewed absolute paths and hashes.
They are not authorization to run a currently denied package.

```powershell
& '<reviewed-python>' -B '<checkout>\scripts\live_t010_public_host_v0_1.py' inspect --request '<request.json>' --sha256 '<sha256>'
& '<reviewed-python>' -B '<checkout>\scripts\live_t010_public_host_v0_1.py' launch --request '<request.json>' --sha256 '<sha256>' --execute-reviewed-t010
& '<reviewed-python>' -B '<checkout>\scripts\live_t010_public_host_v0_1.py' status --journal-root '<journal-root>'
& '<reviewed-python>' -B '<checkout>\scripts\live_t010_public_host_v0_1.py' stop --journal-root '<journal-root>'
& '<reviewed-python>' -B '<checkout>\scripts\live_t010_public_host_v0_1.py' reconcile --journal-root '<journal-root>'
```

`inspect` validates without observing public facts or launching. Structural
preflight is not authenticated external authorization. `launch` starts a
DETACHED_PROCESS with CREATE_NEW_PROCESS_GROUP, closed inherited handles and
CREATE_BREAKAWAY_FROM_JOB when the launcher is in a job. Startup returns a
bounded durable receipt or a held/unproven result. A held receipt timeout does
not imply that the process is absent; inspect the saved state and stop it by its
journal binding. `run` is the internal child entry and uses the same validations.

The final child audits remaining job limits, then enters the original
WindowsHostBoundary named mutex/job. Restrictive remaining job limits are denied.
A residual job with zero limit flags may remain under Windows containment.
QueryInformationJobObject(NULL) describes the current job; it does not prove
properties of all inaccessible ancestors. Tests demonstrate survival of the
exact launcher process dying, not survival of arbitrary sandbox/app teardown.
Use the reviewed external terminal for a production observation. No PID search,
PID adoption, guessed-process termination or fallback scheduler exists.

## Original lifetime, restart and evidence

`session.json` retains the original run ID, interpreter hash, profile/package/
dossier/qualification/facts/handoff references, source binding, startup identity,
initial Operations budget/control, and policy/grant identities. Its launch UTC,
precise perf_counter_ns/QPC origin and native Windows boot identity are immutable.
The observation target is 12 hours; independent hard expiry is 14 hours from the
original launch by either UTC or QPC. Policy/grant expiration also stops the run.
Boot changes and regressed clocks hold/stop instead of renewing the deadline.
Expiry of a saved run is handled before current package/source preflight.

`owner.json` records the exact latest proven child fence/control.
`process-<launch-id>.json` records each host launch and its recorded PID, times,
boot and detachment audit. `status.json` contains bounded sanitized health.
`stop.json` records a committed Operations stop. Digests detect accidental
corruption; they are not authentication. Protect the directory as reviewed
operator evidence. Never delete evidence to reset a run.

After abrupt host death, job containment ends its child. Explicit restart of the
same request requires the original budget and exact latest recorded owner. It
uses original Operations CAS and restart limits; it never adopts a PID or resets
exhaustion. A crash before owner evidence is durable deliberately holds. Missing
or corrupt session/owner evidence never permits a replacement launch. Changed
source, reviewed configuration or interpreter prevents new execution. Existing
Ledger policy/grant selections, counters, original source anchors, attempts and
receipts are reconstructed by their original owners.

## Status, stop and reconciliation

`status` uses SQLite mode=ro plus query_only for Operations. The recorded PID and
host launch ID are historical identities, explicitly not proof of liveness.
Status does not require current approvals, reset ownership or create missing
files. Source/approval changes do not prevent `stop`: it needs only the intact
saved DRY domain/control binding and original budget. A regressed local clock
uses the last durable Operations timestamp as the safety-stop floor, never as
an Authority clock sample. Repeated stop preserves the stopped generation.

The running host commits stop before terminating/joining its exact child. Final
process exit closes the named kill-on-close job. A failed finalization is recorded
as held; it is never reported as a clean end. An interrupted action may remain
awaiting original DRY reconciliation.

`reconcile` first commits stop, then requires the named mutex and an empty prior
job. A live or unconfirmed prior host/job is denied. Once contained, it holds the
original Operations writer lock, verifies the unchanged stopped budget, and
reopens only the original Ledger. It uses finish_interrupted_dry on at most one
original unresolved DRY BUY. No public transport, source repair, clock admission,
policy/grant issuance or Operations acquisition/reset occurs. It rejects a signed
or economic graph. Original prepared bytes and simulation identity are retained,
and the original NON_SUBMITTED terminal retires tentative capacity. Repeated
reconciliation with no pending action does not even create a new Ledger writer.
Operations stop/generation/budget/attempts and original Authority state remain.

## Isolated validation

`python -B scripts/live_t010_public_host_selftest_v0_1.py` exercises synthetic
isolated stores and fake public HTTP boundaries, precise lifetime/restart/expiry,
read-only status, durable stop, the actual child startup timing hook, original
DRY reconciliation at three interruption cuts and a short real Windows detached
fake loop surviving termination of its exact launcher. It does not launch T010
or accepted Step10/Step11 campaigns and never accesses production data.

## Durable whole-session work budget

The request's `resource_envelope` must be the exact reference bound into the
reviewed profile. The public profile codec accepts this optional exact binding,
but complete physical validation remains fail-closed. No final numeric envelope
has been qualified. The bounded
engine consumes only an explicit `work_budget` with `max_startups`,
`max_runtime_steps`, and `target_dry_terminals`. No numeric defaults or extra
headroom are selected by the host. These values and the envelope reference are
bound into the immutable session.

`work_budget.json` reserves each child startup before Process.start and each
Runtime step before sending STEP. An interrupted, failed, or uncertain dispatch
remains charged. Recovery shares these same totals. Operations restart-window
rollover cannot replenish this separate whole-session budget. Missing, corrupt
or differently bound records hold; reopening never recreates the budget.

After each original child startup/step, the trusted input adapter reports actual
new retained exact-simulated DRY terminal roots (excluding original qualification roots) and
whether an admitted action remains pending. Duplicate observations do not add
terminals. The parent validates monotonic retained roots before permitting any
next STEP. When the work target or a total budget is reached, the host enters an
explicit OBSERVING state until the original 12-hour target/14-hour hard expiry.
It no longer polls the mutable supervisor, starts children, dispatches Runtime,
or fabricates source/timer work. It reads original Operations status only.
Pending or unproven work is named in the held observation state and is never
silently cleared, retried beyond the budget, or treated as successful execution.
The original lifetime is unchanged.

Host RSS uses the current process's native Windows GetProcessMemoryInfo working
set through K32GetProcessMemoryInfo. It requires no psutil installation and never
opens another process by PID.

Focused validation: `python -B scripts/live_t010_work_budget_selftest_v0_1.py`.
This includes actual abrupt process exit after durable step reservation and
original supervisor ordering checks proving reservation precedes Process.start
and STEP, with no further dispatch after the original target report.

## Finite source capture and retained-record allocation

Each qualification/host segment has one cumulative 10,000-row allowance for
each of the four original collector tables. The initial qualification capture
shares its segment allowance. Counts are durably charged after metadata checks
and before payload reads, with no refund or reset on failure or restart. The
original source profile remains 10,000 rows per table and 30-second freshness.
The adapter also checks the byte lengths of every selected witness and tail
field in the same SQLite read snapshot before materializing them. The T010 raw
capture allocation is one existing bounded-record unit, 1 MiB.

T010 explicitly allocates one such 1 MiB unit to each retained source verdict;
this is a new T010 allocation, not an inherited limit of the original source
codec. Both serialized progress and the complete verdict must fit. An incremental
JSON encoder stops at the boundary before building a full oversized JSON string.
The first capacity refusal durably records the original observation time,
disposition, predecessor and cursor identities as capacity-stop metadata. It
does not append, truncate, or synthesize a source verdict. Existing history is
preserved. A successful record reservation precedes the original append and is
never refunded after a crash. At most 314 qualification records (including the
initial cut) and 313 host records yield a 657,457,152-byte source JSON ceiling;
SQLite overhead and all other stores require separate physical coverage.

After the first non-HEALTHY original cut, the gate latches and the host observes
read-only. The pre-step check also catches a crash between the source commit and
the completion callback. Original pending DRY recovery still consumes the same
work budget and performs no new source capture. A held or exhausted qualifier
has not met its two exact-simulated terminal criterion.

The source allocation does not establish physical readiness. Cold replay at
evolving retained history, all-store allocations and SQLite main/WAL/SHM/freelist
growth, maximum transaction size and reserved recovery capacity remain subject
to the reviewed envelope's physical evidence and existing volume-reserve checks.
Missing or invalid required evidence continues to deny readiness.

For an explicit `C2_OBSERVATION_ONLY_V1` envelope, `HOST_RSS_BYTES`, `STARTUP_US`,
`PROTECTIVE_STEP_US`, `child_private_bytes`, `parent_first_legal_unit_us`,
`first_runtime_step_us`, `resource_max_age_us` and `recovery_evidence_max_age_us`
are observation dimensions, not numerical resource-limit thresholds. The
envelope requires hash-bound `observation_evidence`, forbids
`derivation.measured_guards`, and permits no non-null `measured_guard_evidence`.
These eight dimensions must be absent from numerical `resource_limits`.
Required observations remain validated evidence; their measured maxima do not
become capacity limits. This mode installs no numerical RSS/private-memory
quota. The separate existing monitor freshness contracts for
`resource_max_age_us` and `recovery_evidence_max_age_us` remain in force.

Health/liveness containment remains finite and separate from resource capacity.
For this mode, the qualification request explicitly supplies
`observation_policy: C2_OBSERVATION_ONLY_V1` and the existing `health` record with
`startup_us`, `progress_us` and `termination_us`. The host request must supply
the same `request.health` / `HealthProfile` values unchanged. Existing finite
startup/progress containment, bounded termination, exact-child ownership,
parent-exit containment and rejection of failed or late qualification output
remain required. Resource observations do not supply replacement health values.

Without the explicit C2 designation, legacy numerical/enforced mode retains its
existing numerical guards and quota enforcement. Missing numerical fields do
not select observation mode. Existing configured capacity ceilings, freshness,
volume reserves and pending/recovery/protective priority are unchanged in both
modes.
No actual T010 launch or public qualification is authorized by these controls.
