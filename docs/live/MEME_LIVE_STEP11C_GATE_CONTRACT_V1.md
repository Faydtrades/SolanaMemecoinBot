# Step11-C exact-freeze preflight contract and handoff runbook

Status: IMPLEMENTED_PENDING_PROJECT_REVIEW. This contract does not accept M56,
M57 or any lifecycle row. The default package has no actual M09/M10 choices,
M58 authorization or project approval; its T010 readiness remains DENIED.

## Immutable evidence and current implementation

M56 retains accepted revision d7993c4dfaff62ac87ed777f8f48366186c5848f and its
immutable Step10, Step11-A and Step11-B evidence separately from current code.
The source freeze explicitly enumerates the seven authorized Step11-C core
changes, each accepted Git hash and each current hash. It reports
accepted_runtime_changed=true. All other accepted source remains immutable.
New tooling and the final affected regression runner are explicitly enumerated.
Historical database hash observations remain references in accepted evidence;
freeze and preflight do not open, initialize, rehash or repair those databases.
The publication commit is a separate reference, avoiding self-reference.

The SAME RuntimeCompositionV01 / ColdRuntimeV01 / Operations startup,
OperationsStore and supervisor now support a fixed DRY domain. The original
continuous source/producer and CandidateHandoff feed original Authority
admission, original runtime_dry.run_dry and durable Ledger NON_SUBMITTED, then
continue to the next original candidate. Cold DRY reopen adjudicates interrupted
original work through the original NON_SUBMITTED recovery. Ownership, STOP,
restart and degradation remain in the original graph. DRY physically branches
before LIVE execution, accepts only DryExecutionPorts and rejects supplied LIVE
resources. The public driver constructs only ExecutionReadOnlyRpc. It has no
signer or send transport. DRY cannot create chain state, inventory, fills or
capital authority; corrupt DRY economic history is denied by original invariants.

## Explicit supported profile and qualification scope

runtime_dry_profile_v0_1.build_profile takes only explicit records: accepted
profile/extension/monitor references, an original WalletObservation and reviewed
known-native baseline, retained source anchors/start, five distinct DRY store
paths, schema digests, finite monitor constraints and public-driver settings.
The original Ledger baseline adjudicator must return ESTABLISHED. Unknown
funding remains unknown and cannot produce an executable profile. Startup also
binds the durable original baseline observation digest; a different observation
cannot silently rebind an existing Ledger.

Public wallet, genesis, provider/profile and source database identity match the
accepted deployment. Source starts use
EXPLICIT_RETAINED_ANCHOR_ORIGINAL_CONTINUATION_NO_REPAIR. The original source,
producer-lineage and health-binding identities retain their independent
namespaces and are connected by the original handoff and continuation checks.
Changing observations, source anchors/start, paths or configuration requires an
explicit new profile, matching qualification artifacts and project review using
the same constructor; it does not require another gate implementation.

The retained qualification is DETERMINISTIC_QUALIFICATION_ONLY. It explicitly
substitutes synthetic public wallet/RPC observations, source rows/anchors and an
isolated source read path while retaining the accepted canonical source database
identity. It starts and cold-recovers the SAME supported constructor, performs
two exact zero-signature simulations and NON_SUBMITTED cycles, and proves
prepared interruption recovery without another public read. No canonical DB,
real public environment, production performance or T010 run is claimed.
A PUBLIC_ENVIRONMENT_REVIEWED input instead requires the canonical source read
path and no synthetic observation/source flags; this record is not itself an
authentication mechanism.

All accepted LIVE guards remain unchanged. The separately authorized DRY-only
qualification amendment allows HISTORY_BYTES=16384 and TOMBSTONE_ROWS=2, based
on the observed two-candidate footprint 12749 bytes / 2 tombstones. Every other
finite accepted constraint remains unchanged. The exact amendment reference,
actual maximum observed metrics and fail-closed bound+1 comparator tests are
bound into the profile and qualification. Host metrics are synthetic external
comparator facts, not measured production capacity. The amendment remains
IMPLEMENTED_PENDING_PROJECT_REVIEW, not accepted project policy or M09/M10.

Missing, stale or contradictory profile/qualification evidence remains an
engineering denial. A valid qualified profile removes only the old static
composition/profile gaps. It does not prove the current environment or supply
owner choices, approval or permission.

## Offline commands and structural preflight

Use new output paths and explicit UTC values:

```text
python -B scripts/live_runtime_dry_profile_v0_1.py --inputs EXPLICIT_INPUTS_JSON --output NEW_PROFILE_JSON
python -B scripts/live_runtime_dry_profile_v0_1.py --verify EXISTING_PROFILE_JSON
python -B scripts/live_acceptance_dossier_v0_1.py --dry-profile PROFILE_JSON --dry-qualification QUALIFICATION_JSON --output NEW_M56_JSON
python -B scripts/live_acceptance_dossier_v0_1.py --verify EXISTING_M56_JSON
python -B scripts/live_t010_preflight_v0_1.py --dossier M56_JSON --at-utc UTC --output NEW_M57_JSON
python -B scripts/live_t010_preflight_v0_1.py --dossier M56_JSON --package EXPLICIT_PACKAGE_JSON --at-utc UTC --output NEW_STRUCTURAL_REPORT_JSON
```

MEME_LIVE_T010_PACKAGE_V1 binds the exact dossier/source, original components,
accepted deployment references and full exact supported profile record under
dry_configuration. Original AuthorityPolicyV02, ArmingGrant and LedgerDomain
validators consume explicit M09 selected track, M10 size/cost/clock fields and
M58 DRY grant. There are no economic defaults or inferred operator choices.
Public approval records bind exact supplied policy/grant fields; original
OperatorProvenance hashes and UTC windows must match. Project review is a
separate exact-dossier/source reference. JSON record assertions do not
authenticate a human or create original Authority controls.

Absent inputs produce DENIED, ready=false and CLI exit 2. A fully consistent
package can produce STRUCTURALLY_READY_PENDING_EXTERNAL_AUTHORIZATION,
ready=true, readiness_scope=STRUCTURAL_PREFLIGHT_ONLY and CLI exit 0. In BOTH
cases grants_permission=false, authenticated_approval=false,
current_environment_proven=false, capital_authority=false and t010_executed=false.
Exit 0 or ready=true alone must never be treated as permission. The positive
fixture uses conspicuously synthetic approval records solely to prove that the
frozen structural tooling has a complete supported path.

## Separately authorized later T010 use

The execution interface already exists: load_profile(exact_record),
profile.configuration(), profile.start(process_identity, now_us,
replace_generation), then profile.drive(started, facts_provider,
public_transport=None). The finite bound is profile.inputs.driver.max_steps;
the original Operations supervisor can own the same start configuration and
profile.driver(facts_provider) context. The driver consumes only typed
DryPublicFacts: original clock interval, entry support, public wallet evidence,
quote/plan/compute constraints, current UTC source cut and host observations.
BoundPublicFacts checks exact profile policy/provider/domain identities; original
source/clock/wallet and Authority consumers determine admission on each step.
The driver contains no economic decision callback or signer/send capability.

Before any later T010, explicit user/project authorization must cover that run,
current public observations/source-start review and exact reviewed configuration.
The original initialization APIs must already have created the expressly
authorized separate DRY stores with the exact baseline, source/producer,
Operations and monitor identities; profile.start does not initialize or repair
missing stores. Record actual current inputs and runtime outcomes separately
from the synthetic qualification. A stale/mismatched public profile must be
rebound and reviewed with this same interface. Actual Authority policy/grant
records must be applied through their original authorized control path, never
inferred from a preflight report.

M59/M60/M61 remain subsequent human gates, not T010 prerequisites. M56/M57 project
acceptance remains an engineering review boundary. No activity in this change
executes T010/T011, opens the canonical stores, changes the disabled M46 task,
contacts the network, signs, sends or broadcasts.
