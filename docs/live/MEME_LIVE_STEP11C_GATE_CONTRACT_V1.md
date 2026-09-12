# Step11-C exact-freeze preflight contract and handoff runbook

Status: IMPLEMENTED_PENDING_PROJECT_REVIEW. M56 and M57 are PARTIAL engineering
closure candidates. This contract does not promote lifecycle rows. T010 is NOT
READY and must not execute against this package.

## Immutable input and identity

M56 starts from accepted revision d7993c4dfaff62ac87ed777f8f48366186c5848f plus
the exact current Step11-C source overlay. Its deterministic digest binds all
Python production dependencies and LIVE entrypoints, immutable accepted
Step10 S01-S12/E01-E05, Step11-A M52/M53, Step11-B M46 references, governing
contracts, accepted deployment configuration/profile/extension/host and the
53 VERIFIED / 2 OWNED_NOT_BUILT / 6 HUMAN_EXTERNAL matrix disposition.

The eventual publication commit contains the dossier; it is a separate
publication reference, not a self-referential dossier input. Existing accepted
documentation is referenced at the immutable base revision. Generated Step11-C
reports and later status bookkeeping do not enter their own digest. This gate
contract and every Step11-C executable are included in the source/contract freeze.

Historical source assertions retain their original scope and accepted parent
hashes. Current code is independently frozen; old profile runtime-code digests
are never substituted for the current content digest. Accepted historical
database hash observations are references inside immutable evidence, not claims
about current store contents. Freeze/preflight never reads those databases.

## Fixed engineering gaps and affected original consumers

These are required engineering transitions, not human inputs:

1. The identical production graph lacks continuous owned DRY composition and
   recovery. RuntimeCompositionV01 in runtime_composition_v0_1.py requires LIVE;
   its ExecutionPorts require actual signer/sender types. ColdRuntimeV01's
   reopen_live and operations_startup configured_identity/start_live are fixed
   to LIVE. OperationsStore construction and initialization in
   operations_ownership_v0_1.py also explicitly require LIVE.
2. A reviewed and qualified DRY production artifact/profile bound to that
   original graph is therefore missing. The finite accepted LIVE host/profile
   and historical qualification startup identity do not qualify an unbuilt
   continuous DRY ownership/capability path.

Accepted Step10 S10 does exercise actual LiveContinuousProducerV02 ->
CandidateHandoffV01 -> original Authority admission -> runtime_dry.run_dry ->
Ledger NON_SUBMITTED -> further producer/admission and original cold DRY
recovery. run_dry consumes one already admitted original action;
ColdDryRuntimeV01 recovers that action. Those accepted component transitions
do not supply a continuous owned production scheduler or its boot/monitor
integration. The Windows host entrypoint is explicitly qualification-only and
denies canonical execution; it cannot be advertised as the T010 driver.

No standalone gate-only runtime or replacement callbacks are supplied. Closing
the gap requires a bounded extension of the original production root,
ownership, cold startup and readiness/monitor consumers with a fixed DRY
capability. It must preserve the original producer/clock/source/Authority
admission path, call only accepted zero-signature exact read/simulation and
NON_SUBMITTED recovery for DRY, and then resume the same lineage. A source/config
rebind, directly affected deterministic qualification, and the governing
composition/capability regression are prerequisites to a new freeze. Original
accepted LIVE evidence remains immutable at its established scope; it cannot
silently qualify newly changed production semantics.

The preflight implementation hard-codes these gaps as ENGINEERING. Neither an
owner approval, project-review assertion, fixture success nor a package field
can override them. M56 cannot claim a full engineering dossier and M57 cannot
claim a T010-ready production artifact until these gaps close.

## Preflight and public approval records

Run only the offline commands, with an explicit output path and evaluation UTC:

```text
python -B scripts/live_acceptance_dossier_v0_1.py --output NEW_M56_JSON
python -B scripts/live_acceptance_dossier_v0_1.py --verify EXISTING_M56_JSON
python -B scripts/live_t010_preflight_v0_1.py --dossier EXISTING_M56_JSON --at-utc UTC --output NEW_M57_JSON
```

Optional --package supplies MEME_LIVE_T010_PACKAGE_V1. Every field is explicit;
the package binds the exact dossier digest/source content, fixed original
components, accepted deployment hashes and a separate DRY domain/configuration.
Missing M09 selected track, M10 policy or M58 T010 permission denies readiness.
M59/M60/M61 are subsequent human gates and are not prerequisites to T010 itself.
Missing M56/M57 project acceptance is ENGINEERING_REVIEW, not a seventh human row.

AuthorityPolicyV02, ArmingGrant and LedgerDomain use their original validators.
There is no implicit policy version, selected track, capital limit, cost,
clock or grant scope. The grant must name the exact supplied policy, be DRY,
have no one-time LIVE root and fit the policy's original UTC window. Public
approval JSON references bind exact policy/grant inputs excluding their approval
metadata to avoid a hash cycle; their hashes match original OperatorProvenance.
The original locked role's strategy version and parameter fingerprint, plus
the original continuous producer model fingerprint, must match actual frozen
source constants; valid-looking replacement digests are rejected.
Project review is a separate exact-dossier/source-bound reference. Structural
validation does not authenticate an operator or create an Authority command.

The accepted LIVE public configuration remains unchanged. An explicitly supplied
DRY domain must have a distinct economic identity, matching public wallet,
genesis and supported provider/profile, separate store paths and explicit
source lineage/start/profile. Unknown native balance remains null; no fill,
inventory or balance is invented. The accepted finite numerical limits and
qualification-only restart profile are preserved as constraints, not promoted
to production capacity or an approved DRY restart policy. No canonical LIVE or
DRY store is created, opened, migrated or repaired during this preflight.

Source lineage, anchors and qualification start must match the exact accepted
profile; an arbitrary well-formed replacement is rejected even if policy fields
co-vary with it. This is only the recorded qualification binding. Its activation
start is still unselected and current source health is unestablished. A later
production activation/rebind belongs in the reviewed original-graph integration
delta and cannot be asserted into readiness through this package.

## Evidence checklist and later gate boundary

Independent review checks the final diff, M56 manifest, M57 report, focused
validation and exact accepted links where necessary. It distinguishes code or
coverage defects from deliberately absent human inputs. The current static
composition/profile gaps remain engineering findings in either case.

Focused fixtures must show exact freeze binding; missing, stale and conflicting
evidence denial; invalid/missing policy/grant/project records; unsupported
mode/capability/configuration/component rejection; a structurally complete
synthetic package; and unchanged files plus no DB/network/sign/send activity.
A structurally complete fixture still reports ready=false and grants_permission=false.

Only after original production integration, affected qualification, a new exact
freeze, independent review and project acceptance can the project authorize a
later driver/runbook revision and separate T010 run. That later run must collect
actual source/clock/provider observations, same-lineage repeated candidates,
original exact zero-signature simulation, durable NON_SUBMITTED and restart
capacity evidence through the identical qualified production artifact. Public
environment observations cannot replace missing static engineering.

This version never signs, sends, broadcasts, fabricates fills/inventory, grants
capital authority, initializes canonical stores or executes T010/T011. CLI exit
2 accompanies denied readiness; an output artifact is still available for review.
