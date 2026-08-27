from __future__ import annotations

import ast
import hashlib
import importlib
import json
import re
import shutil
import sqlite3
import sys
import tempfile
from dataclasses import FrozenInstanceError, fields, replace
from pathlib import Path
from typing import Any, Callable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import phase5  # noqa: E402
from phase5.shadow_domain_v0_1 import (  # noqa: E402
    MODEL_FINGERPRINT,
    MODEL_ID,
    STATE_TRANSITIONS,
    ExecutionIntentV01,
    IntentRole,
    IntentSide,
    InvalidShadowTransition,
    InvalidStateTimestamp,
    ShadowDeterminismConflict,
    ShadowState,
    ShadowStateMachineV01,
)
from phase5.shadow_repository_v0_1 import (  # noqa: E402
    DEFAULT_SHADOW_DATABASE_PATH,
    ShadowRepositoryV01,
    open_shadow_repository,
)


BASE_US = 1_787_824_800_000_000
PROTECTED_HASHES = {
    "scripts/phase4_continuous_firstpullback_multihour_run_v0_5.py": (
        "22d4c0fa40e53e62fb305c75a832f6a633b496a8f10382d75353444e1392db37"
    ),
    "scripts/phase4_continuous_firstpullback_external_observability_run_v0_1.py": (
        "6f517b63220ae19b2edde282aea580ae42a18794c73291115a92a4e0b850518a"
    ),
    "scripts/phase4_continuous_firstpullback_multihour_run_v0_6.py": (
        "8bf11f86f333220dd8e8617021123d99566186f9ba90b9afaaa7b3fd731caf61"
    ),
    "scripts/phase4_continuous_firstpullback_extended_observability_run_v0_1.py": (
        "5f7bcfa1de4216f1e939a59295ec26d93fda968632ee1cec1de95232ece595d2"
    ),
}


def check(name: str, condition: bool, checks: dict[str, bool]) -> None:
    checks[name] = bool(condition)


def expect(error_type: type[BaseException], callback: Callable[[], Any]) -> bool:
    try:
        callback()
    except error_type:
        return True
    return False


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def entry_intent(**overrides: Any) -> ExecutionIntentV01:
    values: dict[str, Any] = {
        "candidate_signal_id": "fp1sig-canonical-candidate-0001",
        "candidate_run_id": "firstpullback-run-0001",
        "strategy_evaluation_id": "PSE-canonical-evaluation-0001",
        "strategy_version": "v1.1",
        "parameter_set_id": "EXP-0012:FINAL",
        "source_run_id": "source-run-0001",
        "source_event_key": "pump_events:704295",
        "source_ingest_seq": 704295,
        "decision_at_us": BASE_US,
        "mint": "Mint111111111111111111111111111111111111111",
        "role": IntentRole.ENTRY,
        "side": IntentSide.BUY,
        "input_asset": "SOL_LAMPORTS",
        "input_amount_base_units": 100_000_000,
    }
    values.update(overrides)
    return ExecutionIntentV01(**values)


def exit_intent(
    entry: ExecutionIntentV01,
    *,
    decision: str = "EXIT-DECISION-FINAL-A-0001",
    lifecycle: str = "EXIT-LIFECYCLE-FINAL-A-0001",
    track: str = "FINAL-A",
    amount: int = 40_000_000,
) -> ExecutionIntentV01:
    return ExecutionIntentV01(
        candidate_signal_id=entry.candidate_signal_id,
        candidate_run_id=entry.candidate_run_id,
        strategy_evaluation_id=None,
        strategy_version=entry.strategy_version,
        parameter_set_id=entry.parameter_set_id,
        source_run_id=entry.source_run_id,
        source_event_key="pump_events:704350",
        source_ingest_seq=704350,
        decision_at_us=BASE_US + 5_000_000,
        mint=entry.mint,
        role=IntentRole.EXIT,
        side=IntentSide.SELL,
        input_asset="MEME_BASE_UNITS",
        input_amount_base_units=amount,
        position_id="PP-position-final-a-0001",
        parent_entry_intent_id=entry.intent_id,
        exit_decision_id=decision,
        exit_track_id=track,
        exit_lifecycle_id=lifecycle,
    )


def mutate_transition_matrix() -> None:
    STATE_TRANSITIONS[ShadowState.COMPLETED] = frozenset(  # type: ignore[index]
        {ShadowState.CREATED}
    )


def _normalized_identifier(value: str) -> str:
    first = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", first).lower()


FORBIDDEN_IDENTIFIERS = frozenset(
    {
        "private_key",
        "secret_key",
        "seed_phrase",
        "keypair",
        "signer",
        "sign",
        "sign_transaction",
        "send_transaction",
        "send_raw_transaction",
        "broadcast",
        "submit_transaction",
        "live_enabled",
        "signing_enabled",
        "broadcast_enabled",
        "wallet_mutation",
    }
)
FORBIDDEN_IMPORT_SEGMENTS = frozenset(
    {"keypair", "signer", "wallet", "secret", "private_key"}
)


class CapabilityFirewallScanner(ast.NodeVisitor):
    def __init__(self) -> None:
        self.violations: list[str] = []

    def _check_identifier(self, name: str, lineno: int) -> None:
        normalized = _normalized_identifier(name)
        parts = set(normalized.split("_"))
        forbidden = (
            normalized in FORBIDDEN_IDENTIFIERS
            or "keypair" in parts
            or "signer" in parts
            or "broadcast" in parts
            or normalized.startswith("sign_")
            or ("transaction" in parts and ("send" in parts or "submit" in parts))
            or (
                ("enable" in parts or "enabled" in parts)
                and bool(parts & {"signing", "broadcast", "live"})
            )
        )
        if forbidden:
            self.violations.append(f"line {lineno}: forbidden API {name}")

    def visit_Name(self, node: ast.Name) -> None:
        self._check_identifier(node.id, node.lineno)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        self._check_identifier(node.attr, node.lineno)
        self.generic_visit(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._check_identifier(node.name, node.lineno)
        self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._check_identifier(node.name, node.lineno)
        self.generic_visit(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._check_identifier(node.name, node.lineno)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            segments = {_normalized_identifier(part) for part in alias.name.split(".")}
            if segments & FORBIDDEN_IMPORT_SEGMENTS:
                self.violations.append(
                    f"line {node.lineno}: forbidden dependency {alias.name}"
                )
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        segments = {_normalized_identifier(part) for part in module.split(".")}
        for alias in node.names:
            segments.add(_normalized_identifier(alias.name))
        if segments & FORBIDDEN_IMPORT_SEGMENTS:
            self.violations.append(
                f"line {node.lineno}: forbidden dependency {module}"
            )
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        if isinstance(node.value, str) and re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*", node.value
        ):
            self._check_identifier(node.value, node.lineno)
        self.generic_visit(node)


def scan_source(source: str) -> tuple[str, ...]:
    scanner = CapabilityFirewallScanner()
    scanner.visit(ast.parse(source))
    return tuple(scanner.violations)


def main() -> int:
    checks: dict[str, bool] = {}
    evidence: dict[str, Any] = {}
    root = Path(tempfile.mkdtemp(prefix="phase5_t001_"))
    try:
        first = entry_intent()
        identical = entry_intent()
        amount_changed = entry_intent(input_amount_base_units=100_000_001)
        lineage_changed = entry_intent(source_event_key="pump_events:704296")
        check("A01_DETERMINISTIC_SERIALIZATION", first.serialize() == identical.serialize(), checks)
        check("A02_DETERMINISTIC_IDENTITY", first.intent_id == identical.intent_id, checks)
        check(
            "A03_IMMUTABLE_FIELD_CHANGE_CHANGES_IDENTITY",
            len({first.intent_id, amount_changed.intent_id, lineage_changed.intent_id}) == 3,
            checks,
        )
        check(
            "A04_FROZEN_IMMUTABILITY",
            expect(FrozenInstanceError, lambda: setattr(first, "mint", "other")),
            checks,
        )
        check("A05_POSITIVE_INTEGER_AMOUNT_ACCEPTED", first.input_amount_base_units > 0, checks)
        check(
            "A06_ZERO_NEGATIVE_FLOAT_BOOL_REJECTED",
            all(
                (
                    expect(ValueError, lambda: entry_intent(input_amount_base_units=0)),
                    expect(ValueError, lambda: entry_intent(input_amount_base_units=-1)),
                    expect(TypeError, lambda: entry_intent(input_amount_base_units=1.5)),
                    expect(TypeError, lambda: entry_intent(input_amount_base_units=True)),
                )
            ),
            checks,
        )
        check("A07_ENTRY_BUY_ACCEPTED", first.role is IntentRole.ENTRY and first.side is IntentSide.BUY, checks)
        check(
            "A08_ENTRY_SELL_REJECTED",
            expect(ValueError, lambda: entry_intent(side=IntentSide.SELL)),
            checks,
        )
        exit_a = exit_intent(first)
        check("A09_EXIT_SELL_WITH_LINEAGE_ACCEPTED", exit_a.role is IntentRole.EXIT and exit_a.side is IntentSide.SELL, checks)
        check(
            "A10_EXIT_BUY_REJECTED",
            expect(ValueError, lambda: replace(exit_a, side=IntentSide.BUY)),
            checks,
        )
        check(
            "A11_MISSING_ENTRY_AND_EXIT_LINEAGE_REJECTED",
            expect(ValueError, lambda: entry_intent(strategy_evaluation_id=None))
            and expect(ValueError, lambda: replace(exit_a, position_id=None)),
            checks,
        )

        sentinel = root / "phase4-sentinel.sqlite3"
        sentinel_conn = sqlite3.connect(sentinel)
        sentinel_conn.execute("CREATE TABLE untouched(value TEXT)")
        sentinel_conn.execute("INSERT INTO untouched VALUES('phase4-remains-unchanged')")
        sentinel_conn.commit()
        sentinel_conn.close()
        sentinel_before = sha256(sentinel)

        database = root / "isolated-shadow" / "shadow.sqlite3"
        repo = open_shadow_repository(database)
        try:
            registered = repo.register_intent(first)
            replayed = repo.register_intent(identical)
            check(
                "A12_EXACT_ENTRY_REPLAY_IDEMPOTENT",
                registered.intent_id == replayed.intent_id,
                checks,
            )
            check(
                "A13_CONFLICTING_SHARED_ENTRY_REJECTED",
                expect(
                    ShadowDeterminismConflict,
                    lambda: repo.register_intent(amount_changed),
                ),
                checks,
            )
            track_entries = tuple(
                repo.register_intent(entry_intent())
                for _track in ("FINAL-A", "FINAL-B", "SENS-C")
            )
            entry_count = int(
                repo._conn.execute(
                    "SELECT COUNT(*) FROM shadow_execution_intents WHERE role='ENTRY'"
                ).fetchone()[0]
            )
            check(
                "A14_ALTERNATIVE_TRACKS_SHARE_ONE_ENTRY",
                len({item.intent_id for item in track_entries}) == 1
                and entry_count == 1,
                checks,
            )
            exit_b = exit_intent(
                first,
                decision="EXIT-DECISION-FINAL-A-PARTIAL-0002",
                lifecycle="EXIT-LIFECYCLE-FINAL-A-PARTIAL-0002",
                amount=20_000_000,
            )
            missing_parent = replace(
                exit_a,
                parent_entry_intent_id="P5EI-missing-parent",
                exit_decision_id="EXIT-DECISION-MISSING-PARENT",
            )
            conflicting_parent = replace(
                exit_a,
                candidate_signal_id="different-canonical-candidate",
                exit_decision_id="EXIT-DECISION-CONFLICTING-PARENT",
            )
            check(
                "A15_MALFORMED_PARENT_ENTRY_LINEAGE_REJECTED",
                expect(
                    ShadowDeterminismConflict,
                    lambda: repo.register_intent(missing_parent),
                )
                and expect(
                    ShadowDeterminismConflict,
                    lambda: repo.register_intent(conflicting_parent),
                ),
                checks,
            )
            repo.register_intent(exit_a)
            repo.register_intent(exit_b)
            check(
                "A16_MULTIPLE_PARTIAL_EXIT_DECISIONS_SUPPORTED",
                exit_a.position_id == exit_b.position_id
                and exit_a.intent_id != exit_b.intent_id,
                checks,
            )

            all_skips_rejected = True
            for source, allowed in STATE_TRANSITIONS.items():
                for destination in ShadowState:
                    if destination in allowed:
                        continue
                    all_skips_rejected = all_skips_rejected and expect(
                        InvalidShadowTransition,
                        lambda s=source, d=destination: ShadowStateMachineV01.validate(s, d),
                    )
            check("B01_EVERY_PROHIBITED_SKIP_REJECTED", all_skips_rejected, checks)

            first_step = repo.transition(
                first.intent_id,
                ShadowState.ELIGIBILITY_CHECKED,
                idempotency_key="ELIGIBILITY-0001",
                effective_at_us=BASE_US + 1,
                reason_code="ELIGIBLE",
                evidence={"eligible": True},
            )
            repo.transition(
                first.intent_id,
                ShadowState.ROUTE_BOUND,
                idempotency_key="ROUTE-0001",
                effective_at_us=BASE_US + 2,
                reason_code="ROUTE_REFERENCE_BOUND",
                evidence={"route_ref": "future-route-ref"},
            )
            exact_replay = repo.transition(
                first.intent_id,
                ShadowState.ELIGIBILITY_CHECKED,
                idempotency_key="ELIGIBILITY-0001",
                effective_at_us=BASE_US + 1,
                reason_code="ELIGIBLE",
                evidence={"eligible": True},
            )
            check(
                "B02_EXACT_OLDER_TRANSITION_REPLAY_IDEMPOTENT",
                exact_replay.transition_id == first_step.transition_id,
                checks,
            )
            check(
                "B03_CONFLICTING_IDEMPOTENCY_REPLAY_REJECTED",
                expect(
                    ShadowDeterminismConflict,
                    lambda: repo.transition(
                        first.intent_id,
                        ShadowState.ELIGIBILITY_CHECKED,
                        idempotency_key="ELIGIBILITY-0001",
                        effective_at_us=BASE_US + 1,
                        reason_code="ELIGIBLE",
                        evidence={"eligible": False},
                    ),
                ),
                checks,
            )
            check(
                "B04_STALE_TRANSITION_TIME_REJECTED",
                expect(
                    InvalidStateTimestamp,
                    lambda: repo.transition(
                        first.intent_id,
                        ShadowState.QUOTE_BOUND,
                        idempotency_key="STALE-QUOTE",
                        effective_at_us=BASE_US + 1,
                        reason_code="STALE",
                        evidence={},
                    ),
                ),
                checks,
            )
            path = (
                (ShadowState.QUOTE_BOUND, "QUOTE-0001", 3),
                (ShadowState.PLAN_BUILT, "PLAN-0001", 4),
                (ShadowState.SIMULATED, "SIMULATION-0001", 5),
                (ShadowState.COMPLETED, "COMPLETE-0001", 6),
            )
            for state, key, offset in path:
                repo.transition(
                    first.intent_id,
                    state,
                    idempotency_key=key,
                    effective_at_us=BASE_US + offset,
                    reason_code=f"{state.value}_EVIDENCED",
                    evidence={"stage": state.value},
                )
            check(
                "B05_FULL_LEGAL_PATH_TO_COMPLETED",
                repo.current_state(first.intent_id) is ShadowState.COMPLETED
                and repo.reconstruct_state(first.intent_id) is ShadowState.COMPLETED
                and repo.audit_intent(first.intent_id),
                checks,
            )
            check(
                "B06_COMPLETED_BEFORE_SIMULATED_REJECTED",
                expect(
                    InvalidShadowTransition,
                    lambda: ShadowStateMachineV01.validate(
                        ShadowState.PLAN_BUILT, ShadowState.COMPLETED
                    ),
                ),
                checks,
            )
            check(
                "B07_TERMINAL_RESURRECTION_REJECTED",
                expect(
                    InvalidShadowTransition,
                    lambda: repo.transition(
                        first.intent_id,
                        ShadowState.ELIGIBILITY_CHECKED,
                        idempotency_key="RESURRECT",
                        effective_at_us=BASE_US + 7,
                        reason_code="NOT_ALLOWED",
                        evidence={},
                    ),
                ),
                checks,
            )

            negative_states = (
                (ShadowState.REJECTED, "candidate-rejected", "NOT_ELIGIBLE"),
                (ShadowState.EXPIRED, "candidate-expired", "INTENT_EXPIRED"),
                (ShadowState.FAILED, "candidate-failed", "OPERATION_FAILED"),
            )
            negative_ok = True
            for state, candidate, reason in negative_states:
                item = entry_intent(candidate_signal_id=candidate)
                repo.register_intent(item)
                repo.transition(
                    item.intent_id,
                    state,
                    idempotency_key=f"TERMINAL-{state.value}",
                    effective_at_us=BASE_US + 1,
                    reason_code=reason,
                    evidence={"reason_code": reason},
                )
                negative_ok = negative_ok and repo.current_state(item.intent_id) is state
            check("B08_LEGAL_REJECT_EXPIRY_FAILURE_PATHS", negative_ok, checks)
            check(
                "B09_TRANSITION_MATRIX_IS_IMMUTABLE",
                expect(TypeError, mutate_transition_matrix),
                checks,
            )

            history_before = repo.history(first.intent_id)
            update_blocked = expect(
                sqlite3.IntegrityError,
                lambda: repo._conn.execute(
                    "UPDATE shadow_transition_history SET reason_code='MUTATED' "
                    "WHERE intent_id=?",
                    (first.intent_id,),
                ),
            )
            repo._conn.rollback()
            delete_blocked = expect(
                sqlite3.IntegrityError,
                lambda: repo._conn.execute(
                    "DELETE FROM shadow_transition_history WHERE intent_id=?",
                    (first.intent_id,),
                ),
            )
            repo._conn.rollback()
            intent_update_blocked = expect(
                sqlite3.IntegrityError,
                lambda: repo._conn.execute(
                    "UPDATE shadow_execution_intents SET role='EXIT' WHERE intent_id=?",
                    (first.intent_id,),
                ),
            )
            repo._conn.rollback()
            check(
                "C01_IMMUTABLE_INTENTS_APPEND_ONLY_HISTORY",
                update_blocked
                and delete_blocked
                and intent_update_blocked
                and repo.history(first.intent_id) == history_before,
                checks,
            )
            fk_blocked = expect(
                sqlite3.IntegrityError,
                lambda: repo._conn.execute(
                    "INSERT INTO shadow_state_machines VALUES(?,?,?,?)",
                    ("missing-intent", ShadowState.CREATED.value, BASE_US, 0),
                ),
            )
            repo._conn.rollback()
            check(
                "C02_FOREIGN_KEYS_ON_AND_ENFORCED",
                repo.foreign_keys_enabled
                and fk_blocked
                and repo.foreign_key_violations() == (),
                checks,
            )
            check("C03_SQLITE_QUICK_CHECK_OK", repo.quick_check() == "ok", checks)
            check(
                "C04_ISOLATED_DATABASE_PATH",
                repo.database_path == database.resolve()
                and any("shadow" in part.lower() for part in database.parts)
                and DEFAULT_SHADOW_DATABASE_PATH.parent.name == "shadow"
                and not hasattr(repo, "connection"),
                checks,
            )
            digest_before_reopen = repo.canonical_digest()
            serialized_before_reopen = first.serialize()
            history_count_before_reopen = len(repo.history(first.intent_id))
        finally:
            repo.close()

        reopened = open_shadow_repository(database)
        try:
            recovered = reopened.get_intent(first.intent_id)
            check(
                "C05_CLOSE_REOPEN_PRESERVES_INTENT",
                recovered is not None
                and recovered.serialize() == serialized_before_reopen,
                checks,
            )
            check(
                "C06_CLOSE_REOPEN_PRESERVES_HISTORY_STATE_DIGEST",
                len(reopened.history(first.intent_id)) == history_count_before_reopen
                and reopened.current_state(first.intent_id) is ShadowState.COMPLETED
                and reopened.audit_intent(first.intent_id)
                and reopened.canonical_digest() == digest_before_reopen,
                checks,
            )
            evidence["repository_digest"] = reopened.canonical_digest()
            evidence["quick_check"] = reopened.quick_check()
        finally:
            reopened.close()

        check(
            "C07_PHASE4_SENTINEL_UNCHANGED",
            sha256(sentinel) == sentinel_before,
            checks,
        )
        forbidden_path = PROJECT_ROOT / "data" / "paper" / "p5_t001_forbidden.sqlite3"
        check(
            "C08_PHASE4_AND_SOURCE_PATHS_FAIL_CLOSED_BEFORE_WRITE",
            expect(ValueError, lambda: open_shadow_repository(forbidden_path))
            and not forbidden_path.exists(),
            checks,
        )
        bypass_conn = sqlite3.connect(sentinel)
        try:
            constructor_bypass_rejected = expect(
                TypeError,
                lambda: ShadowRepositoryV01(
                    bypass_conn,
                    root / "isolated-shadow" / "bypass.sqlite3",
                ),
            )
        finally:
            bypass_conn.close()
        check(
            "C09_DIRECT_REPOSITORY_CONSTRUCTION_REJECTED",
            constructor_bypass_rejected,
            checks,
        )

        package_root = SRC_ROOT / "phase5"
        violations: dict[str, tuple[str, ...]] = {}
        phase4_imports: list[str] = []
        for source_path in sorted(package_root.rglob("*.py")):
            source = source_path.read_text(encoding="utf-8")
            found = scan_source(source)
            if found:
                violations[source_path.name] = found
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("phase4"):
                    phase4_imports.append(f"{source_path.name}:{node.lineno}")
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.startswith("phase4"):
                            phase4_imports.append(f"{source_path.name}:{node.lineno}")
        check("D01_PRODUCTION_PACKAGE_FIREWALL_SCAN_CLEAN", not violations, checks)
        check("D02_NO_PHASE4_RUNTIME_DEPENDENCY", not phase4_imports, checks)
        check(
            "D03_PUBLIC_API_HAS_NO_LIVE_CAPABILITY",
            not ({_normalized_identifier(name) for name in phase5.__all__} & FORBIDDEN_IDENTIFIERS),
            checks,
        )
        all_field_names = {
            field.name
            for model in (ExecutionIntentV01,)
            for field in fields(model)
        }
        check(
            "D04_NO_CAPABILITY_ENABLE_CONFIG_FIELD",
            not any(name.endswith("_enabled") for name in all_field_names),
            checks,
        )
        forbidden_states = {
            "SIGNED",
            "SUBMITTED",
            "BROADCAST",
            "CONFIRMED_LIVE",
        }
        check(
            "D05_NO_TRANSACTION_SUBMISSION_STATE",
            not ({state.value for state in ShadowState} & forbidden_states),
            checks,
        )
        future_read_only_source = """
async def read_and_simulate(client, unsigned_plan):
    state = await client.get_account_info('public-address')
    result = await client.simulate_transaction(unsigned_plan)
    return state, result
"""
        check(
            "D06_READ_ONLY_RPC_AND_SIMULATION_REMAIN_EXTENSIBLE",
            scan_source(future_read_only_source) == (),
            checks,
        )
        imported = importlib.import_module("phase5")
        check(
            "D07_PACKAGE_IMPORTABLE_WITH_CURRENT_ALLOWED_DEPENDENCIES",
            imported.MODEL_ID == MODEL_ID,
            checks,
        )
        injected_live_sources = (
            "from solders.keypair import Keypair",
            "def send_transaction(payload): return payload",
            "signing_enabled = False",
            "async def hidden(c, p): return await getattr(c, 'sendTransaction')(p)",
        )
        check(
            "D08_FIREWALL_CATCHES_INJECTED_LIVE_CAPABILITIES",
            all(scan_source(source) for source in injected_live_sources),
            checks,
        )

        actual_hashes = {
            path: sha256(PROJECT_ROOT / path) for path in PROTECTED_HASHES
        }
        check(
            "E01_PROTECTED_ACTIVE_PHASE4_BASELINES_EXACT",
            actual_hashes == PROTECTED_HASHES,
            checks,
        )
        evidence["protected_hashes"] = actual_hashes
        evidence["intent_id"] = first.intent_id
        evidence["intent_fingerprint"] = first.fingerprint
        evidence["model_fingerprint"] = MODEL_FINGERPRINT
    finally:
        shutil.rmtree(root, ignore_errors=True)

    print("=" * 112)
    print("PHASE-5 SHADOW DOMAIN + CAPABILITY FIREWALL v0.1 SELF-TEST")
    print("=" * 112)
    for name, passed in checks.items():
        print(f"{name}: {'PASS' if passed else 'FAIL'}")
    print(f"MODEL_ID: {MODEL_ID}")
    print(f"MODEL_FINGERPRINT: {evidence.get('model_fingerprint')}")
    print(f"SAMPLE_INTENT_ID: {evidence.get('intent_id')}")
    print(f"SAMPLE_INTENT_FINGERPRINT: {evidence.get('intent_fingerprint')}")
    print(f"REPOSITORY_DIGEST: {evidence.get('repository_digest')}")
    print(f"SQLITE_QUICK_CHECK: {evidence.get('quick_check')}")
    print(f"CHECKS: {sum(checks.values())}/{len(checks)}")
    passed = bool(checks) and all(checks.values())
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
