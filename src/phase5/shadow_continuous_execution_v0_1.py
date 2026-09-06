from __future__ import annotations

import json
import time
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Mapping

from . import shadow_domain_v0_1 as d
from . import shadow_venue_route_quote_v0_1 as v
from . import shadow_unsigned_plan_simulation_v0_1 as s
from . import shadow_readonly_rpc_v0_1 as r
from . import shadow_continuous_source_bridge_v0_1 as a
from . import shadow_lifecycle_bridge_v0_1 as b
from . import shadow_simulation_repository_v0_1 as sr
from .shadow_venue_repository_v0_1 import open_venue_repository
from .shadow_simulation_repository_v0_1 import (
    open_simulation_repository, record_plan_state, record_simulation_state,
)

MODEL_ID = "P5-CONTINUOUS-SHADOW-EXECUTION-0001"
COMPONENT_FINGERPRINTS = {
    "T001": d.MODEL_FINGERPRINT, "T002": v.MODEL_FINGERPRINT,
    "T003": s.MODEL_FINGERPRINT, "T004A": a.MODEL_FINGERPRINT,
    "T004B": b.MODEL_FINGERPRINT, "T004C1": r.MODEL_FINGERPRINT,
    "T003_persistence": sr.PERSISTENCE_FINGERPRINT,
}
EXPECTED_ONLY = "EXPECTED_INVENTORY_ONLY_NO_ACTUAL_BASE_ACCOUNT"
CONTRACT_SPEC = {
    "model_id": MODEL_ID, "components": COMPONENT_FINGERPRINTS,
    "cycle_order": ["poll_entries", "execute_entries", "inventory", "poll_exits",
                    "execute_exits", "comparisons"],
    "slippage_bps": 2000, "maximum_refreshes": 1,
    "recovery": "IMMUTABLE_RPC_JOURNAL_AND_EXACT_ACCEPTED_EVIDENCE_REPLAY",
    "context_fence": "EVERY_CONTEXT_RESPONSE_AT_OR_ABOVE_REQUESTED_MINIMUM",
    "program_rejected_terminal": "FAILED", "exit_missing_base": EXPECTED_ONLY,
    "comparison": "APPEND_ONLY_OBSERVATIONS_WITH_CURRENT_POINTER",
    "comparison_capture": "SINGLE_AUTHORITATIVE_OBSERVATION_ALLOW_PAPER_ADVANCEMENT",
    "inventory_backpressure": "DRAIN_TERMINAL_ENTRY_BACKLOG_BEFORE_NEW_EXECUTION",
    "timing": "DURABLE_WALL_CLOCK_COMPLETION_OBSERVATION_NOT_CHAIN_ORDER",
    "phase4_access": "SQLITE_MODE_RO_QUERY_ONLY", "inventory": "INDEPENDENT_TRACKS",
    "phase4_source_scope": "ONE_T004A_AND_ONE_T004B_PER_SQLITE_SOURCE",
    "candidate_run_id": "PER_INTENT_IMMUTABLE_LINEAGE_NOT_SOURCE_SELECTOR",
}
MODEL_FINGERPRINT = d.content_fingerprint(CONTRACT_SPEC)


class ContextFenceError(RuntimeError):
    pass


class ReplayConflict(d.ShadowDeterminismConflict):
    pass


class InjectedCrash(BaseException):
    """Test-only interruption, deliberately outside ordinary error handling."""


def _account_payload(account: v.RpcAccountV01 | None) -> Any:
    return None if account is None else {
        "pubkey": account.pubkey, "owner": account.owner, "data_hex": account.data.hex(),
    }


def _encode(response: Any) -> dict[str, Any]:
    if isinstance(response, v.RpcAccountBatchV01):
        return {"context_slot": response.context_slot,
                "accounts": [_account_payload(x) for x in response.accounts]}
    return asdict(response)


def _decode(operation: str, payload: dict[str, Any]) -> Any:
    if operation == "accounts":
        return v.RpcAccountBatchV01(payload["context_slot"], tuple(
            None if x is None else v.RpcAccountV01(x["pubkey"], x["owner"], bytes.fromhex(x["data_hex"]))
            for x in payload["accounts"]
        ))
    types = {"blockhash": s.LatestBlockhashResponseV01, "height": s.BlockHeightResponseV01,
             "validity": s.BlockhashValidityResponseV01, "simulation": s.SimulationRpcResponseV01}
    return types[operation](**payload)


class _JournalRpc:
    def __init__(self, owner: ContinuousShadowExecutionV01, intent_id: str, replay_only: bool):
        self.owner, self.intent_id, self.replay_only = owner, intent_id, replay_only
        self.sequence = 0

    def _call(self, operation: str, request: dict[str, Any], callback: Callable[[], Any], minimum: int | None):
        index = self.sequence
        self.sequence += 1
        key = f"rpc:{index}"
        saved = self.owner._read(self.intent_id, key)
        if saved is None:
            if self.replay_only:
                raise ReplayConflict("completed intent has incomplete RPC journal")
            try:
                response = callback()
                if minimum is not None and hasattr(response, "context_slot") and response.context_slot < minimum:
                    raise ContextFenceError("RPC context below requested minimum")
                saved = {"operation": operation, "request": request, "response": _encode(response)}
            except Exception as exc:
                saved = {"operation": operation, "request": request, "error_type": type(exc).__name__,
                         "error_reason": str(exc) if isinstance(exc, (r.ShadowRpcError, ContextFenceError)) else type(exc).__name__}
            self.owner._put(self.intent_id, key, saved)
            self.owner._fault("journal:" + key, self.intent_id)
        if saved["operation"] != operation or saved["request"] != request:
            raise ReplayConflict("RPC replay request conflicts with durable evidence")
        if "error_type" in saved:
            errors = {cls.__name__: cls for cls in (
                r.ShadowRpcError, r.ShadowRpcProtocolError, r.ShadowRpcResponseError,
                r.ShadowRpcTransportError, ContextFenceError,
            )}
            raise errors.get(saved["error_type"], RuntimeError)(saved["error_reason"])
        response = _decode(operation, saved["response"])
        if minimum is not None and hasattr(response, "context_slot") and response.context_slot < minimum:
            raise ContextFenceError("journal context below requested minimum")
        return response

    def get_multiple_accounts(self, pubkeys: tuple[str, ...], *, min_context_slot: int | None = None):
        return self._call("accounts", {"pubkeys": list(pubkeys), "minimum": min_context_slot},
                          lambda: self.owner.rpc.get_multiple_accounts(pubkeys, min_context_slot=min_context_slot), min_context_slot)

    def get_latest_blockhash(self, *, commitment: str, min_context_slot: int):
        return self._call("blockhash", {"commitment": commitment, "minimum": min_context_slot},
                          lambda: self.owner.rpc.get_latest_blockhash(commitment=commitment, min_context_slot=min_context_slot), min_context_slot)

    def get_block_height(self, *, commitment: str, min_context_slot: int):
        return self._call("height", {"commitment": commitment, "minimum": min_context_slot},
                          lambda: self.owner.rpc.get_block_height(commitment=commitment, min_context_slot=min_context_slot), min_context_slot)

    def is_blockhash_valid(self, blockhash: str, *, commitment: str, min_context_slot: int):
        return self._call("validity", {"blockhash": blockhash, "commitment": commitment, "minimum": min_context_slot},
                          lambda: self.owner.rpc.is_blockhash_valid(blockhash, commitment=commitment, min_context_slot=min_context_slot), min_context_slot)

    def simulate_transaction(self, transaction_base64: str, *, config: Mapping[str, Any]):
        return self._call("simulation", {"transaction_base64": transaction_base64, "config": dict(config)},
                          lambda: self.owner.rpc.simulate_transaction(transaction_base64, config=config), config["minContextSlot"])


class ContinuousShadowExecutionV01:
    def __init__(self, paper_db: str | Path, shadow_db: str | Path, actor_public_key: str,
                 rpc: r.StrictReadOnlySolanaRpcV01, *, cycle_limit: int = 10,
                 clock_us: Callable[[], int] = lambda: time.time_ns() // 1000,
                 fault_hook: Callable[[str, str], None] | None = None):
        if type(cycle_limit) is not int or not 1 <= cycle_limit <= 100:
            raise ValueError("cycle_limit must be an integer from 1 through 100")
        self.actor = s.PublicShadowActorV01(actor_public_key)
        self.rpc, self.limit, self.clock_us, self.fault_hook = rpc, cycle_limit, clock_us, fault_hook
        self.stack = ExitStack()
        try:
            self.entries = self.stack.enter_context(a.open_continuous_source_bridge(paper_db, shadow_db))
            self.exits = self.stack.enter_context(b.open_shadow_lifecycle_bridge(paper_db, shadow_db))
            self.venue = self.stack.enter_context(open_venue_repository(shadow_db))
            self.simulation = self.stack.enter_context(open_simulation_repository(shadow_db))
            self.shadow = self.entries._shadow
            self.conn = self.entries._conn
            self.source_id = self.entries.source_identity.source_id
            if self.exits.source_identity != self.entries.source_identity:
                raise ReplayConflict("T004A/T004B Phase4 source identity mismatch")
            self._schema()
        except BaseException:
            self.stack.close()
            raise

    def _schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS shadow_c2_meta(singleton INTEGER PRIMARY KEY, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS shadow_c2_journal(
                intent_id TEXT NOT NULL, stage TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL,
                PRIMARY KEY(intent_id,stage));
            CREATE TABLE IF NOT EXISTS shadow_c2_work(
                intent_id TEXT PRIMARY KEY, done INTEGER NOT NULL DEFAULT 0, inventory_done INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS shadow_c2_comparisons(
                observation_id TEXT PRIMARY KEY, intent_id TEXT NOT NULL, payload TEXT NOT NULL, fingerprint TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS shadow_c2_comparison_heads(intent_id TEXT PRIMARY KEY, observation_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS shadow_c2_scan(singleton INTEGER PRIMARY KEY, after_intent TEXT NOT NULL);
            INSERT OR IGNORE INTO shadow_c2_scan VALUES(1,'');
        """)
        for table in ("shadow_c2_journal", "shadow_c2_comparisons", "shadow_c2_meta"):
            for operation in ("UPDATE", "DELETE"):
                self.conn.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_{operation.lower()} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'immutable C2 evidence'); END")
        payload = d.canonical_json({"model_fingerprint": MODEL_FINGERPRINT, "source_id": self.source_id,
                                    "actor_public_key": self.actor.public_key})
        row = self.conn.execute("SELECT payload FROM shadow_c2_meta WHERE singleton=1").fetchone()
        if row is not None and row[0] != payload:
            raise ReplayConflict("C2 actor/source/contract binding mismatch")
        self.conn.execute("INSERT OR IGNORE INTO shadow_c2_meta VALUES(1,?)", (payload,))
        self.conn.commit()

    def _fault(self, stage: str, intent_id: str):
        if self.fault_hook:
            self.fault_hook(stage, intent_id)

    def _read(self, intent_id: str, stage: str):
        row = self.conn.execute("SELECT payload,fingerprint FROM shadow_c2_journal WHERE intent_id=? AND stage=?", (intent_id, stage)).fetchone()
        if row is None:
            return None
        payload = json.loads(row[0])
        if d.canonical_json(payload) != row[0] or d.content_fingerprint(payload) != row[1]:
            raise ReplayConflict("C2 journal fingerprint conflict")
        return payload

    def _put(self, intent_id: str, stage: str, payload: dict[str, Any]):
        old = self._read(intent_id, stage)
        if old is not None:
            if old != payload:
                raise ReplayConflict("C2 immutable stage replay conflict")
            return
        with self.conn:
            self.conn.execute("INSERT INTO shadow_c2_journal VALUES(?,?,?,?)",
                              (intent_id, stage, d.canonical_json(payload), d.content_fingerprint(payload)))

    def _transition(self, intent, target, start, offset, reason, evidence):
        self.shadow.transition(intent.intent_id, target, idempotency_key=f"C2:{offset}:{target.value}",
                               effective_at_us=start + offset, reason_code=reason, evidence=evidence)
        self._fault("transition:" + target.value, intent.intent_id)

    def _venue_snapshot(self, rpc, intent, observed_at_us):
        primary = (v.derive_bonding_curve_pda(intent.mint), v.derive_pumpswap_pool_pda(intent.mint))

        def dependencies(batch):
            curve, pool = batch.accounts
            keys = list(primary)
            if curve is not None:
                keys += [intent.mint, v.derive_pump_global_pda(), v.derive_pump_fee_config_pda()]
            if pool is not None:
                decoded = v.decode_pumpswap_pool(pool, intent.mint)
                keys += [intent.mint, decoded.pool_base_token_account, decoded.pool_quote_token_account,
                         v.derive_pumpswap_global_pda(), v.derive_pumpswap_fee_config_pda()]
            return tuple(dict.fromkeys(keys))

        coherent = v.coherent_dependent_read(rpc, primary, dependencies, max_attempts=3)
        dependent_keys = dependencies(coherent.primary)
        accounts = dict(zip(dependent_keys, coherent.dependent.accounts, strict=True))
        for key, original in zip(primary, coherent.primary.accounts, strict=True):
            if accounts[key] != original:
                raise v.SnapshotCoherenceError("intermediate route-defining snapshot changed")
        slot = coherent.dependent.context_slot
        curve = pool = None
        if accounts[primary[0]] is not None:
            curve = v.build_pump_state(intent, accounts[primary[0]], accounts[intent.mint],
                accounts[v.derive_pump_global_pda()], accounts[v.derive_pump_fee_config_pda()],
                slot_min=slot, slot_max=slot, observed_at_us=observed_at_us)
        if accounts[primary[1]] is not None:
            decoded = v.decode_pumpswap_pool(accounts[primary[1]], intent.mint)
            pool = v.build_pumpswap_state(intent, accounts[primary[1]],
                accounts[decoded.pool_base_token_account], accounts[decoded.pool_quote_token_account],
                accounts[intent.mint], accounts[v.derive_pumpswap_global_pda()], accounts[v.derive_pumpswap_fee_config_pda()],
                slot_min=slot, slot_max=slot, observed_at_us=observed_at_us)
        return curve, pool, coherent.slot_max

    def _persist_venue(self, kind, obj, intent):
        table, identity, column = {
            "state": ("shadow_t002_venue_states", "state_id", "state_json"),
            "route": ("shadow_t002_routes", "route_id", "route_json"),
            "quote": ("shadow_t002_quotes", "quote_id", "quote_json"),
        }[kind]
        row = self.conn.execute(f"SELECT content_fingerprint,{column} FROM {table} WHERE {identity}=?", (getattr(obj, identity),)).fetchone()
        if row is not None:
            if tuple(row) != (obj.fingerprint, d.canonical_json(obj.payload())):
                raise ReplayConflict("accepted venue evidence conflicts with journal")
        else:
            {"state": self.venue.persist_state, "route": self.venue.persist_route, "quote": self.venue.persist_quote}[kind](obj)
        self._fault(kind, intent.intent_id)

    def _pipeline(self, intent, start, *, replay_only=False):
        rpc = _JournalRpc(self, intent.intent_id, replay_only)
        self._transition(intent, d.ShadowState.ELIGIBILITY_CHECKED, start, 1, "PERSISTED_PHASE4_INTENT", {"intent_fingerprint": intent.fingerprint})
        curve, pool, prerequisite = self._venue_snapshot(rpc, intent, start + 2)
        for state in (curve, pool):
            if state is not None:
                self._persist_venue("state", state, intent)
        route = v.decide_route(intent, curve, pool)
        if not route.executable:
            self._transition(intent, d.ShadowState.REJECTED, start, 3, route.reason_code, {"route_id": route.route_id, "outcome": route.outcome.value})
            self._persist_venue("route", route, intent)
            return None, None
        self._transition(intent, d.ShadowState.ROUTE_BOUND, start, 3, route.reason_code, {"route_id": route.route_id})
        self._persist_venue("route", route, intent)
        state = next(x for x in (curve, pool) if x is not None and x.state_id == route.selected_state_id)
        quote = v.create_executable_quote(intent, state, route, v.QuotePolicyV01(slippage_bps=2000))
        self._transition(intent, d.ShadowState.QUOTE_BOUND, start, 4, "EXECUTABLE_QUOTE", {"quote_id": quote.quote_id})
        self._persist_venue("quote", quote, intent)
        global_key = v.derive_pump_global_pda() if isinstance(state, v.PumpBondingCurveStateV01) else v.derive_pumpswap_global_pda()
        base_key = s.derive_associated_token_address(self.actor.public_key, intent.mint, state.base_token_program)
        quote_key = s.derive_associated_token_address(self.actor.public_key, v.WSOL_MINT, v.TOKEN_PROGRAM_ID)
        actor_read = rpc.get_multiple_accounts((base_key, quote_key, global_key), min_context_slot=max(prerequisite, state.slot_max))
        base_account, quote_account, global_account = actor_read.accounts
        try:
            snapshot = s.build_actor_account_snapshot(state, self.actor, base_account=base_account,
                quote_account=quote_account, context_slot=actor_read.context_slot, observed_at_us=start + 5)
        except s.PlanError as exc:
            raise ReplayConflict(str(exc)) from exc
        self._put(intent.intent_id, "actor", snapshot.payload())
        if intent.role is d.IntentRole.EXIT and not snapshot.base_exists:
            self._transition(intent, d.ShadowState.REJECTED, start, 6, EXPECTED_ONLY,
                             {"quote_id": quote.quote_id, "actor_snapshot_fingerprint": snapshot.fingerprint})
            return quote, None
        if global_account is None:
            raise ReplayConflict("recipient global account missing")
        try:
            recipients = s.decode_verified_recipient_evidence(state, global_account)
        except s.PlanError as exc:
            raise ReplayConflict(str(exc)) from exc
        plan = s.build_unsigned_transaction_plan(intent, state, route, quote, self.actor,
            s.TransactionPlanPolicyV01(), recipients, snapshot)
        self.simulation.persist_plan(plan)
        self._fault("plan", intent.intent_id)
        record_plan_state(self.shadow, self.simulation, plan, effective_at_us=start + 6)
        self._fault("transition:PLAN_BUILT", intent.intent_id)
        run = s.run_bounded_simulation(rpc, plan, observed_at_us=start + 7, maximum_refreshes=1)
        self.simulation.persist_run(run)
        self._fault("simulation", intent.intent_id)
        record_simulation_state(self.shadow, self.simulation, plan, run, effective_at_us=start + 30)
        self._fault("transition:SIMULATION_TERMINAL", intent.intent_id)
        return quote, run.final_result

    def _execute(self, intent):
        identity = intent.intent_id
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO shadow_c2_work(intent_id) VALUES(?)", (identity,))
        beginning = None
        try:
            beginning = self._read(identity, "start")
            if beginning is None:
                beginning = {"at_us": max(self.clock_us(), intent.decision_at_us), "intent_fingerprint": intent.fingerprint}
                self._put(identity, "start", beginning)
            if (beginning["intent_fingerprint"] != intent.fingerprint
                    or type(beginning["at_us"]) is not int or beginning["at_us"] < intent.decision_at_us):
                raise ReplayConflict("intent/start evidence changed after execution start")
            if intent.role is d.IntentRole.EXIT and self._read(intent.parent_entry_intent_id, "integrity_failure"):
                raise ReplayConflict("parent ENTRY has terminal execution integrity failure")
            self._pipeline(intent, beginning["at_us"])
        except Exception as exc:
            current = self.shadow.current_state(identity)
            failure = {"exception_type": type(exc).__name__,
                       "original_exception_type": type(exc.__cause__).__name__ if exc.__cause__ else type(exc).__name__,
                       "original_reason": str(exc) if isinstance(exc, (v.QuoteError, v.VenueStateError, v.SnapshotCoherenceError, s.PlanError, r.ShadowRpcError, ContextFenceError, ReplayConflict)) else type(exc).__name__}
            self._put(identity, "failure", failure)
            if current not in d.TERMINAL_STATES:
                target = d.ShadowState.REJECTED if isinstance(exc, (v.QuoteError, s.PlanError)) and current in (d.ShadowState.CREATED, d.ShadowState.ELIGIBILITY_CHECKED, d.ShadowState.ROUTE_BOUND, d.ShadowState.QUOTE_BOUND) else d.ShadowState.FAILED
                failure_at = max(self.clock_us(), self.shadow.history(identity)[-1].effective_at_us)
                if beginning is not None and type(beginning.get("at_us")) is int:
                    failure_at = max(failure_at, beginning["at_us"] + 40)
                # Replaying an already persisted failure never changes its time.
                failure_clock = self._read(identity, "failure_clock")
                if failure_clock is None:
                    failure_clock = {"at_us": failure_at}
                    self._put(identity, "failure_clock", failure_clock)
                self._transition(intent, target, failure_clock["at_us"], 0, type(exc).__name__, failure)
            elif isinstance(exc, d.ShadowDeterminismConflict) or current is d.ShadowState.COMPLETED:
                self._put(identity, "integrity_failure", failure)
        if self._read(identity, "finished") is None:
            self._put(identity, "finished", {"terminal": self.shadow.current_state(identity).value,
                "recorded_at_us": max(self.clock_us(), self.shadow.history(identity)[-1].effective_at_us)})
        self._fault("finished", identity)
        with self.conn:
            self.conn.execute("UPDATE shadow_c2_work SET done=1 WHERE intent_id=?", (identity,))
        self._fault("entry_done" if intent.role is d.IntentRole.ENTRY else "exit_done", identity)

    def _scope(self):
        return """SELECT intent_id FROM shadow_t004a_entry_lineage WHERE source_id=?
                  UNION SELECT shadow_exit_intent_id FROM shadow_t004b_exit_source_evidence
                  WHERE source_id=? AND shadow_exit_intent_id IS NOT NULL"""

    def _pending(self, role):
        return [self.shadow.get_intent(row[0]) for row in self.conn.execute(
            "SELECT i.intent_id FROM shadow_execution_intents i LEFT JOIN shadow_c2_work w ON w.intent_id=i.intent_id "
            f"WHERE i.intent_id IN ({self._scope()}) AND i.role=? AND COALESCE(w.done,0)=0 ORDER BY i.decision_at_us,i.intent_id LIMIT ?",
            (self.source_id, self.source_id, role, self.limit))]

    def _inventories(self):
        rows = self.conn.execute("SELECT w.intent_id FROM shadow_c2_work w JOIN shadow_execution_intents i USING(intent_id) "
            "WHERE w.done=1 AND w.inventory_done=0 AND i.role='ENTRY' ORDER BY w.intent_id LIMIT ?", (self.limit,)).fetchall()
        for row in rows:
            intent = self.shadow.get_intent(row[0])
            if not self._read(intent.intent_id, "integrity_failure") and self.shadow.current_state(intent.intent_id) is d.ShadowState.COMPLETED:
                try:
                    start = self._read(intent.intent_id, "start")["at_us"]
                    quote, result = self._pipeline(intent, start, replay_only=True)
                    self.exits.materialize_expected_inventory(intent, quote, result, evidence_at_us=start + 41)
                    self._fault("inventory", intent.intent_id)
                except Exception as exc:
                    self._put(intent.intent_id, "integrity_failure", {
                        "exception_type": type(exc).__name__, "original_reason": "INVENTORY_CHAIN_VALIDATION_FAILED"})
            with self.conn:
                self.conn.execute("UPDATE shadow_c2_work SET inventory_done=1 WHERE intent_id=?", (intent.intent_id,))

    def comparison(self, intent_id):
        def verified(row):
            payload = json.loads(row[0])
            if d.canonical_json(payload) != row[0] or d.content_fingerprint(payload) != row[1]:
                raise ReplayConflict("comparison source evidence fingerprint conflict")
            return payload
        intent = self.shadow.get_intent(intent_id)
        history = self.shadow.history(intent_id)
        final = history[-1]
        route = self.conn.execute("SELECT route_json,content_fingerprint FROM shadow_t002_routes WHERE intent_id=?", (intent_id,)).fetchall()
        quotes = self.conn.execute("SELECT quote_json,content_fingerprint FROM shadow_t002_quotes WHERE intent_id=?", (intent_id,)).fetchall()
        if len(route) > 1 or len(quotes) > 1:
            raise ReplayConflict("multiple economic snapshots for one C2 intent")
        route = verified(route[0]) if route else None
        quote = verified(quotes[0]) if quotes else None
        result = self.conn.execute("SELECT r.result_json,r.content_fingerprint FROM shadow_t003_results r JOIN shadow_t003_attempts a USING(attempt_id) JOIN shadow_t003_plans p USING(plan_id) WHERE p.intent_id=? ORDER BY a.attempt_index DESC LIMIT 1", (intent_id,)).fetchone()
        result = verified(result) if result else None
        if intent.role is d.IntentRole.ENTRY:
            lineage = self.conn.execute("SELECT lineage_json,content_fingerprint,route_id FROM shadow_t004a_entry_lineage WHERE intent_id=?", (intent_id,)).fetchone()
            paper = self.entries._paper_conn.execute("SELECT state,state_reason FROM paper_entry_routes WHERE route_id=?", (lineage[2],)).fetchone()
            if paper is None:
                raise ReplayConflict("paper route disappeared")
            paper_data = {"route_id": lineage[2], "state": paper[0], "reason": paper[1], "lineage": verified(lineage)}
        else:
            lineage = self.conn.execute("SELECT evidence_json,content_fingerprint FROM shadow_t004b_exit_source_evidence WHERE shadow_exit_intent_id=?", (intent_id,)).fetchone()
            paper_data = verified(lineage)["phase4_exit"]
            paper = self.entries._paper_conn.execute("SELECT state FROM paper_exit_track_states WHERE paper_position_id=?", (intent.exit_lifecycle_id,)).fetchone()
            paper_data = dict(paper_data, state=None if paper is None else paper[0])
        outcome = None if result is None else result["outcome"]
        integrity = self._read(intent_id, "integrity_failure")
        success = outcome == "SIMULATION_SUCCESS" and final.to_state is d.ShadowState.COMPLETED
        classification = ("INTEGRITY_FAILURE" if integrity else EXPECTED_ONLY if final.reason_code == EXPECTED_ONLY else
            f"PAPER_{paper_data['state']}+SHADOW_{'SIM_SUCCESS' if success else 'NON_SUCCESS' if final.to_state in d.TERMINAL_STATES else 'PENDING'}")
        finished = self._read(intent_id, "finished")
        return {"model_fingerprint": MODEL_FINGERPRINT, "intent_id": intent_id, "intent_fingerprint": intent.fingerprint,
            "role": intent.role.value, "track_id": intent.exit_track_id, "parent_entry_intent_id": intent.parent_entry_intent_id,
            "paper": paper_data, "shadow_state": final.to_state.value, "shadow_reason_code": final.reason_code,
            "shadow_reason_evidence": json.loads(final.evidence_json), "route": route,
            "route_venue": None if quote is None else quote["venue"], "quote_available": quote is not None,
            "quote": quote, "simulation_outcome": outcome, "simulation_reason": None if result is None else result["reason_code"],
            "simulation_error": None if result is None else result["error"], "integrity_failure": integrity,
            "decision_to_shadow_us": None if finished is None else finished["recorded_at_us"] - intent.decision_at_us,
            "timing_basis": "DURABLE_TERMINAL_OBSERVATION_WALL_CLOCK_NOT_CHAIN_ORDER",
            "classification": classification, "snapshots_may_differ": True}

    def persist_comparison(self, intent_id):
        # Capture once: Paper may legitimately advance while this observation is
        # being persisted. A later cycle records a new immutable observation.
        payload = self.comparison(intent_id)
        fingerprint = d.content_fingerprint(payload)
        identity = d.deterministic_id("P5C2CMP", payload["intent_id"], fingerprint)
        expected = (payload["intent_id"], d.canonical_json(payload), fingerprint)
        row = self.conn.execute("SELECT intent_id,payload,fingerprint FROM shadow_c2_comparisons WHERE observation_id=?", (identity,)).fetchone()
        if row is not None and tuple(row) != expected:
            raise ReplayConflict("comparison identity conflict")
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO shadow_c2_comparisons VALUES(?,?,?,?)", (identity, *expected))
            self.conn.execute("INSERT INTO shadow_c2_comparison_heads VALUES(?,?) ON CONFLICT(intent_id) DO UPDATE SET observation_id=excluded.observation_id", (payload["intent_id"], identity))
        self._fault("comparison", payload["intent_id"])
        return identity

    def _comparisons(self):
        after = self.conn.execute("SELECT after_intent FROM shadow_c2_scan WHERE singleton=1").fetchone()[0]
        rows = self.conn.execute(f"SELECT intent_id FROM ({self._scope()}) WHERE intent_id>? ORDER BY intent_id LIMIT ?",
                                 (self.source_id, self.source_id, after, self.limit)).fetchall()
        if not rows:
            rows = self.conn.execute(f"SELECT intent_id FROM ({self._scope()}) ORDER BY intent_id LIMIT ?",
                                     (self.source_id, self.source_id, self.limit)).fetchall()
        for row in rows:
            self.persist_comparison(row[0])
            with self.conn:
                self.conn.execute("UPDATE shadow_c2_scan SET after_intent=? WHERE singleton=1", (row[0],))

    def summary(self):
        groups = {}
        for row in self.conn.execute("SELECT h.intent_id,c.payload,c.fingerprint FROM shadow_c2_comparison_heads h JOIN shadow_c2_comparisons c USING(observation_id)"):
            payload = json.loads(row[1])
            if payload["intent_id"] != row[0] or d.content_fingerprint(payload) != row[2]:
                raise ReplayConflict("comparison content conflict")
            key = (payload["role"], payload["track_id"], payload["paper"]["state"], payload["shadow_state"],
                   payload["simulation_outcome"], payload["route_venue"], payload["classification"])
            groups[key] = groups.get(key, 0) + 1
        return [{"role": k[0], "track": k[1], "paper_state": k[2], "shadow_state": k[3],
                 "simulation_outcome": k[4], "venue": k[5], "classification": k[6], "count": n}
                for k, n in sorted(groups.items(), key=lambda item: str(item[0]))]

    def cycle(self):
        self.entries.poll_once(limit=self.limit)
        # A crash can leave terminal entries awaiting stage 3. Drain that bounded
        # backlog before producing more terminal entries; do not starve old work.
        inventory_backlog = self.conn.execute("SELECT 1 FROM shadow_c2_work w JOIN shadow_execution_intents i USING(intent_id) WHERE w.done=1 AND w.inventory_done=0 AND i.role='ENTRY' LIMIT 1").fetchone()
        if not inventory_backlog:
            for intent in self._pending("ENTRY"):
                self._execute(intent)
        self._inventories()
        inventory_backlog = self.conn.execute("SELECT 1 FROM shadow_c2_work w JOIN shadow_execution_intents i USING(intent_id) WHERE w.done=1 AND w.inventory_done=0 AND i.role='ENTRY' LIMIT 1").fetchone()
        if not inventory_backlog:
            self.exits.poll_exit_once(limit=self.limit)
        for intent in self._pending("EXIT"):
            self._execute(intent)
        self._comparisons()

    def close(self):
        self.stack.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
