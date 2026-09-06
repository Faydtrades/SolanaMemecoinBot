from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import sys
import tempfile
from contextlib import closing
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import phase5_shadow_unsigned_plan_simulation_selftest_v0_1 as fx
import phase5_shadow_continuous_execution_v0_1 as runner
from phase5 import shadow_continuous_execution_v0_1 as c


EXPECTED_MODEL_FINGERPRINT = "a848a74533872e2e2d7f1cfc2e690df2f285932d2bf87fcf00f6bb83075cba56"


def paper_fixture(path, *, state="FILLED", exits=False, entries=1, run_ids=None):
    stamp = datetime.fromtimestamp(fx.BASE_US / 1_000_000, timezone.utc).isoformat()
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
        CREATE TABLE paper_continuous_signal_contexts_v0_1(
          signal_key TEXT, route_id TEXT, source_evaluation_id TEXT, run_id TEXT, mint TEXT,
          strategy_version TEXT, parameter_set_id TEXT, source_cursor INTEGER, source_event_key TEXT);
        CREATE TABLE paper_entry_routes(route_id TEXT, candidate_id TEXT, mint TEXT, strategy_version TEXT,
          parameter_set_id TEXT, signal_observed_at TEXT, signal_ingest_seq INTEGER,
          requested_size_lamports INTEGER, state TEXT, state_reason TEXT);
        CREATE TABLE paper_exit_intents(exit_intent_id TEXT, paper_position_id TEXT, paper_order_id TEXT,
          signal_key TEXT, mint TEXT, track_id TEXT, exit_variant TEXT, reason TEXT, requested_at TEXT,
          trigger_observed_at TEXT, trigger_ingest_seq INTEGER, trigger_source_event_key TEXT,
          trigger_price_numerator_raw TEXT, trigger_price_denominator_raw TEXT, rule_return_bps INTEGER,
          trail_peak_return_bps INTEGER, last_fresh_observed_at TEXT, last_fresh_ingest_seq INTEGER,
          last_fresh_return_bps INTEGER);
        CREATE TABLE paper_exit_track_states(paper_position_id TEXT, paper_order_id TEXT, signal_key TEXT,
          mint TEXT, track_id TEXT, exit_variant TEXT, signal_ingest_seq INTEGER, state TEXT, exit_intent_id TEXT);
        """)
        run_ids = tuple(run_ids or ("synthetic-run",) * entries)
        if len(run_ids) != entries:
            raise ValueError("run_ids must match entries")
        for index in range(entries):
            db.execute("INSERT INTO paper_continuous_signal_contexts_v0_1 VALUES(?,?,?,?,?,?,?,?,?)",
                (f"signal{index}", f"route{index}", f"eval{index}", run_ids[index], fx.fx.MINT,
                 "strategy", "parameters", index + 1, f"event{index}"))
            db.execute("INSERT INTO paper_entry_routes VALUES(?,?,?,?,?,?,?,?,?,?)",
                (f"route{index}", f"candidate{index}", fx.fx.MINT, "strategy", "parameters",
                 stamp, index + 1, 100_000_000, state, "SYNTHETIC_PAPER"))
        if exits:
            for index in range(entries):
                for track in ("FINAL-A", "FINAL-B", "SENS-C"):
                    suffix = f"{index}-{track}"
                    db.execute("INSERT INTO paper_exit_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        ("exit"+suffix, "position"+suffix, "order"+suffix, f"signal{index}", fx.fx.MINT,
                         track, "variant"+track, "TAKE_PROFIT", stamp, stamp, index + 2,
                         "exit-event"+str(index), "2", "1", 1000, None, stamp, index + 2, 1000))
                    db.execute("INSERT INTO paper_exit_track_states VALUES(?,?,?,?,?,?,?,?,?)",
                        ("position"+suffix, "order"+suffix, f"signal{index}", fx.fx.MINT, track,
                         "variant"+track, index + 1, "EXIT_INTENT", "exit"+suffix))


class Network:
    def __init__(self, *, venue="pump", base=False, outcome="success", stale=None):
        self.venue, self.base, self.outcome, self.stale = venue, base, outcome, stale
        self.calls = []
        v = c.v
        accounts = [fx.fx.mint_account(), fx.fx.fee_account(v.PUMP_PROGRAM_ID),
                    fx.fx.fee_account(v.PUMPSWAP_PROGRAM_ID),
                    fx.fx.account(v.derive_pump_global_pda(), v.PUMP_PROGRAM_ID, fx.pump_global_full()),
                    fx.fx.account(v.derive_pumpswap_global_pda(), v.PUMPSWAP_PROGRAM_ID, fx.pumpswap_global_full())]
        if venue != "none":
            accounts.append(fx.fx.account(v.derive_bonding_curve_pda(fx.fx.MINT), v.PUMP_PROGRAM_ID,
                                         fx.curve_bytes(complete=venue == "swap")))
        if venue in ("swap", "ambiguous"):
            accounts.extend([fx.fx.account(v.derive_pumpswap_pool_pda(fx.fx.MINT), v.PUMPSWAP_PROGRAM_ID, fx.pool_bytes()),
                fx.fx.token_account(fx.fx.BASE_VAULT, fx.fx.MINT, 500_000_000_000),
                fx.fx.token_account(fx.fx.QUOTE_VAULT, v.WSOL_MINT, 20_000_000_000)])
        if base:
            accounts.append(fx.actor_token(c.s.derive_associated_token_address(fx.ACTOR, fx.fx.MINT, v.TOKEN_PROGRAM_ID),
                                           fx.fx.MINT, 500_000_000_000, v.TOKEN_PROGRAM_ID))
        self.accounts = {x.pubkey: x for x in accounts}

    def handler(self, request):
        body = json.loads(request.content)
        method, params = body["method"], body["params"]
        self.calls.append(body)
        if self.outcome == "transport":
            raise httpx.ConnectError("synthetic transport secret must be redacted", request=request)
        config = params[-1]
        slot = max(fx.fx.SLOT, config.get("minContextSlot", 0))
        label = method
        if method == "getMultipleAccounts":
            label = "actor" if len(params[0]) == 3 else "dependent" if len(params[0]) > 3 else "primary"
        if self.stale == label and config.get("minContextSlot") is not None:
            slot = config["minContextSlot"] - 1
        if method == "getMultipleAccounts":
            value = []
            for key in params[0]:
                account = self.accounts.get(key)
                value.append(None if account is None else {"owner": account.owner,
                    "data": [base64.b64encode(account.data).decode(), "base64"], "executable": False,
                    "lamports": 1, "rentEpoch": 0})
        elif method == "getLatestBlockhash":
            value = {"blockhash": str(fx.Hash.default()), "lastValidBlockHeight": 200}
        elif method == "getBlockHeight":
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": 201 if self.outcome == "expired" else 100})
        elif method == "isBlockhashValid":
            value = self.outcome != "expired"
        elif method == "simulateTransaction":
            assert config["sigVerify"] is False and config["replaceRecentBlockhash"] is False
            transaction = fx.Transaction.from_bytes(base64.b64decode(params[0]))
            assert all(bytes(signature) == bytes(64) for signature in transaction.signatures)
            value = {"err": {"InstructionError": [0, {"Custom": 6001}]} if self.outcome == "program" else None,
                     "logs": ["synthetic program evidence"], "unitsConsumed": 123, "returnData": None,
                     "innerInstructions": []}
            if self.outcome == "malformed":
                value = {"logs": []}
        else:
            raise AssertionError("unauthorized RPC method " + method)
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": body["id"], "result": {"context": {"slot": slot}, "value": value}})

    def rpc(self, url="https://synthetic.invalid"):
        return c.r.StrictReadOnlySolanaRpcV01(url, transport=httpx.MockTransport(self.handler))


def main():
    checks = {}
    def check(name, condition):
        if not condition:
            raise AssertionError(name)
        checks[name] = True
    root = ROOT / "data" / "shadow"
    root.mkdir(parents=True, exist_ok=True)
    check("exact corrected contract fingerprint", c.MODEL_FINGERPRINT == EXPECTED_MODEL_FINGERPRINT)
    with tempfile.TemporaryDirectory(prefix="t004c2_", dir=root) as directory:
        temporary = Path(directory)
        def case(name, network=None, **paper_args):
            path = temporary / (name + ".paper.sqlite3")
            paper_fixture(path, **paper_args)
            shadow = temporary / (name + ".shadow.sqlite3")
            return path, shadow, network or Network()
        paper, shadow, net = case("entry")
        before = paper.read_bytes()
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
            app.cycle()
            rows = app.conn.execute("SELECT intent_id FROM shadow_execution_intents").fetchall()
            identity = rows[0][0]
            comparison = app.comparison(identity)
            check("entry completed", comparison["shadow_state"] == "COMPLETED")
            check("entry expected inventory", app.exits.get_inventory(identity) is not None)
            check("paper filled comparison", comparison["classification"] == "PAPER_FILLED+SHADOW_SIM_SUCCESS")
            check("paper byte unchanged", paper.read_bytes() == before)
            check("read only source connections", app.entries._paper_conn.execute("PRAGMA query_only").fetchone()[0] == 1 and app.exits._paper_conn.execute("PRAGMA query_only").fetchone()[0] == 1)
            count = app.conn.execute("SELECT COUNT(*) FROM shadow_c2_comparisons").fetchone()[0]
            calls = len(net.calls)
            app.cycle()
            check("comparison replay idempotent", count == app.conn.execute("SELECT COUNT(*) FROM shadow_c2_comparisons").fetchone()[0] and len(net.calls) == calls)
            observation = app.persist_comparison(identity)
            with closing(sqlite3.connect(paper)) as writer, writer:
                writer.execute("UPDATE paper_entry_routes SET state='REJECTED'")
            original_id = c.d.deterministic_id
            def comparison_collision(*parts):
                return observation if parts[0] == "P5C2CMP" else original_id(*parts)
            with patch.object(c.d, "deterministic_id", side_effect=comparison_collision):
                check("comparison conflict fail closed", fx.expect(c.ReplayConflict, lambda: app.persist_comparison(identity)))
            app.persist_comparison(identity)
            check("paper advancement creates observation", app.conn.execute("SELECT COUNT(*) FROM shadow_c2_comparisons").fetchone()[0] == count + 1)
            check("accepted evidence audits", app.venue.audit() and app.simulation.audit() and app.shadow.quick_check() == "ok")
        for name, options, expected in (
            ("paper_rejected", {}, ("COMPLETED", "SIMULATION_SUCCESS")),
            ("swap", {"venue": "swap"}, ("COMPLETED", "SIMULATION_SUCCESS")),
            ("none", {"venue": "none"}, ("REJECTED", None)),
            ("ambiguous", {"venue": "ambiguous"}, ("REJECTED", None)),
            ("transport", {"outcome": "transport"}, ("FAILED", None)),
            ("expiry", {"outcome": "expired"}, ("EXPIRED", "BLOCKHASH_EXPIRED")),
            ("program", {"outcome": "program"}, ("FAILED", "PROGRAM_REJECTED")),
            ("malformed", {"outcome": "malformed"}, ("FAILED", "MALFORMED_RESPONSE")),
        ):
            paper, shadow, net = case(name, Network(**options), state="REJECTED" if name == "paper_rejected" else "FILLED")
            before = paper.read_bytes()
            with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
                app.cycle()
                identity = app.conn.execute("SELECT intent_id FROM shadow_execution_intents").fetchone()[0]
                result = app.comparison(identity)
                check(name + " terminal outcome " + str((result["shadow_state"], result["simulation_outcome"])), (result["shadow_state"], result["simulation_outcome"]) == expected)
                if name == "program":
                    check("program exact original error", result["simulation_error"] == {"InstructionError": [0, {"Custom": 6001}]} and result["simulation_reason"] == "SIMULATION_PROGRAM_REJECTED")
                if name == "paper_rejected":
                    check("paper rejected shadow success", result["classification"] == "PAPER_REJECTED+SHADOW_SIM_SUCCESS")
                if name == "swap":
                    check("onchain swap selected", result["route_venue"] == "PUMPSWAP_CANONICAL")
                check(name + " paper immutable", paper.read_bytes() == before)
        for stale in ("dependent", "primary", "actor", "getLatestBlockhash", "isBlockhashValid", "simulateTransaction"):
            paper, shadow, net = case("stale" + stale, Network(stale=stale))
            with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
                app.cycle()
                identity = app.conn.execute("SELECT intent_id FROM shadow_execution_intents").fetchone()[0]
                check("slot fence " + stale, app.shadow.current_state(identity) is c.d.ShadowState.FAILED and app.exits.get_inventory(identity) is None)
        for venue in ("pump", "swap"):
            for base in (False, True):
                name = venue + "_exit_" + str(base)
                paper, shadow, net = case(name, Network(venue=venue, base=base), exits=True)
                before = paper.read_bytes()
                with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
                    app.cycle()
                    rows = app.conn.execute("SELECT intent_id FROM shadow_execution_intents WHERE role='EXIT'").fetchall()
                    check(name + " three exits", len(rows) == 3)
                    parents, tracks, amounts = set(), set(), set()
                    for row in rows:
                        intent = app.shadow.get_intent(row[0])
                        result = app.comparison(row[0])
                        parents.add(intent.parent_entry_intent_id)
                        tracks.add(intent.exit_track_id)
                        amounts.add(intent.input_amount_base_units)
                        plans = app.conn.execute("SELECT COUNT(*) FROM shadow_t003_plans WHERE intent_id=?", (row[0],)).fetchone()[0]
                        check(name + " " + intent.exit_track_id, result["quote_available"] and (
                            result["shadow_state"] == "COMPLETED" and result["simulation_outcome"] == "SIMULATION_SUCCESS" and plans == 1
                            if base else result["shadow_state"] == "REJECTED" and result["shadow_reason_code"] == c.EXPECTED_ONLY and plans == 0 and result["simulation_outcome"] is None))
                    inventory = app.exits.get_inventory(next(iter(parents)))
                    check(name + " independent full inventory", len(parents) == 1 and tracks == {"FINAL-A", "FINAL-B", "SENS-C"} and amounts == {inventory.expected_base_amount})
                    check(name + " summary distinct", len(app.summary()) == 4)
                    app.cycle()
                    check(name + " inventory never decremented", inventory == app.exits.get_inventory(next(iter(parents))))
                    check(name + " paper immutable", paper.read_bytes() == before)
        stages = ("transition:ELIGIBILITY_CHECKED", "state", "transition:ROUTE_BOUND", "route",
                  "transition:QUOTE_BOUND", "quote", "plan", "transition:PLAN_BUILT", "simulation",
                  "transition:SIMULATION_TERMINAL", "inventory", "comparison", "simulated",
                  "journal:rpc:0", "journal:rpc:3", "journal:rpc:7", "finished", "entry_done")
        for index, stage in enumerate(stages):
            paper, shadow, net = case("crash" + str(index))
            def fault(at, _intent):
                if at == stage:
                    raise c.InjectedCrash(stage)
            with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100, fault_hook=fault) as app:
                if stage == "simulated":
                    transition = app.shadow.transition
                    def injected(*args, **kwargs):
                        value = transition(*args, **kwargs)
                        if args[1] is c.d.ShadowState.SIMULATED:
                            raise c.InjectedCrash(stage)
                        return value
                    app.shadow.transition = injected
                check("crash reached " + stage, fx.expect(c.InjectedCrash, app.cycle))
                immutable = {table: list(app.conn.execute("SELECT * FROM " + table)) for table in (
                    "shadow_t002_venue_states", "shadow_t002_routes", "shadow_t002_quotes", "shadow_t003_plans", "shadow_t003_results")}
            with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+9999) as app:
                app.cycle()
                identity = app.conn.execute("SELECT intent_id FROM shadow_execution_intents").fetchone()[0]
                check("restart complete " + stage, app.shadow.current_state(identity) is c.d.ShadowState.COMPLETED and app.exits.get_inventory(identity) is not None)
                check("restart exact evidence " + stage, all(all(tuple(row) in [tuple(x) for x in app.conn.execute("SELECT * FROM " + table)] for row in rows) for table, rows in immutable.items()))
                check("one terminal " + stage, sum(x.to_state in c.d.TERMINAL_STATES for x in app.shadow.history(identity)) == 1)
                calls = len(net.calls)
                digest = app.simulation.canonical_digest()
                app.cycle()
                check("replay no RPC/economic writes " + stage, len(net.calls) == calls and app.simulation.canonical_digest() == digest)
        for outcome, expected in (("program", "FAILED"), ("expired", "EXPIRED"), ("transport", "FAILED")):
            paper, shadow, net = case("negative_restart_" + outcome, Network(outcome=outcome))
            def terminal_fault(stage, _identity):
                if stage == "finished":
                    raise c.InjectedCrash(stage)
            with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100, fault_hook=terminal_fault) as app:
                check(outcome + " terminal crash", fx.expect(c.InjectedCrash, app.cycle))
                identity = app.conn.execute("SELECT intent_id FROM shadow_execution_intents").fetchone()[0]
                history = app.shadow.history(identity)
            net.outcome = "success"
            calls = len(net.calls)
            with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+999999) as app:
                app.cycle()
                check(outcome + " no resurrection", app.shadow.current_state(identity).value == expected and app.exits.get_inventory(identity) is None)
                check(outcome + " exact failure replay", history == app.shadow.history(identity) and calls == len(net.calls))
        paper, shadow, net = case("failure_isolation", entries=2)
        original_handler = net.handler
        def first_failure(request):
            if not net.calls:
                net.outcome = "transport"
            try:
                return original_handler(request)
            finally:
                net.outcome = "success"
        net.handler = first_failure
        before = paper.read_bytes()
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
            app.cycle()
            check("failed intent does not block next entry", sorted(x[0] for x in app.conn.execute("SELECT current_state FROM shadow_state_machines")) == ["COMPLETED", "FAILED"])
            check("failure isolation paper unchanged", paper.read_bytes() == before)
        paper, shadow, net = case("backlog", entries=3, exits=True)
        def after_entry(stage, _identity):
            if stage == "entry_done":
                raise c.InjectedCrash(stage)
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, cycle_limit=1, clock_us=lambda: fx.BASE_US+100, fault_hook=after_entry) as app:
            check("inventory backlog crash", fx.expect(c.InjectedCrash, app.cycle))
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, cycle_limit=1, clock_us=lambda: fx.BASE_US+100) as app:
            for _ in range(16):
                count_before = app.conn.execute("SELECT COUNT(*) FROM shadow_c2_work WHERE done=1").fetchone()[0]
                app.cycle()
                count_after = app.conn.execute("SELECT COUNT(*) FROM shadow_c2_work WHERE done=1").fetchone()[0]
                check("bounded work per cycle", count_after - count_before <= 2)
                if count_after == 12:
                    break
            check("backlog eventually drains entries after exits", app.conn.execute("SELECT COUNT(*) FROM shadow_c2_work WHERE done=1").fetchone()[0] == 12)
            check("inventory backlog no false missing-inventory error", app.conn.execute("SELECT COUNT(*) FROM shadow_t004b_expected_inventory").fetchone()[0] == 3)
        paper, shadow, net = case("invalid_actor", Network(base=True))
        key = c.s.derive_associated_token_address(fx.ACTOR, fx.fx.MINT, c.v.TOKEN_PROGRAM_ID)
        net.accounts[key] = fx.fx.token_account(key, fx.fx.MINT, 1000, authority=fx.fx.ADMIN)
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
            app.cycle()
            identity = app.conn.execute("SELECT intent_id FROM shadow_execution_intents").fetchone()[0]
            check("malformed actor FAILED not deterministic REJECTED", app.shadow.current_state(identity) is c.d.ShadowState.FAILED)
        paper, shadow, net = case("journal_conflict", entries=2)
        def state_crash(stage, _identity):
            if stage == "state":
                raise c.InjectedCrash(stage)
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100, fault_hook=state_crash) as app:
            check("conflict fixture crash", fx.expect(c.InjectedCrash, app.cycle))
            identity = app.conn.execute("SELECT intent_id FROM shadow_c2_work").fetchone()[0]
            with app.conn:
                app.conn.execute("DROP TRIGGER shadow_c2_journal_update")
                app.conn.execute("UPDATE shadow_c2_journal SET payload='{}' WHERE intent_id=? AND stage='start'", (identity,))
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
            app.cycle()
            check("corrupt start quarantined per intent", app.shadow.current_state(identity) is c.d.ShadowState.FAILED and app.conn.execute("SELECT COUNT(*) FROM shadow_state_machines WHERE current_state='COMPLETED'").fetchone()[0] == 1)
        # Evidence corruption must not turn a completed intent into a false comparison success.
        paper, shadow, net = case("comparison_corrupt")
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
            app.cycle()
            identity = app.conn.execute("SELECT intent_id FROM shadow_execution_intents").fetchone()[0]
            with app.conn:
                app.conn.execute("DROP TRIGGER shadow_t002_quotes_no_update")
                app.conn.execute("UPDATE shadow_t002_quotes SET quote_json='{}'")
            check("comparison source corruption fails closed", fx.expect(c.ReplayConflict, lambda: app.persist_comparison(identity)))
        paper, shadow, net = case("parent_quarantined", exits=True)
        with net.rpc() as rpc, c.ContinuousShadowExecutionV01(paper, shadow, fx.ACTOR, rpc, clock_us=lambda: fx.BASE_US+100) as app:
            app.entries.poll_once(limit=10)
            entry = app._pending("ENTRY")[0]
            app._execute(entry)
            app._inventories()
            app._put(entry.intent_id, "integrity_failure", {"reason": "synthetic detected conflict"})
            count = len(net.calls)
            app.cycle()
            check("parent integrity cannot produce successful exits", app.conn.execute("SELECT COUNT(*) FROM shadow_execution_intents i JOIN shadow_state_machines m USING(intent_id) WHERE i.role='EXIT' AND m.current_state='FAILED'").fetchone()[0] == 3 and len(net.calls) == count)
        for once in (True, False):
            paper, shadow, net = case("runner" + str(once))
            tick = [0.0]
            def sleep(seconds):
                tick[0] += seconds
            argv = ["--paper-db", str(paper), "--shadow-db", str(shadow), "--actor-public-key", fx.ACTOR,
                    "--rpc-url", "https://synthetic.invalid", "--poll-ms", "1000"]
            argv += ["--once"] if once else ["--duration-seconds", "2"]
            check("runner " + str(once), runner.main(argv, rpc_factory=net.rpc, monotonic=lambda: tick[0], sleep=sleep, clock_us=lambda: fx.BASE_US+100) == 0)
            check("duration bound " + str(once), tick[0] == (0 if once else 2))
    print(json.dumps({"model_id": c.MODEL_ID, "model_fingerprint": c.MODEL_FINGERPRINT,
                      "checks": checks, "check_count": len(checks)}, sort_keys=True))
    print("RESULT: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
