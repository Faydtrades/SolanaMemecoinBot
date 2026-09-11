"""B1 process supervision of the accepted owned startup/Runtime boundary.

Trusted host configuration and external inputs are spawn-serializable. Health
means completion of a requested Runtime step, never readiness or permission.
Only retained exact child handles are signalled; PID lookup/adoption is absent.
"""
from __future__ import annotations

import multiprocessing
import os
from dataclasses import dataclass
from time import monotonic_ns, sleep, time_ns
from uuid import uuid4

from .operations_ownership_v0_1 import OperationsStore, OwnerFence
from .operations_startup_v0_1 import StartupIdentity, start_live

VERSION = "live_operations_supervisor_v0.1"


@dataclass(frozen=True, slots=True)
class HealthProfile:
    startup_us: int
    progress_us: int
    termination_us: int

    def __post_init__(self):
        if any(type(v) is not int or not 1 <= v <= 86400000000
               for v in (self.startup_us, self.progress_us, self.termination_us)):
            raise ValueError("OPERATIONS_FINITE_HEALTH_PROFILE_REQUIRED")


@dataclass(frozen=True, slots=True)
class SupervisionFacts:
    state: str
    pid: int | None
    generation: int | None
    completed_steps: int
    last_work: str | None
    grants_permission: bool = False


def _runtime_child(channel, launch_id, configuration, external_inputs, now_us, generation):
    """No inherited owner: the original startup acquires inside the new process."""
    started = None
    try:
        started = start_live(**configuration, process_identity=launch_id+":"+str(os.getpid()),
                             now_us=now_us, replace_generation=generation)
        channel.send(("STARTED", started.audit.owner_fence))
        while channel.recv() == "STEP":
            # Host supplies external resources only. Original Runtime selects and
            # authorizes every work unit; no supervisor economic decision exists.
            result = started.runtime.step(**external_inputs(started))
            channel.send(("PROGRESS", result.work))
    except EOFError:
        pass
    finally:
        if started is not None:
            started.close()
        channel.close()


class OperationsSupervisor:
    """Serialized finite poll plus a minimal bounded driver, not a service.

    An existing orphan is never adopted automatically. A trusted host can supply
    its explicitly reviewed expected generation for a fresh CAS acquisition.
    Launch/pre-acquisition uncertainty, normal exit, ownership drift and failed
    termination latch this instance held. Reopening a supervisor is an explicit
    host action; it is not an automatic retry path. No reset API is called.
    """
    def __init__(self, configuration, external_inputs, health, *, expected_generation=None):
        if (type(health) is not HealthProfile or not callable(external_inputs)
                or type(configuration.get("expected_identity")) is not StartupIdentity
                or any(k in configuration for k in ("process_identity", "now_us", "replace_generation"))):
            raise ValueError("OPERATIONS_REVIEWED_CHILD_CONFIGURATION_REQUIRED")
        # Freeze a detached spawn copy. No subsequent caller dict edit can change
        # the restart input, and no owner/runtime capability is serialized.
        from pickle import dumps, loads
        self._configuration = loads(dumps(configuration))
        self._external_inputs, self.health = external_inputs, health
        self.store = OperationsStore(configuration["operations_path"], configuration["domain"])
        self._context = multiprocessing.get_context("spawn")
        self.process = self._channel = None
        self.fence = None
        self._generation = expected_generation
        self._launch_identity = None
        self._initial_generation = self.store.snapshot()["generation"]
        self._last_mono = None
        self._deadline = None
        self._waiting = False
        self._failed = False
        self._latched = None
        self.completed_steps, self.last_work = 0, None
        state = self.store.snapshot()
        if expected_generation is not None and (type(expected_generation) is not int
                                                or expected_generation != state["generation"]):
            raise ValueError("OPERATIONS_SUPERVISOR_GENERATION_CONFLICT")
        if state["nonce"] is not None and expected_generation is None:
            self._latched = "HELD_EXISTING_OWNER"

    def _facts(self, state):
        return SupervisionFacts(state, None if self.process is None else self.process.pid,
            None if self.fence is None else self.fence.generation, self.completed_steps, self.last_work)

    def _hold(self, reason):
        self._latched = reason
        return self._facts(reason)

    def _matches(self, state):
        matches = (self._launch_identity == state["process_identity"]
                   and state["generation"] == self._initial_generation+1
                   and state["nonce"] is not None)
        if self.fence is not None:
            matches = matches and all(state[k] == getattr(self.fence, k) for k in
                ("domain_id", "binding_digest", "generation", "process_identity", "nonce"))
        return matches

    def _launch(self, now_us, mono):
        # Reject nonserializable host inputs before spawn can partially create
        # a process. This failure cannot consume A1 and must remain held.
        from pickle import dumps
        try:
            dumps((self._configuration, self._external_inputs))
        except Exception:
            return self._hold("HELD_LAUNCH_UNPROVEN")
        launch_id = uuid4().hex
        parent, child = self._context.Pipe()
        process = self._context.Process(target=_runtime_child,
            args=(child, launch_id, self._configuration, self._external_inputs, now_us, self._generation))
        try:
            process.start()
        except Exception:
            parent.close()
            child.close()
            # A partial start can retain a handle; never launch another child.
            self.process = process if process.pid is not None else None
            return self._hold("HELD_LAUNCH_UNPROVEN")
        child.close()
        self.process, self._channel = process, parent
        self._launch_identity = launch_id+":"+str(process.pid)
        self.fence = None
        self._failed, self._waiting = False, True
        self._deadline = mono+self.health.startup_us
        return self._facts("STARTING")

    def poll(self, *, now_us, monotonic_us):
        OperationsStore._time(now_us)
        if type(monotonic_us) is not int or monotonic_us < 0:
            raise ValueError("OPERATIONS_MONOTONIC_CLOCK_REQUIRED")
        if self._last_mono is not None and monotonic_us < self._last_mono:
            return self._hold("HELD_MONOTONIC_REGRESSION")
        self._last_mono = monotonic_us
        if self._latched:
            return self._facts(self._latched)
        # Do not hold SQLite read locks while the timeout=0 A1 acquisition
        # is committing in a starting child. Stop still fences that acquisition;
        # startup health is bounded and durable facts are refreshed at handshake.
        if (self.process is not None and self.fence is None and not self._failed
                and self.process.exitcode is None and monotonic_us < self._deadline):
            try:
                if not self._channel.poll(0):
                    return self._facts("STARTING")
            except (OSError, ValueError):
                pass
        try:
            state = self.store.snapshot()
        except Exception:
            return self._hold("HELD_CONTROL_UNREADABLE")
        if now_us < state["last_control_us"]:
            return self._hold("HELD_CONTROL_CLOCK_REGRESSION")
        if state["stopped"] or state["exhausted"]:
            return self._hold("OPERATOR_STOPPED" if state["stopped"] else "RESTART_EXHAUSTED")
        if self.process is not None:
            code = self.process.exitcode
            if code is not None:
                self.process.join(timeout=0)
                if not self._matches(state):
                    return self._hold("HELD_EXIT_WITHOUT_EXACT_ACQUISITION")
                if code == 0 and not self._failed:
                    return self._hold("HELD_NORMAL_EXIT")
                # Confirmed death and original durable acquisition, even when
                # startup audit failed before the STARTED handshake.
                self._generation = state["generation"]
                self._initial_generation = state["generation"]
                self._channel.close()
                self._channel = None
                self.process.close()
                self.process = None
            else:
                if self._failed:
                    if monotonic_us >= self._deadline:
                        return self._hold("HELD_TERMINATION_UNCONFIRMED")
                    return self._facts("TERMINATING")
                if (self.fence is not None and not self._matches(state)) or (
                        self.fence is None and state["generation"] != self._initial_generation
                        and not self._matches(state)):
                    return self._hold("HELD_OWNER_CHANGED")
                if self._waiting and monotonic_us >= self._deadline:
                    self._failed = True
                    try:
                        self.process.terminate()
                    except OSError:
                        return self._hold("HELD_TERMINATION_UNCONFIRMED")
                    self._deadline = monotonic_us+self.health.termination_us
                    return self._facts("TERMINATING")
                try:
                    if self._waiting and self._channel.poll(0):
                        message, value = self._channel.recv()
                        if self.fence is None:
                            # STARTED may arrive after the poll's first read.
                            try:
                                state = self.store.snapshot()
                            except Exception:
                                return self._hold("HELD_CONTROL_UNREADABLE")
                            if state["stopped"] or state["exhausted"]:
                                return self._hold("OPERATOR_STOPPED" if state["stopped"] else "RESTART_EXHAUSTED")
                            if (message != "STARTED" or type(value) is not OwnerFence
                                    or not self._matches(state)
                                    or any(state[k] != getattr(value, k) for k in
                                           ("domain_id", "binding_digest", "generation", "process_identity", "nonce"))):
                                return self._hold("HELD_CHILD_IDENTITY_CONFLICT")
                            self.fence = value
                        else:
                            if message != "PROGRESS" or type(value) is not str or len(value) > 128:
                                return self._hold("HELD_CHILD_PROTOCOL_CONFLICT")
                            self.completed_steps += 1
                            self.last_work = value
                        self._waiting = False
                    if not self._waiting:
                        self._waiting = True
                        self._deadline = monotonic_us+self.health.progress_us
                        self._channel.send("STEP")
                except (EOFError, OSError, ValueError):
                    # Pipe loss is not proof of death or non-landing. Watch the
                    # same exact handle to the finite deadline before signalling.
                    return self._facts("CHILD_CHANNEL_LOST")
                return self._facts("RUNNING" if self.fence else "STARTING")
        if state["generation"] != self._initial_generation:
            return self._hold("HELD_OWNER_CHANGED")
        if now_us < state["next_attempt_us"]:
            return self._facts("RESTART_BACKOFF")
        # At the attempt limit, this child can only execute original A1's denied
        # acquire, which durably latches exhaustion before cold reconstruction.
        return self._launch(now_us, monotonic_us)

    def operator_stop(self, *, now_us):
        # A busy in-flight mutation raises here; do not claim a committed stop
        # and do not signal/restart before the durable transition succeeds.
        self.store.operator_stop(now_us=now_us)
        self._latched = "OPERATOR_STOPPED"
        if self.process is not None and self.process.exitcode is None:
            self.process.terminate()
        return self._facts(self._latched)

    def run(self, *, max_polls, interval_seconds=0.05):
        """Bounded host driver. Returning does not implicitly stop an owned child."""
        if type(max_polls) is not int or not 1 <= max_polls <= 1000000 or not 0.001 <= interval_seconds <= 1:
            raise ValueError("OPERATIONS_BOUNDED_WATCHDOG_REQUIRED")
        result = None
        for _ in range(max_polls):
            result = self.poll(now_us=time_ns()//1000, monotonic_us=monotonic_ns()//1000)
            if self._latched:
                break
            sleep(interval_seconds)
        return result
