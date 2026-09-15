"""Read-only native premises and volume reserves for the isolated T010 host.

These observations grant no authority. Missing facts, changed binary/VFS or a
short reserve deny ordinary work. No environment variable, SQLite setting,
file, process quota or other process is changed here.
"""
from __future__ import annotations

import ctypes
import hashlib
import os
import sqlite3
import sys
from ctypes import wintypes as w
from contextlib import ExitStack
from decimal import getcontext
from pathlib import Path

from .authority_controls_v0_1 import require
from phase5.shadow_domain_v0_1 import content_fingerprint

SOURCE_ID = "2026-05-05 10:34:17 c88b22011a54b4f6fbd149e9f8e4de77658ce58143a1af0e3785e4e6475127e9"
STORE_NAMES = ("ledger", "producer", "operations", "evidence_store", "monitor")


def _api():
    require(os.name == "nt", "T010_RESOURCE_WINDOWS_REQUIRED")
    return ctypes.WinDLL("kernel32", use_last_error=True)


def _sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def native_identity():
    """Observe the already-loaded SQLite DLL and its actual temporary path.

    The pinned Win32 VFS calls GetTempPathW, not GetTempPath2W. An explicit
    sqlite3_temp_directory override is ineligible for this default-VFS model.
    Loading the already-resident DLL by its verified path adds only a reference.
    """
    import _sqlite3
    api = _api()
    api.GetModuleHandleW.argtypes = [w.LPCWSTR]
    api.GetModuleHandleW.restype = w.HMODULE
    handle = api.GetModuleHandleW("sqlite3.dll")
    require(bool(handle), "T010_RESOURCE_LOADED_SQLITE_DLL_REQUIRED")
    api.GetModuleFileNameW.argtypes = [w.HMODULE, w.LPWSTR, w.DWORD]
    api.GetModuleFileNameW.restype = w.DWORD
    name = ctypes.create_unicode_buffer(32768)
    count = api.GetModuleFileNameW(handle, name, len(name))
    require(0 < count < len(name), "T010_RESOURCE_SQLITE_DLL_PATH_UNAVAILABLE")
    library = ctypes.CDLL(name.value)
    require(ctypes.c_void_p.in_dll(library, "sqlite3_temp_directory").value is None,
        "T010_RESOURCE_SQLITE_TEMP_OVERRIDE_DENIED")
    library.sqlite3_sourceid.restype = ctypes.c_char_p
    require(library.sqlite3_sourceid().decode("ascii") == SOURCE_ID
        and sqlite3.sqlite_version == "3.53.1", "T010_RESOURCE_SQLITE_ENGINE_CONFLICT")
    class Vfs(ctypes.Structure):
        _fields_ = [("version", ctypes.c_int), ("file_size", ctypes.c_int),
            ("max_path", ctypes.c_int), ("next", ctypes.c_void_p), ("name", ctypes.c_char_p)]
    library.sqlite3_vfs_find.argtypes = [ctypes.c_char_p]
    library.sqlite3_vfs_find.restype = ctypes.POINTER(Vfs)
    vfs = library.sqlite3_vfs_find(None)
    require(bool(vfs) and vfs.contents.name == b"win32", "T010_RESOURCE_DEFAULT_WIN32_VFS_REQUIRED")
    api.GetTempPathW.argtypes = [w.DWORD, w.LPWSTR]
    api.GetTempPathW.restype = w.DWORD
    temporary = ctypes.create_unicode_buffer(32768)
    count = api.GetTempPathW(len(temporary), temporary)
    require(0 < count < len(temporary), "T010_RESOURCE_SQLITE_TEMP_PATH_UNAVAILABLE")
    require(sys.get_int_max_str_digits() == 4300 and getcontext().prec == 60,
        "T010_RESOURCE_SERIALIZATION_PREMISE_CONFLICT")
    return {"sqlite_source_id": SOURCE_ID, "sqlite_version": sqlite3.sqlite_version,
        "sqlite_dll_sha256": _sha(name.value), "sqlite_extension_sha256": _sha(_sqlite3.__file__),
        "python_sha256": _sha(sys.executable), "python_version": sys.version,
        "vfs": "win32", "temporary_directory": str(Path(temporary.value).resolve()),
        "temporary_path_api": "GetTempPathW", "integer_digits": 4300, "decimal_precision": 60}


def volume_observation(path):
    """Bind a local existing directory to its volume GUID and allocation unit.

    free_bytes is bytes available to this caller (including any filesystem
    quota), rather than an unqualified volume-wide free-space number.
    """
    path = Path(path).resolve(strict=True)
    require(path.is_dir() and not str(path).startswith("\\\\"), "T010_RESOURCE_LOCAL_DIRECTORY_REQUIRED")
    api = _api()
    api.GetVolumePathNameW.argtypes = [w.LPCWSTR, w.LPWSTR, w.DWORD]
    api.GetVolumePathNameW.restype = w.BOOL
    mount = ctypes.create_unicode_buffer(32768)
    require(api.GetVolumePathNameW(str(path), mount, len(mount)), "T010_RESOURCE_VOLUME_PATH_UNAVAILABLE")
    api.GetVolumeNameForVolumeMountPointW.argtypes = [w.LPCWSTR, w.LPWSTR, w.DWORD]
    api.GetVolumeNameForVolumeMountPointW.restype = w.BOOL
    identity = ctypes.create_unicode_buffer(128)
    require(api.GetVolumeNameForVolumeMountPointW(mount.value, identity, len(identity)),
        "T010_RESOURCE_VOLUME_IDENTITY_UNAVAILABLE")
    api.GetDiskFreeSpaceW.argtypes = [w.LPCWSTR] + [ctypes.POINTER(w.DWORD)]*4
    api.GetDiskFreeSpaceW.restype = w.BOOL
    sectors, size, free_clusters, clusters = (w.DWORD() for _ in range(4))
    require(api.GetDiskFreeSpaceW(mount.value, ctypes.byref(sectors), ctypes.byref(size),
        ctypes.byref(free_clusters), ctypes.byref(clusters)), "T010_RESOURCE_ALLOCATION_UNIT_UNAVAILABLE")
    api.GetDiskFreeSpaceExW.argtypes = [w.LPCWSTR] + [ctypes.POINTER(ctypes.c_ulonglong)]*3
    api.GetDiskFreeSpaceExW.restype = w.BOOL
    available, total, free = (ctypes.c_ulonglong() for _ in range(3))
    require(api.GetDiskFreeSpaceExW(str(path), ctypes.byref(available), ctypes.byref(total), ctypes.byref(free)),
        "T010_RESOURCE_VOLUME_RESERVE_UNAVAILABLE")
    return {"volume_id": identity.value, "allocation_unit_bytes": sectors.value*size.value,
        "free_bytes": available.value, "total_bytes": total.value}


def volume_requirements(bindings, owned_peak, recovery_bytes, temporary_peak, reserve_floor, immutable_bytes):
    """Sum simultaneous claims when stores and temporary files share a volume.

    The existing reserve floor is preserved independently on each volume. An
    independent owned-file copy is explicit recovery capacity, never permission
    to copy or rewrite retained history.
    """
    require(type(bindings) is dict and set(bindings) == {"owned", "temporary"},
        "T010_RESOURCE_TWO_VOLUME_BINDING_REQUIRED")
    require(all(type(n) is int and n > 0 for n in (owned_peak, recovery_bytes, temporary_peak, reserve_floor))
        and type(immutable_bytes) is int and immutable_bytes >= 0, "T010_RESOURCE_FINITE_DISK_MODEL_REQUIRED")
    result = {}
    for role, amount in (("owned", owned_peak+recovery_bytes+immutable_bytes), ("temporary", temporary_peak)):
        item = bindings[role]
        require(type(item) is dict and set(item) == {"path", "volume_id", "allocation_unit_bytes"}
            and type(item["volume_id"]) is str and bool(item["volume_id"])
            and item["allocation_unit_bytes"] == 4096, "T010_RESOURCE_EXACT_VOLUME_BINDING_REQUIRED")
        result[item["volume_id"]] = result.get(item["volume_id"], reserve_floor)+amount
    return result


def check_volume_reserves(bindings, requirements, *, observe=volume_observation):
    observations = {}
    for role in ("owned", "temporary"):
        expected = bindings[role]
        actual = observe(expected["path"])
        require(all(actual[k] == expected[k] for k in ("volume_id", "allocation_unit_bytes")),
            "T010_RESOURCE_VOLUME_BINDING_CHANGED")
        require(type(actual["free_bytes"]) is int and actual["free_bytes"] >= requirements[expected["volume_id"]],
            "T010_RESOURCE_VOLUME_RESERVE_INSUFFICIENT")
        observations[role] = actual
    return observations


def owned_connection_facts(runtime):
    """Inspect original owners' connections without a checkpoint or mutation."""
    require(runtime.capability == "NO_BROADCAST", "T010_RESOURCE_DRY_ONLY")
    with ExitStack() as stack:
        owners = {"ledger": runtime.ledger._conn, "producer": runtime.producer.conn,
            "evidence_store": runtime.source._conn,
            "operations": stack.enter_context(runtime._ownership._store._connection()),
            "monitor": stack.enter_context(runtime._operations_degradation.store._connection())}
        return _connection_facts(owners)


def _connection_facts(owners):
    result = {}
    for name, connection in owners.items():
        expected = {"journal_mode": "delete" if name in ("operations", "monitor") else "wal",
            "page_size": 4096, "auto_vacuum": 0, "encoding": "UTF-8", "threads": 0,
            "cache_size": -2000, "temp_store": 0, "locking_mode": "normal"}
        actual = {key: connection.execute("PRAGMA "+key).fetchone()[0] for key in expected}
        require(actual == expected and connection.execute("SELECT sqlite_source_id()").fetchone()[0] == SOURCE_ID,
            "T010_RESOURCE_CONNECTION_PREMISE_CONFLICT")
        databases = list(connection.execute("PRAGMA database_list"))
        # Original schema/introspection reads can initialize SQLite's empty
        # built-in temp schema. It is not an attached database. The reviewed
        # temp-VFS proof covers internal sort/replay work, not named temp
        # tables or an independently attached database.
        require(databases and databases[0][0] == 0 and databases[0][1] == "main"
            and (len(databases) == 1 or len(databases) == 2
                and tuple(databases[1]) == (1, "temp", "")), "T010_RESOURCE_ATTACHED_DATABASE_DENIED")
        if len(databases) == 2:
            require(connection.execute("SELECT 1 FROM temp.sqlite_schema LIMIT 1").fetchone() is None,
                "T010_RESOURCE_NAMED_TEMP_SCHEMA_DENIED")
        path = Path(databases[0][2]).resolve(strict=True)
        with path.open("rb") as stream:
            header = stream.read(100)
        require(len(header) == 100 and header[:16] == b"SQLite format 3\x00" and header[20] == 0,
            "T010_RESOURCE_RESERVED_PAGE_BYTES_CONFLICT")
        rows = [tuple(row) for row in connection.execute(
            "SELECT type,name,tbl_name,rootpage,sql FROM sqlite_schema ORDER BY name")]
        require(not any(row[1].startswith("sqlite_stat") for row in rows), "T010_RESOURCE_SQLITE_STATISTICS_DENIED")
        options = sorted(row[0] for row in connection.execute("PRAGMA compile_options"))
        # pagerWriteLargeSector dirties sibling pages when sector > page. The
        # reviewed dirty-page derivation requires this branch to be ineligible;
        # conservative journal-header padding is not a substitute for this pin.
        require("DEFAULT_SECTOR_SIZE=4096" in options and "TEMP_STORE=1" in options
            and "DEFAULT_CACHE_SIZE=-2000" in options and "DEFAULT_WORKER_THREADS=0" in options,
            "T010_RESOURCE_SQLITE_COMPILE_PREMISE_CONFLICT")
        result[name] = {"path": str(path), "pragmas": actual, "schema_digest": content_fingerprint(rows),
            "compile_options_digest": content_fingerprint(options)}
    return result


def constructor_input_references(inputs):
    """Existing external constructor records; exclude the binding's own DAG.

    Resource certificates and the resulting profile/dossier are immutable
    inputs by launch, never host-created growth. Their occupied space is
    already subtracted from the fresh native free-space observations.
    """
    from .runtime_dry_profile_v0_1 import read_reference
    references = [inputs[key] for key in ("accepted_profile", "accepted_extension", "accepted_monitor",
        "deployment_rebind", "public_rpc_rebind") if inputs.get(key) is not None]
    target = read_reference(inputs["accepted_profile"])["target"]
    references.append({"path": target["public_configuration_path"], "sha256": target["public_configuration_sha256"]})
    if inputs.get("public_rpc_rebind") is not None:
        references.append(read_reference(inputs["public_rpc_rebind"])["source_evidence"])
    return references


def existing_input_files(references):
    """Read-only allocation facts for already existing, hash-bound JSON inputs."""
    result = {}
    for reference in references:
        require(type(reference) is dict and set(reference) == {"path", "sha256"},
            "T010_RESOURCE_IMMUTABLE_REFERENCE_REQUIRED")
        path = Path(reference["path"]).resolve(strict=True)
        require(path.is_file() and path.suffix.lower() == ".json" and path.stat().st_size <= 16777216
            and _sha(path) == reference["sha256"], "T010_RESOURCE_IMMUTABLE_INPUT_CHANGED")
        volume = volume_observation(path.parent)
        size = path.stat().st_size
        unit = volume["allocation_unit_bytes"]
        value = {"reference": reference, "logical_bytes": size,
            "allocation_upper_bound_bytes": ((size+unit-1)//unit)*unit,
            "volume_id": volume["volume_id"], "allocation_unit_bytes": unit}
        require(str(path) not in result or result[str(path)] == value,
            "T010_RESOURCE_IMMUTABLE_REFERENCE_CONFLICT")
        result[str(path)] = value
    return [result[key] for key in sorted(result)]


def validate_binding(binding, *, store_paths=None, runtime=None, check_reserve=True, immutable_references=None):
    require(type(binding) is dict and set(binding) == {"schema", "native", "connections", "volumes",
        "owned_peak_bytes", "recovery_bytes", "temporary_peak_bytes", "reserve_floor_bytes",
        "immutable_input_bytes", "immutable_inputs", "requirements"}, "T010_RESOURCE_ENVIRONMENT_BINDING_REQUIRED")
    require(binding["schema"] == "MEME_LIVE_T010_RESOURCE_ENVIRONMENT_V1"
        and binding["native"] == native_identity(), "T010_RESOURCE_NATIVE_BINDING_CHANGED")
    # No future copies of immutable inputs are made by the host. Existing
    # occupied bytes are reflected in free space; do not accept a caller-chosen
    # scalar as evidence of their allocation or charge them to a wrong volume.
    require(type(binding["immutable_input_bytes"]) is int and binding["immutable_input_bytes"] == 0
        and type(binding["immutable_inputs"]) is list, "T010_RESOURCE_IMMUTABLE_GROWTH_DENIED")
    references = immutable_references if immutable_references is not None else [
        item["reference"] for item in binding["immutable_inputs"]]
    require(binding["immutable_inputs"] == existing_input_files(references),
        "T010_RESOURCE_IMMUTABLE_ALLOCATION_BINDING_CONFLICT")
    require(type(binding["connections"]) is dict and set(binding["connections"]) == set(STORE_NAMES),
        "T010_RESOURCE_FIVE_CONNECTION_BINDING_REQUIRED")
    expected_paths = {name: fact["path"] for name, fact in binding["connections"].items()}
    if store_paths is not None:
        require(expected_paths == {name: str(Path(path).resolve()) for name, path in store_paths.items()},
            "T010_RESOURCE_EXACT_STORE_BINDING_REQUIRED")
    owned = binding["volumes"]["owned"]
    temporary = binding["volumes"]["temporary"]
    require(str(Path(temporary["path"]).resolve()) == binding["native"]["temporary_directory"],
        "T010_RESOURCE_SQLITE_TEMP_BINDING_CHANGED")
    for path in expected_paths.values():
        actual = volume_observation(Path(path).parent)
        require(all(actual[k] == owned[k] for k in ("volume_id", "allocation_unit_bytes")),
            "T010_RESOURCE_OWNED_VOLUME_CHANGED")
    if runtime is not None:
        require(owned_connection_facts(runtime) == binding["connections"],
            "T010_RESOURCE_OWNED_CONNECTIONS_CHANGED")
    calculated = volume_requirements(binding["volumes"], binding["owned_peak_bytes"],
        binding["recovery_bytes"], binding["temporary_peak_bytes"], binding["reserve_floor_bytes"],
        binding["immutable_input_bytes"])
    require(calculated == binding["requirements"], "T010_RESOURCE_VOLUME_REQUIREMENTS_CONFLICT")
    if check_reserve:
        return check_volume_reserves(binding["volumes"], calculated)
    return {}


def memory_observation():
    """Current system physical/commit availability, without changing paging."""
    class Performance(ctypes.Structure):
        _fields_ = [("size", w.DWORD)] + [(key, ctypes.c_size_t) for key in (
            "commit_total", "commit_limit", "commit_peak", "physical_total", "physical_available",
            "system_cache", "kernel_total", "kernel_paged", "kernel_nonpaged", "page_bytes")]
        _fields_ += [(key, w.DWORD) for key in ("handles", "processes", "threads")]
    _api()
    api = ctypes.WinDLL("psapi", use_last_error=True)
    api.GetPerformanceInfo.argtypes = [ctypes.POINTER(Performance), w.DWORD]
    api.GetPerformanceInfo.restype = w.BOOL
    value = Performance(); value.size = ctypes.sizeof(value)
    require(api.GetPerformanceInfo(ctypes.byref(value), value.size), "T010_RESOURCE_HOST_MEMORY_UNAVAILABLE")
    return {"physical_available_bytes": value.physical_available*value.page_bytes,
        "commit_available_bytes": (value.commit_limit-value.commit_total)*value.page_bytes,
        "physical_total_bytes": value.physical_total*value.page_bytes,
        "commit_limit_bytes": value.commit_limit*value.page_bytes, "page_bytes": value.page_bytes}


def check_memory_reserve(guards, *, observe=memory_observation):
    # One full additional measured process tree remains available for recovery,
    # independently of the OS/collector/other processes already occupying RAM.
    physical = guards["HOST_RSS_BYTES"]+guards["supervisor_rss_bytes"]
    committed = guards["child_private_bytes"]+guards["supervisor_private_bytes"]
    actual = observe()
    require(type(actual["physical_available_bytes"]) is int and type(actual["commit_available_bytes"]) is int
        and actual["physical_available_bytes"] >= physical and actual["commit_available_bytes"] >= committed,
        "T010_RESOURCE_HOST_RECOVERY_MEMORY_INSUFFICIENT")
    return actual
