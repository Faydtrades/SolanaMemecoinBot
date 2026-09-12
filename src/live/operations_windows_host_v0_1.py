"""Windows host exclusivity/containment, independent of economic ownership.

The calling process is the actual OperationsSupervisor. Its children inherit
job membership, never the job handle. A fresh host cannot run while a previous
named job contains processes. No PID adoption, process search or reset exists.
"""
from __future__ import annotations

import ctypes
from ctypes import wintypes as w
import os
import re


class HostBoundaryError(RuntimeError):
    pass


class _BasicLimits(ctypes.Structure):
    _fields_ = [('ProcessTime', ctypes.c_longlong), ('JobTime', ctypes.c_longlong),
        ('LimitFlags', w.DWORD), ('MinimumWorkingSetSize', ctypes.c_size_t),
        ('MaximumWorkingSetSize', ctypes.c_size_t), ('ActiveProcessLimit', w.DWORD),
        ('Affinity', ctypes.c_size_t), ('PriorityClass', w.DWORD), ('SchedulingClass', w.DWORD)]


class _IO(ctypes.Structure):
    _fields_ = [(name, ctypes.c_ulonglong) for name in
        ('ReadOperationCount', 'WriteOperationCount', 'OtherOperationCount',
         'ReadTransferCount', 'WriteTransferCount', 'OtherTransferCount')]


class _Limits(ctypes.Structure):
    _fields_ = [('BasicLimitInformation', _BasicLimits), ('IoInfo', _IO),
        ('ProcessMemoryLimit', ctypes.c_size_t), ('JobMemoryLimit', ctypes.c_size_t),
        ('PeakProcessMemoryUsed', ctypes.c_size_t), ('PeakJobMemoryUsed', ctypes.c_size_t)]


class _Accounting(ctypes.Structure):
    _fields_ = [(name, ctypes.c_longlong) for name in
        ('TotalUserTime', 'TotalKernelTime', 'ThisPeriodTotalUserTime', 'ThisPeriodTotalKernelTime')]
    _fields_ += [(name, w.DWORD) for name in
        ('TotalPageFaultCount', 'TotalProcesses', 'ActiveProcesses', 'TotalTerminatedProcesses')]


def kernel():
    if os.name != 'nt':
        raise HostBoundaryError('HOST_WINDOWS_REQUIRED')
    api = ctypes.WinDLL('kernel32', use_last_error=True)
    signatures = {
        'CreateMutexW': (w.HANDLE, [ctypes.c_void_p, w.BOOL, w.LPCWSTR]),
        'WaitForSingleObject': (w.DWORD, [w.HANDLE, w.DWORD]),
        'CreateJobObjectW': (w.HANDLE, [ctypes.c_void_p, w.LPCWSTR]),
        'SetInformationJobObject': (w.BOOL, [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]),
        'QueryInformationJobObject': (w.BOOL, [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.c_void_p]),
        'AssignProcessToJobObject': (w.BOOL, [w.HANDLE, w.HANDLE]),
        'GetCurrentProcess': (w.HANDLE, []),
        'CloseHandle': (w.BOOL, [w.HANDLE]),
        'ReleaseMutex': (w.BOOL, [w.HANDLE]),
    }
    for name, (result, args) in signatures.items():
        method = getattr(api, name)
        method.restype, method.argtypes = result, args
    return api


class WindowsHostBoundary:
    def __init__(self, domain_id):
        if not isinstance(domain_id, str) or not re.fullmatch('[0-9a-f]{64}', domain_id):
            raise HostBoundaryError('HOST_DOMAIN_ID_REQUIRED')
        self.api = kernel()
        self.mutex = self.job = None
        self.locked = self.assigned = False
        self.name = 'Global\\MEME-LIVE-M46-' + domain_id

    def enter(self):
        self.mutex = self.api.CreateMutexW(None, False, self.name + '-mutex')
        if not self.mutex:
            raise HostBoundaryError('HOST_MUTEX_UNAVAILABLE')
        outcome = self.api.WaitForSingleObject(self.mutex, 0)
        if outcome not in (0, 0x80):  # acquired or abandoned; both require job audit
            self.close_unassigned()
            raise HostBoundaryError('HOST_DUPLICATE_DENIED')
        self.locked = True
        try:
            # NULL security attributes => non-inheritable handle. Default child
            # creation inherits membership; no BREAKAWAY limit is enabled.
            self.job = self.api.CreateJobObjectW(None, self.name + '-job')
            if not self.job:
                raise HostBoundaryError('HOST_JOB_UNAVAILABLE')
            accounting = _Accounting()
            if not self.api.QueryInformationJobObject(self.job, 1, ctypes.byref(accounting),
                                                     ctypes.sizeof(accounting), None):
                raise HostBoundaryError('HOST_JOB_AUDIT_FAILED')
            if accounting.ActiveProcesses:
                raise HostBoundaryError('HOST_PRIOR_JOB_NOT_EMPTY')
            limits = _Limits()
            limits.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE
            if not self.api.SetInformationJobObject(self.job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                raise HostBoundaryError('HOST_JOB_LIMIT_FAILED')
            if not self.api.AssignProcessToJobObject(self.job, self.api.GetCurrentProcess()):
                raise HostBoundaryError('HOST_JOB_ASSIGNMENT_FAILED')
            self.assigned = True
            return self
        except Exception:
            self.close_unassigned()
            raise

    def close_unassigned(self):
        # Once assigned, retain both handles until process exit: releasing the
        # mutex before job termination would create a replacement race. Closing
        # the job here would kill this calling process and hide its exit result.
        if self.assigned:
            return
        if self.job:
            self.api.CloseHandle(self.job)
            self.job = None
        if self.mutex:
            if self.locked:
                self.api.ReleaseMutex(self.mutex)
            self.api.CloseHandle(self.mutex)
            self.mutex = None
