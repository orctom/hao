import logging
import math
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Union

import psutil
import pynvml

LOGGER = logging.getLogger(__name__)


@dataclass
class Percent:
    value: Union[float, int]
    formatted: str = field(init=False)
    def __post_init__(self):
        if self.value > 1:
            self.value = self.value / 100
        self.formatted = f"{100 * self.value:.1f}%"

    def __str__(self):
        return self.formatted

    def __repr__(self):
        return self.__str__()


@dataclass
class Cpu:
    count: int
    percent: Percent

    def __str__(self):
        return f"cpu: {self.count} cores ({self.percent})"

    def __repr__(self):
        return self.__str__()


@dataclass
class Bytes:
    value: int
    mega: str = field(init=False)
    human: str = field(init=False)
    def __post_init__(self):
        self.mega = bytes2mega(self.value)
        self.human = bytes2human(self.value)

    def __str__(self):
        return self.human

    def __repr__(self):
        return self.__str__()


@dataclass
class Bits:
    value: int
    mega: str = field(init=False)
    human: str = field(init=False)
    def __post_init__(self):
        _bytes = math.ceil(self.value // 8)
        self.mega = bytes2mega(_bytes)
        self.human = bytes2human(_bytes)

    def __str__(self):
        return self.human

    def __repr__(self):
        return self.__str__()


@dataclass
class Mem:
    used: Bytes
    free: Bytes
    total: Bytes
    percent: Percent = field(init=False, default=None)
    def __post_init__(self):
        self.percent = Percent(round(self.used.value / self.total.value, 3))

    def __str__(self):
        return f"mem: {self.used} / {self.total} ({self.percent})"

    def __repr__(self):
        return self.__str__()


@dataclass
class Process:
    pid: int
    device_id: int
    started: str
    username: str
    name: str
    command: str
    cpu: float
    mem: Bytes
    gpu: Bytes

    def __str__(self):
        username = f"{self.username: <12}" if len(self.username) <= 12 else f"{self.username[:10]}.."
        return (
            f"{self.pid: <8} "
            f"| {username} "
            f"| mem: {str(self.mem): >7} "
            f"| gpu: {str(self.gpu): >8} "
            f"| started: {self.started} "
            f"| {self.command}"
        )

    def __repr__(self):
        return self.__str__()


@dataclass
class GpuDevice:
    i: int
    uuid: str
    bus_id: str
    name: str
    fan_speed: int
    temperature: int
    mem: Mem
    util: Percent
    processes: List[Process]

    def __str__(self):
        processes = ''.join([f"\n    {process}" for process in self.processes])
        return (
            f"[{self.i}] {self.name} ({self.bus_id}), {self.mem}, util: {self.util}, fan: {self.fan_speed}, temp: {self.temperature}℃"
            f"{processes}"
        )

    def __repr__(self):
        return self.__str__()


@dataclass
class Gpu:
    driver_version: str
    cuda_version: str
    devices: List[GpuDevice]

    def __str__(self):
        devices = ''.join([f"\n{device}" for device in self.devices])
        return f"gpu: (driver: {self.driver_version}, cuda: {self.cuda_version}){devices}"

    def __repr__(self):
        return self.__str__()


@dataclass
class Info:
    cpu: Cpu
    mem: Mem
    gpu: Gpu

    def __str__(self):
        return f"{self.cpu}\n{self.mem}\n{self.gpu}"

    def __repr__(self):
        return self.__str__()


def bytes2human(n, format="%(value).2f%(symbol)s"):
    units = ('B', 'k', 'm', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(units[1:]):
        prefix[s] = 1 << (i + 1) * 10
    for symbol in reversed(units[1:]):
        if abs(n) >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format % locals()
    return format % dict(symbol=units[0], value=n)


def bytes2mega(n):
    return f"{n//1024//1024}m"


def get_cpu_info():
    return Cpu(count=psutil.cpu_count(), percent=Percent(psutil.cpu_percent()/100))


def get_mem_info():
    mem = psutil.virtual_memory()
    return Mem(
        used=Bytes(mem.used),
        free=Bytes(mem.available),
        total=Bytes(mem.total),
    )


def get_gpu_info():
    def get_driver_version():
        return pynvml.nvmlSystemGetDriverVersion()  # 545.23.08

    def get_cuda_version():
        version = pynvml.nvmlSystemGetCudaDriverVersion()
        return f"{version // 1000}.{version % 1000 // 10}"

    def get_fan_speed(_handle):
        try:
            return pynvml.nvmlDeviceGetFanSpeed(_handle)
        except pynvml.NVMLError:
            return -1

    def get_mem_info(_handle):
        _mem = pynvml.nvmlDeviceGetMemoryInfo(_handle)
        return Mem(
            used=Bytes(_mem.used),
            free=Bytes(_mem.free),
            total=Bytes(_mem.total),
        )

    def get_processes_on_device(_handle, _device_id: int):
        _processes = []
        for proc in pynvml.nvmlDeviceGetComputeRunningProcesses(_handle):
            try:
                pid = proc.pid
                ps = psutil.Process(pid)
                _processes.append(
                    Process(
                        pid=pid,
                        device_id=_device_id,
                        started=datetime.fromtimestamp(ps.create_time()).strftime('%Y-%m-%d %H:%M:%S'),
                        username=ps.username(),
                        name=ps.name(),
                        command=' '.join(ps.cmdline()),
                        cpu=Percent(ps.cpu_percent()),
                        mem=Bytes(ps.memory_info().rss),
                        gpu=Bytes(proc.usedGpuMemory),
                    )
                )
            except psutil.NoSuchProcess:
                pass
        return _processes

    def get_device_info(_device_id: int):
        handle = pynvml.nvmlDeviceGetHandleByIndex(_device_id)

        return GpuDevice(
            i=_device_id,
            uuid=pynvml.nvmlDeviceGetUUID(handle),
            bus_id=pynvml.nvmlDeviceGetPciInfo(handle).busId,
            name=pynvml.nvmlDeviceGetName(handle),
            fan_speed=get_fan_speed(handle),
            temperature=pynvml.nvmlDeviceGetTemperature(handle, 0),
            mem=get_mem_info(handle),
            util=Percent(pynvml.nvmlDeviceGetUtilizationRates(handle).gpu / 100),
            processes=get_processes_on_device(handle, _device_id),
        )

    pynvml.nvmlInit()

    try:
        device_count = pynvml.nvmlDeviceGetCount()
        if device_count == 0:
            devices = []
        else:
            devices = [get_device_info(device_id) for device_id in range(device_count)]
        return Gpu(
            driver_version=get_driver_version(),
            cuda_version=get_cuda_version(),
            devices=devices,
        )
    finally:
        pynvml.nvmlShutdown()


def get_info():
    return Info(
        cpu=get_cpu_info(),
        mem=get_mem_info(),
        gpu=get_gpu_info(),
    )
