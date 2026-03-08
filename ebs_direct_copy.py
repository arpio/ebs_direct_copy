"""
Cross-region EBS snapshot, AMI, and EC2 instance copy using the EBS Direct APIs.

Supports copying one or more snapshots, AMIs, or EC2 instances in parallel.

Requirements:
    pip install boto3

IAM permissions needed:
    Source:      ebs:ListSnapshotBlocks, ebs:GetSnapshotBlock
                 ec2:DescribeSnapshots, ec2:DescribeImages, ec2:DescribeInstances
                 ec2:CreateImage (for instance copies)
    Destination: ebs:StartSnapshot, ebs:PutSnapshotBlock, ebs:CompleteSnapshot
                 ec2:DescribeSnapshots, ec2:DeleteSnapshot, ec2:RegisterImage
"""

from typing import Optional, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
import hashlib
import base64
import sys
import io
import signal
import time
import argparse
import logging
import threading
import uuid
import shutil
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

# Number of concurrent block-copy workers shared across all concurrent copies
WORKERS = 32
# Boto3 client timeouts (seconds)
CLIENT_CONFIG = Config(
    connect_timeout=10,
    read_timeout=60,
    retries={"max_attempts": 5, "mode": "adaptive"},
    max_pool_connections=WORKERS,
)
# Per-block retry settings for unreliable regions
BLOCK_MAX_RETRIES = 5
BLOCK_RETRY_BASE_DELAY = 2  # seconds; doubles each attempt
# EBS Direct API block size is always 512 KiB
BLOCK_SIZE = 512 * 1024

# AWS error codes that indicate a permanent failure — never retry these
PERMANENT_ERROR_CODES = {
    "InvalidInstanceID.NotFound",
    "InvalidInstanceID.Malformed",
    "InvalidAMIID.NotFound",
    "InvalidAMIID.Malformed",
    "InvalidAMIName.Duplicate",
    "InvalidSnapshotID.NotFound",
    "InvalidSnapshotID.Malformed",
    "ResourceNotFoundException",
    "InvalidParameterValue",
    "InvalidParameter",
    "MissingParameter",
    "UnauthorizedOperation",
    "AuthFailure",
}

# Global stop event — set on Ctrl-C to signal all threads to exit cleanly
_stop = threading.Event()

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stderr capture — prevents botocore tracebacks from corrupting the display
# ---------------------------------------------------------------------------

class _StderrCapture(io.TextIOBase):
    """Redirect stderr to an in-memory buffer so it doesn't corrupt the display."""
    def __init__(self):
        self._lock = threading.Lock()
        self._lines: list[str] = []

    def write(self, s: str) -> int:
        if s.strip():
            with self._lock:
                self._lines.append(s)
        return len(s)

    def flush(self):
        pass

    def drain(self) -> str:
        """Return and clear all captured output."""
        with self._lock:
            out = "".join(self._lines)
            self._lines.clear()
        return out


_stderr_capture = _StderrCapture()


# ---------------------------------------------------------------------------
# Status dataclasses
# ---------------------------------------------------------------------------

class CopyStage(Enum):
    FETCHING_METADATA   = auto()
    LISTING_BLOCKS      = auto()
    STARTING_SNAPSHOT   = auto()
    COPYING_BLOCKS      = auto()
    COMPLETING_SNAPSHOT = auto()
    DONE                = auto()


@dataclass
class CopyStatus:
    stage: CopyStage
    src_snapshot_id: str
    dst_snapshot_id: Optional[str] = None
    total_blocks: int = 0
    completed_blocks: int = 0
    volume_size_gib: int = 0
    error: Optional[str] = None

    @property
    def progress_pct(self) -> float:
        if self.total_blocks == 0:
            return 0.0
        return self.completed_blocks / self.total_blocks * 100


StatusCallback = Callable[[CopyStatus], None]


@dataclass
class AmiCopyStatus:
    src_ami_id: str
    dst_ami_id: Optional[str] = None
    snapshot_statuses: dict = field(default_factory=dict)
    stage: str = "copying_snapshots"
    error: Optional[str] = None

    @property
    def total_blocks(self) -> int:
        return sum(s.total_blocks for s in self.snapshot_statuses.values())

    @property
    def completed_blocks(self) -> int:
        return sum(s.completed_blocks for s in self.snapshot_statuses.values())

    @property
    def progress_pct(self) -> float:
        t = self.total_blocks
        return self.completed_blocks / t * 100 if t else 0.0


AmiStatusCallback = Callable[[AmiCopyStatus], None]


def _null_callback(status) -> None:
    pass


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _is_permanent_error(e: Exception) -> bool:
    """Return True if this AWS error should never be retried."""
    if isinstance(e, ClientError):
        code = e.response.get("Error", {}).get("Code", "")
        return code in PERMANENT_ERROR_CODES
    return False


def _retry_on_error(fn, *args, label="operation", **kwargs):
    """
    Call fn(*args, **kwargs), retrying transient errors up to BLOCK_MAX_RETRIES
    times with exponential backoff. Permanent AWS errors are raised immediately.
    Also stops immediately if the global stop event is set.
    """
    delay = BLOCK_RETRY_BASE_DELAY
    for attempt in range(1, BLOCK_MAX_RETRIES + 1):
        if _stop.is_set():
            raise InterruptedError("Cancelled")
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if _is_permanent_error(e) or attempt == BLOCK_MAX_RETRIES:
                log.warning(f"  {label} failed: {e}")
                raise
            log.warning(f"  {label} attempt {attempt} failed ({e}), retrying in {delay}s...")
            # Use stop event as an interruptible sleep
            _stop.wait(delay)
            delay *= 2


def _get_snapshot_metadata(ec2_client, snapshot_id: str) -> dict:
    resp = _retry_on_error(
        ec2_client.describe_snapshots,
        SnapshotIds=[snapshot_id],
        label=f"DescribeSnapshots({snapshot_id})",
    )
    snaps = resp["Snapshots"]
    if not snaps:
        raise ValueError(f"Snapshot {snapshot_id} not found")
    snap = snaps[0]
    if snap["State"] != "completed":
        raise ValueError(f"Snapshot {snapshot_id} is in state '{snap['State']}', must be 'completed'")
    return snap


def _list_all_blocks(ebs_client, snapshot_id: str) -> dict:
    blocks = {}
    paginator_kwargs = {"SnapshotId": snapshot_id}
    next_token = None
    while True:
        if _stop.is_set():
            raise InterruptedError("Cancelled")
        if next_token:
            paginator_kwargs["NextToken"] = next_token
        resp = _retry_on_error(
            ebs_client.list_snapshot_blocks,
            **paginator_kwargs,
            label=f"ListSnapshotBlocks(token={next_token})",
        )
        for b in resp.get("Blocks", []):
            blocks[b["BlockIndex"]] = b["BlockToken"]
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return blocks


def _get_block(ebs_src, snapshot_id: str, block_index: int, block_token: str) -> tuple:
    resp = ebs_src.get_snapshot_block(
        SnapshotId=snapshot_id,
        BlockIndex=block_index,
        BlockToken=block_token,
    )
    return block_index, resp["BlockData"].read()


def _put_block(ebs_dst, dest_snapshot_id: str, block_index: int, data: bytes) -> int:
    checksum = base64.b64encode(hashlib.sha256(data).digest()).decode("utf-8")
    ebs_dst.put_snapshot_block(
        SnapshotId=dest_snapshot_id,
        BlockIndex=block_index,
        BlockData=data,
        DataLength=len(data),
        Checksum=checksum,
        ChecksumAlgorithm="SHA256",
    )
    return block_index


# ---------------------------------------------------------------------------
# Shared boto3 clients
# ---------------------------------------------------------------------------

@dataclass
class ClientSet:
    ec2_src: object
    ebs_src: object
    ebs_dst: object
    ec2_dst: object

    @classmethod
    def create(
        cls,
        src_region: str,
        dst_region: str,
        workers: int,
        src_profile: Optional[str] = None,
        dst_profile: Optional[str] = None,
    ) -> "ClientSet":
        config = CLIENT_CONFIG.merge(Config(max_pool_connections=workers))
        src_session = boto3.Session(profile_name=src_profile, region_name=src_region)
        dst_session = boto3.Session(profile_name=dst_profile, region_name=dst_region)
        return cls(
            ec2_src=src_session.client("ec2", config=config),
            ebs_src=src_session.client("ebs", config=config),
            ebs_dst=dst_session.client("ebs", config=config),
            ec2_dst=dst_session.client("ec2", config=config),
        )


# ---------------------------------------------------------------------------
# Public API: copy_snapshot
# ---------------------------------------------------------------------------

def copy_snapshot(
    src_snapshot_id: str,
    clients: ClientSet,
    src_region: str,
    description: Optional[str] = None,
    kms_key_id: Optional[str] = None,
    executor: Optional[ThreadPoolExecutor] = None,
    on_status: StatusCallback = _null_callback,
) -> str:
    status = CopyStatus(stage=CopyStage.FETCHING_METADATA, src_snapshot_id=src_snapshot_id)

    on_status(status)
    meta = _get_snapshot_metadata(clients.ec2_src, src_snapshot_id)
    status.volume_size_gib = meta["VolumeSize"]

    status.stage = CopyStage.LISTING_BLOCKS
    on_status(status)
    src_blocks = _list_all_blocks(clients.ebs_src, src_snapshot_id)
    status.total_blocks = len(src_blocks)

    status.stage = CopyStage.STARTING_SNAPSHOT
    on_status(status)
    start_kwargs = {
        "VolumeSize": status.volume_size_gib,
        "Description": description or f"Copy of {src_snapshot_id} from {src_region}",
        # FIX: use a random suffix instead of a timestamp to avoid ClientToken
        # collisions when the same snapshot is copied more than once concurrently
        # (e.g. as a standalone snap- arg AND as part of an AMI's block devices).
        "ClientToken": f"copy-{src_snapshot_id}-{uuid.uuid4().hex[:12]}",
    }
    if kms_key_id:
        start_kwargs["Encrypted"] = True
        start_kwargs["KmsKeyArn"] = kms_key_id
    elif meta.get("Encrypted"):
        start_kwargs["Encrypted"] = True

    dest_snapshot_id = clients.ebs_dst.start_snapshot(**start_kwargs)["SnapshotId"]
    status.dst_snapshot_id = dest_snapshot_id

    status.stage = CopyStage.COPYING_BLOCKS
    on_status(status)

    block_digests = {}
    errors = []
    lock = threading.Lock()

    def transfer_block(idx, token):
        if _stop.is_set():
            raise InterruptedError("Cancelled")
        _, data = _retry_on_error(
            _get_block, clients.ebs_src, src_snapshot_id, idx, token,
            label=f"GetSnapshotBlock(index={idx})",
        )
        digest = hashlib.sha256(data).digest()
        _retry_on_error(
            _put_block, clients.ebs_dst, dest_snapshot_id, idx, data,
            label=f"PutSnapshotBlock(index={idx})",
        )
        return idx, digest

    own_executor = executor is None
    pool = ThreadPoolExecutor(max_workers=WORKERS) if own_executor else executor

    try:
        futures = {
            pool.submit(transfer_block, idx, token): idx
            for idx, token in src_blocks.items()
        }
        for f in as_completed(futures):
            if _stop.is_set():
                break
            try:
                idx, digest = f.result()
                block_digests[idx] = digest
                with lock:
                    status.completed_blocks += 1
                on_status(status)
            except Exception as e:
                errors.append((futures[f], str(e)))
    finally:
        if own_executor:
            pool.shutdown(wait=False)

    if _stop.is_set():
        clients.ec2_dst.delete_snapshot(SnapshotId=dest_snapshot_id)
        raise InterruptedError("Cancelled — destination snapshot deleted")

    if errors:
        clients.ec2_dst.delete_snapshot(SnapshotId=dest_snapshot_id)
        msg = (
            f"Copy aborted: {len(errors)} block(s) failed after all retries. "
            f"Destination snapshot {dest_snapshot_id} has been deleted."
        )
        status.error = msg
        on_status(status)
        raise RuntimeError(msg)

    status.stage = CopyStage.COMPLETING_SNAPSHOT
    on_status(status)

    agg = hashlib.sha256()
    for idx in sorted(block_digests.keys()):
        agg.update(block_digests[idx])

    clients.ebs_dst.complete_snapshot(
        SnapshotId=dest_snapshot_id,
        ChangedBlocksCount=status.total_blocks,
        Checksum=base64.b64encode(agg.digest()).decode("utf-8"),
        ChecksumAlgorithm="SHA256",
        ChecksumAggregationMethod="LINEAR",
    )

    status.stage = CopyStage.DONE
    on_status(status)

    return dest_snapshot_id


# ---------------------------------------------------------------------------
# AMI helpers
# ---------------------------------------------------------------------------

def _get_ami_metadata(ec2_client, ami_id: str) -> dict:
    resp = _retry_on_error(
        ec2_client.describe_images,
        ImageIds=[ami_id],
        label=f"DescribeImages({ami_id})",
    )
    images = resp.get("Images", [])
    if not images:
        raise ValueError(f"AMI {ami_id} not found")
    return images[0]


def _build_register_kwargs(src_image: dict, snap_id_map: dict) -> dict:
    bdms = []
    for bdm in src_image.get("BlockDeviceMappings", []):
        new_bdm = {"DeviceName": bdm["DeviceName"]}
        if "Ebs" in bdm:
            ebs = {k: v for k, v in bdm["Ebs"].items()
                   if k in ("VolumeSize", "VolumeType", "DeleteOnTermination",
                             "Iops", "Throughput")}
            src_snap = bdm["Ebs"].get("SnapshotId")
            if src_snap and src_snap in snap_id_map:
                ebs["SnapshotId"] = snap_id_map[src_snap]
            else:
                if "Encrypted" in bdm["Ebs"]:
                    ebs["Encrypted"] = bdm["Ebs"]["Encrypted"]
            new_bdm["Ebs"] = ebs
        elif "VirtualName" in bdm:
            new_bdm["VirtualName"] = bdm["VirtualName"]
        elif "NoDevice" in bdm:
            new_bdm["NoDevice"] = bdm["NoDevice"]
        bdms.append(new_bdm)

    kwargs = {
        "Name":               src_image["Name"],
        "Architecture":       src_image["Architecture"],
        "RootDeviceName":     src_image["RootDeviceName"],
        "VirtualizationType": src_image["VirtualizationType"],
        "BlockDeviceMappings": bdms,
    }
    for attr, key in [
        ("Description",     "Description"),
        ("EnaSupport",      "EnaSupport"),
        ("SriovNetSupport", "SriovNetSupport"),
        ("BootMode",        "BootMode"),
        ("TpmSupport",      "TpmSupport"),
        ("ImdsSupport",     "ImdsSupport"),
    ]:
        if key in src_image:
            kwargs[attr] = src_image[key]
    if src_image.get("KernelId"):
        kwargs["KernelId"] = src_image["KernelId"]
    if src_image.get("RamdiskId"):
        kwargs["RamdiskId"] = src_image["RamdiskId"]
    return kwargs


def _wait_for_snapshots(ec2_client, snapshot_ids: list, poll_interval: int = 10) -> None:
    pending = set(snapshot_ids)
    while pending and not _stop.is_set():
        resp = _retry_on_error(
            ec2_client.describe_snapshots,
            SnapshotIds=list(pending),
            label="DescribeSnapshots(waiting for completion)",
        )
        for snap in resp["Snapshots"]:
            if snap["State"] == "completed":
                pending.discard(snap["SnapshotId"])
            elif snap["State"] == "error":
                raise RuntimeError(
                    f"Snapshot {snap['SnapshotId']} entered error state: "
                    f"{snap.get('StateMessage', 'unknown')}"
                )
        if pending:
            _stop.wait(poll_interval)


def _create_ami_from_instance(
    ec2_client,
    instance_id: str,
    name: Optional[str],
    description: Optional[str],
    on_wait: Optional[Callable[[str], None]] = None,
) -> str:
    resp = _retry_on_error(
        ec2_client.describe_instances,
        InstanceIds=[instance_id],
        label=f"DescribeInstances({instance_id})",
    )
    if not resp.get("Reservations"):
        raise ValueError(f"Instance {instance_id} not found")

    ami_name = name or f"copy-{instance_id}-{int(time.time())}"
    ami_desc = description or f"Created from {instance_id}"

    resp = _retry_on_error(
        ec2_client.create_image,
        InstanceId=instance_id,
        Name=ami_name,
        Description=ami_desc,
        NoReboot=True,
        label=f"CreateImage({instance_id})",
    )
    ami_id = resp["ImageId"]

    while not _stop.is_set():
        images = _retry_on_error(
            ec2_client.describe_images,
            ImageIds=[ami_id],
            label=f"DescribeImages({ami_id})",
        ).get("Images", [])
        if not images:
            raise RuntimeError(f"AMI {ami_id} disappeared while waiting")
        state = images[0]["State"]
        if state == "available":
            return ami_id
        if state in ("failed", "error", "deregistered", "invalid"):
            raise RuntimeError(f"AMI {ami_id} entered state '{state}'")
        if on_wait:
            on_wait(f"Waiting for {ami_id} to become available ({state})...")
        _stop.wait(15)

    raise InterruptedError("Cancelled")


# ---------------------------------------------------------------------------
# Public API: copy_ami
# ---------------------------------------------------------------------------

def copy_ami(
    src_ami_id: str,
    clients: ClientSet,
    src_region: str,
    name_override: Optional[str] = None,
    description_override: Optional[str] = None,
    kms_key_id: Optional[str] = None,
    executor: Optional[ThreadPoolExecutor] = None,
    on_status: AmiStatusCallback = _null_callback,
) -> str:
    src_image = _get_ami_metadata(clients.ec2_src, src_ami_id)

    src_snapshot_ids = [
        bdm["Ebs"]["SnapshotId"]
        for bdm in src_image.get("BlockDeviceMappings", [])
        if "Ebs" in bdm and "SnapshotId" in bdm["Ebs"]
    ]
    if not src_snapshot_ids:
        raise ValueError(f"AMI {src_ami_id} has no EBS snapshots to copy")

    ami_status = AmiCopyStatus(src_ami_id=src_ami_id)
    snap_id_map = {}
    lock = threading.Lock()

    def snapshot_callback(snap_status: CopyStatus):
        with lock:
            ami_status.snapshot_statuses[snap_status.src_snapshot_id] = snap_status
        on_status(ami_status)

    with ThreadPoolExecutor(max_workers=len(src_snapshot_ids)) as ami_pool:
        futures = {
            ami_pool.submit(
                copy_snapshot,
                src_snapshot_id=snap_id,
                clients=clients,
                src_region=src_region,
                description=f"Copied from {snap_id} ({src_ami_id}) in {src_region}",
                kms_key_id=kms_key_id,
                executor=executor,
                on_status=snapshot_callback,
            ): snap_id
            for snap_id in src_snapshot_ids
        }
        for f in as_completed(futures):
            snap_id = futures[f]
            try:
                snap_id_map[snap_id] = f.result()
            except Exception as e:
                msg = f"Failed to copy snapshot {snap_id}: {e}"
                ami_status.error = msg
                on_status(ami_status)
                raise RuntimeError(msg)

    if _stop.is_set():
        raise InterruptedError("Cancelled")

    ami_status.stage = "waiting_for_snapshots"
    on_status(ami_status)
    _wait_for_snapshots(clients.ec2_dst, list(snap_id_map.values()))

    if _stop.is_set():
        raise InterruptedError("Cancelled")

    ami_status.stage = "registering"
    on_status(ami_status)

    register_kwargs = _build_register_kwargs(src_image, snap_id_map)
    if name_override:
        register_kwargs["Name"] = name_override
    else:
        register_kwargs["Name"] = f"{register_kwargs['Name']}-{int(time.time())}"
    if description_override:
        register_kwargs["Description"] = description_override

    ami_status.dst_ami_id = clients.ec2_dst.register_image(**register_kwargs)["ImageId"]
    ami_status.stage = "done"
    on_status(ami_status)

    return ami_status.dst_ami_id


# ---------------------------------------------------------------------------
# CLI: display manager
# ---------------------------------------------------------------------------

BAR_WIDTH = 24


def _format_bar(pct: float) -> str:
    filled = int(BAR_WIDTH * min(pct, 100.0) / 100.0)
    return "█" * filled + "░" * (BAR_WIDTH - filled)


def _format_line(
    source_id: str,
    pct: float,
    message: str,
    dst_id: Optional[str] = None,
    done: bool = False,
    error: Optional[str] = None,
) -> str:
    src = f"{source_id:<24}"
    if error:
        return f"  {src} [{'!' * BAR_WIDTH}] FAILED: {error}"
    bar = _format_bar(pct)
    dst = f"  → {dst_id}" if dst_id else ""
    check = " ✓" if done else ""
    return f"  {src} [{bar}] {pct:5.1f}%  {message}{dst}{check}"


class DisplayManager:
    """Thread-safe in-place multi-line terminal display."""

    MIN_REDRAW_INTERVAL = 0.1

    def __init__(self, count: int):
        self._lock = threading.Lock()
        self._lines: list = [""] * count
        self._initialized = False
        self._last_redraw = 0.0

    def update(self, idx: int, line: str) -> None:
        with self._lock:
            self._lines[idx] = line
            now = time.monotonic()
            if now - self._last_redraw >= self.MIN_REDRAW_INTERVAL:
                self._redraw()
                self._last_redraw = now

    def finish(self) -> None:
        with self._lock:
            cols = shutil.get_terminal_size((120, 24)).columns
            if self._initialized:
                sys.stdout.write(f"\033[{len(self._lines)}A")
            for line in self._lines:
                sys.stdout.write(f"\033[2K{line[:cols - 1]}\n")
            sys.stdout.flush()

    def _redraw(self) -> None:
        # FIX: truncate each line to terminal width so long error messages don't
        # wrap and throw off the cursor-up count used to redraw in place.
        cols = shutil.get_terminal_size((120, 24)).columns
        n = len(self._lines)
        if self._initialized:
            sys.stdout.write(f"\033[{n}A")
        for line in self._lines:
            sys.stdout.write(f"\033[2K{line[:cols - 1]}\n")
        sys.stdout.flush()
        self._initialized = True


# ---------------------------------------------------------------------------
# CLI: per-item processing
# ---------------------------------------------------------------------------

def process_item(
    idx: int,
    source_id: str,
    src_region: str,
    clients: ClientSet,
    display: DisplayManager,
    executor: ThreadPoolExecutor,
    description: Optional[str] = None,
    name: Optional[str] = None,
    kms_key_id: Optional[str] = None,
) -> tuple:

    def upd(pct, msg, dst_id=None, done=False, error=None):
        display.update(idx, _format_line(source_id, pct, msg, dst_id, done, error))

    try:
        upd(0.0, "Starting...")
        actual_id = source_id

        if source_id.startswith("i-"):
            upd(0.0, "Creating AMI from instance (no reboot)...")
            actual_id = _create_ami_from_instance(
                clients.ec2_src, source_id, name, description,
                on_wait=lambda msg: upd(0.0, msg),
            )
            upd(0.0, "AMI ready, beginning copy...", dst_id=actual_id)

        if actual_id.startswith("ami-"):

            def ami_callback(status: AmiCopyStatus):
                if status.error:
                    upd(0.0, "", error=status.error)
                    return
                pct = status.progress_pct
                n_total = len(status.snapshot_statuses)
                n_done = sum(1 for s in status.snapshot_statuses.values()
                             if s.stage == CopyStage.DONE)
                dst_snaps = sorted(s.dst_snapshot_id
                                   for s in status.snapshot_statuses.values()
                                   if s.dst_snapshot_id)
                snaps_str = ", ".join(dst_snaps) if dst_snaps else "..."
                if status.stage == "copying_snapshots":
                    upd(pct,  f"Copying snapshots ({n_done}/{n_total}) [{snaps_str}]")
                elif status.stage == "waiting_for_snapshots":
                    upd(99.0, f"Waiting for snapshots [{snaps_str}]")
                elif status.stage == "registering":
                    upd(99.0, f"Registering AMI [{snaps_str}]")
                elif status.stage == "done":
                    upd(100.0, "Done", dst_id=status.dst_ami_id, done=True)

            dst_id = copy_ami(
                src_ami_id=actual_id,
                clients=clients,
                src_region=src_region,
                name_override=name,
                description_override=description,
                kms_key_id=kms_key_id,
                executor=executor,
                on_status=ami_callback,
            )
            return source_id, dst_id

        elif actual_id.startswith("snap-"):
            dst_snap: list = [None]

            def snap_callback(status: CopyStatus):
                if status.dst_snapshot_id:
                    dst_snap[0] = status.dst_snapshot_id
                dst = dst_snap[0]
                if status.error:
                    upd(0.0, "", error=status.error)
                    return
                pct = status.progress_pct
                if status.stage == CopyStage.FETCHING_METADATA:
                    upd(0.0, "Fetching metadata...")
                elif status.stage == CopyStage.LISTING_BLOCKS:
                    upd(0.0, f"Listing blocks ({status.volume_size_gib} GiB)...")
                elif status.stage == CopyStage.STARTING_SNAPSHOT:
                    upd(0.0, f"Starting snapshot ({status.total_blocks} blocks)...")
                elif status.stage == CopyStage.COPYING_BLOCKS:
                    upd(pct, f"Copying {status.completed_blocks}/{status.total_blocks} blocks", dst_id=dst)
                elif status.stage == CopyStage.COMPLETING_SNAPSHOT:
                    upd(99.0, "Completing...", dst_id=dst)
                elif status.stage == CopyStage.DONE:
                    upd(100.0, "Done", dst_id=dst, done=True)

            dst_id = copy_snapshot(
                src_snapshot_id=actual_id,
                clients=clients,
                src_region=src_region,
                description=description,
                kms_key_id=kms_key_id,
                executor=executor,
                on_status=snap_callback,
            )
            return source_id, dst_id

        else:
            raise ValueError(f"Unrecognized ID format: {source_id} (expected snap-, ami-, or i-)")

    except Exception as e:
        upd(0.0, "", error=str(e))
        raise


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description=(
            "Copy one or more EBS snapshots, AMIs, or EC2 instances across regions "
            "using the EBS Direct APIs. All items are processed in parallel."
        )
    )
    p.add_argument(
        "source_ids", nargs="+",
        help="One or more source IDs: snapshot (snap-...), AMI (ami-...), or instance (i-...)",
    )
    p.add_argument("--from-region", required=True, help="Source AWS region (e.g. us-east-1)")
    p.add_argument("--to-region",   help="Destination AWS region; defaults to current region")
    p.add_argument("--description", help="Description for copied resources")
    p.add_argument("--name",        help="Name for copied AMI(s)")
    p.add_argument("--src-profile", help="AWS profile for source region")
    p.add_argument("--dst-profile", help="AWS profile for destination region")
    p.add_argument("--kms-key-id",  help="KMS key ARN for destination encryption")
    p.add_argument("--workers", type=int, default=WORKERS,
                   help=f"Total block-copy worker threads shared across all copies (default: {WORKERS})")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    dst_region = args.to_region or boto3.Session().region_name
    if not dst_region:
        print(
            "ERROR: Could not determine destination region. "
            "Set AWS_DEFAULT_REGION or pass --to-region.",
            file=sys.stderr,
        )
        sys.exit(1)

    source_ids = args.source_ids

    clients = ClientSet.create(
        src_region=args.from_region,
        dst_region=dst_region,
        workers=args.workers,
        src_profile=args.src_profile,
        dst_profile=args.dst_profile,
    )

    display = DisplayManager(len(source_ids))
    results: dict = {}

    # Redirect stderr so botocore tracebacks don't corrupt the display.
    # Also update the logging handler's stream reference — StreamHandler captures
    # sys.stderr at creation time, so it doesn't follow the reassignment above.
    # Without this, log.warning() calls (e.g. from _retry_on_error) write to the
    # real terminal fd, shift the cursor, and cause \033[nA to undershoot on the
    # next redraw, leaving stale display lines unoverwritten.
    sys.stderr = _stderr_capture
    for handler in logging.root.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.stream = _stderr_capture

    def _handle_sigint(sig, frame):
        _stop.set()

    signal.signal(signal.SIGINT, _handle_sigint)

    with ThreadPoolExecutor(max_workers=len(source_ids)) as executor:
        item_futures = {
            executor.submit(
                process_item,
                idx=idx,
                source_id=src_id,
                src_region=args.from_region,
                clients=clients,
                display=display,
                executor=executor,
                description=args.description,
                name=args.name,
                kms_key_id=args.kms_key_id,
            ): src_id
            for idx, src_id in enumerate(source_ids)
        }
        for f in as_completed(item_futures):
            src_id = item_futures[f]
            try:
                _, dst_id = f.result()
                results[src_id] = (dst_id, None)
            except InterruptedError:
                results[src_id] = (None, "Cancelled")
            except Exception as e:
                results[src_id] = (None, str(e))

    # Restore stderr and logging handler, then flush any captured output
    sys.stderr = sys.__stderr__
    for handler in logging.root.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.stream = sys.__stderr__
    display.finish()

    captured = _stderr_capture.drain()
    if captured:
        # Print captured stderr below the display as a single block
        print("\n--- Suppressed output ---", file=sys.stderr)
        print(captured.strip(), file=sys.stderr)
        print("-------------------------", file=sys.stderr)

    if _stop.is_set():
        print("\nCancelled.", file=sys.stderr)

    # Summary
    sep = "─" * 74
    print(f"\n{sep}")
    print(f"  {'SOURCE':<26}  {'DESTINATION':<26}  RESULT")
    print(sep)
    all_ok = True
    for src_id in source_ids:
        dst_id, error = results.get(src_id, (None, "unknown error"))
        if error:
            all_ok = False
            err_preview = error[:60] + "..." if len(error) > 60 else error
            print(f"  {src_id:<26}  {'—':<26}  FAILED: {err_preview}")
        else:
            print(f"  {src_id:<26}  {dst_id:<26}  ✓  ({dst_region})")
    print(sep)

    sys.exit(0 if all_ok else 1)
