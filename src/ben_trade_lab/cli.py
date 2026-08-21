from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from . import __version__
from .anchor import verify_anchor_store
from .audit import create_pro_review_receipt, create_test_receipt
from .config import load_config
from .data import (
    DataIntegrityError,
    bind_manifest_to_config,
    fetch_klines,
    load_bars_from_manifest,
    partition_lockbox,
    read_manifest_metadata,
    verify_manifest,
)
from .integrity import verified_hashed_object
from .paper import initialize_paper, paper_status, stop_paper
from .validation import (
    _require_config_anchor_store_id,
    finalize_holdout,
    select_candidate,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ben-trade")
    parser.add_argument("--root", default=".")
    parser.add_argument("--config", default="configs/btcusdt_1h.toml")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("doctor")

    anchor = subcommands.add_parser("anchor")
    anchor_commands = anchor.add_subparsers(dest="anchor_command", required=True)
    anchor_verify = anchor_commands.add_parser("verify")
    anchor_verify.add_argument("--anchor-root", required=True)
    anchor_verify.add_argument("--anchor-store-id", required=True)

    data = subcommands.add_parser("data")
    data_commands = data.add_subparsers(dest="data_command", required=True)
    data_commands.add_parser("fetch")
    verify = data_commands.add_parser("verify")
    verify.add_argument("--manifest", required=True)
    partition = data_commands.add_parser("partition-lockbox")
    partition.add_argument("--manifest", required=True)

    research = subcommands.add_parser("research")
    research_commands = research.add_subparsers(dest="research_command", required=True)
    select = research_commands.add_parser("select")
    select.add_argument("--manifest", required=True)
    finalize = research_commands.add_parser("finalize")
    finalize.add_argument("--manifest", required=True)
    finalize.add_argument("--selection", required=True)
    finalize.add_argument("--test-receipt", required=True)
    finalize.add_argument("--review-receipt", required=True)
    finalize.add_argument("--anchor-root", required=True)
    finalize.add_argument("--anchor-store-id", required=True)

    audit = subcommands.add_parser("audit")
    audit_commands = audit.add_subparsers(dest="audit_command", required=True)
    tests = audit_commands.add_parser("tests")
    tests.add_argument("--selection", required=True)
    review = audit_commands.add_parser("record-pro-review")
    review.add_argument("--selection", required=True)
    review.add_argument("--review", required=True)
    review.add_argument("--verdict", choices=("PROCEED", "BLOCKED"), required=True)
    review.add_argument("--model-visible", required=True)
    review.add_argument("--reasoning-visible", required=True)

    paper = subcommands.add_parser("paper")
    paper_commands = paper.add_subparsers(dest="paper_command", required=True)
    init = paper_commands.add_parser("init")
    init.add_argument("--report", required=True)
    init.add_argument("--capital", type=float, default=1000.0)
    init.add_argument("--anchor-root", required=True)
    init.add_argument("--anchor-store-id", required=True)
    paper_commands.add_parser("status")
    paper_commands.add_parser("stop")
    paper_commands.add_parser("run-once")
    return parser


def _emit(value: object) -> None:
    print(json.dumps(value, sort_keys=True, indent=2, ensure_ascii=False))


def _requests_live_execution(value: str) -> bool:
    normalized = value.strip().casefold().replace("_", "-")
    return normalized in {
        "live",
        "--live",
        "live-trading",
        "live-execution",
    } or normalized.startswith(("--live=", "--execution-mode=live", "--mode=live"))


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if any(_requests_live_execution(value) for value in arguments):
        print("LIVE_EXECUTION_UNAVAILABLE", file=sys.stderr)
        return 2
    args = _parser().parse_args(arguments)
    root = Path(args.root).resolve()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = root / config_path
    config = load_config(config_path)

    if args.command == "anchor":
        configured_store_id = str(config.raw["anchor"]["store_id"])
        _require_config_anchor_store_id(config, args.anchor_store_id)
        store = verify_anchor_store(
            args.anchor_root,
            repository_root=root,
            expected_store_id=configured_store_id,
            expected_store_sha256=str(config.raw["anchor"]["store_sha256"]),
        )
        _emit(
            {
                "status": "ANCHOR_STORE_VERIFIED",
                "anchor_root": str(store.root),
                "anchor_store_id": store.store_id,
            }
        )
        return 0

    if args.command == "doctor":
        _emit(
            {
                "status": "PASS",
                "version": __version__,
                "config_sha256": config.config_sha256,
                "modes": ["BACKTEST", "PAPER"],
                "live_execution": "UNAVAILABLE",
                "market_data_authentication": "NONE",
            }
        )
        return 0
    if args.command == "data" and args.data_command == "fetch":
        manifest = fetch_klines(config, root=root)
        _emit({"status": "PASS", "manifest": str(manifest)})
        return 0
    if args.command == "data" and args.data_command == "verify":
        manifest = verify_manifest(args.manifest, root=root)
        bind_manifest_to_config(manifest, config, str(manifest.get("kind", "FULL_SOURCE")))
        _emit(
            {
                "status": "PASS",
                "row_count": manifest["row_count"],
                "normalized_sha256": manifest["normalized_sha256"],
            }
        )
        return 0
    if args.command == "data" and args.data_command == "partition-lockbox":
        preholdout, holdout = partition_lockbox(args.manifest, config, root=root)
        _emit(
            {
                "status": "PASS",
                "preholdout_manifest": str(preholdout),
                "locked_holdout_manifest": str(holdout),
            }
        )
        return 0
    if args.command == "research":
        if args.research_command == "select":
            preflight = read_manifest_metadata(args.manifest, root=root)
            bind_manifest_to_config(preflight, config, "PREHOLDOUT")
            bars, manifest = load_bars_from_manifest(
                args.manifest,
                root=root,
                expected_kind="PREHOLDOUT",
            )
            if manifest["manifest_file_sha256"] != preflight["manifest_file_sha256"]:
                raise DataIntegrityError("preholdout manifest changed after metadata preflight")
            if (
                manifest.get("partition_descriptor_sha256")
                != preflight.get("partition_descriptor_sha256")
            ):
                raise DataIntegrityError(
                    "preholdout partition descriptor changed after metadata preflight"
                )
            artifact = select_candidate(bars, manifest, config, root=root)
        else:
            artifact = finalize_holdout(
                args.manifest,
                config,
                args.selection,
                args.test_receipt,
                args.review_receipt,
                anchor_root=args.anchor_root,
                anchor_store_id=args.anchor_store_id,
                root=root,
            )
        _emit({"status": "PASS", "artifact": str(artifact)})
        return 0
    if args.command == "audit" and args.audit_command == "tests":
        artifact = create_test_receipt(root, args.selection, config)
        receipt = verified_hashed_object(artifact, "receipt_sha256", root=root)
        replay = receipt.get("full_provenance_replay")
        authorized = (
            receipt.get("status") == "PASS"
            and isinstance(replay, dict)
            and replay.get("status") == "PASS"
        )
        _emit(
            {
                "status": "PASS" if authorized else "BLOCKED",
                "artifact": str(artifact),
                "full_provenance_replay": replay,
            }
        )
        return 0 if authorized else 3
    if args.command == "audit" and args.audit_command == "record-pro-review":
        artifact = create_pro_review_receipt(
            root,
            args.selection,
            config,
            args.review,
            args.verdict,
            args.model_visible,
            args.reasoning_visible,
        )
        _emit({"status": "PASS", "artifact": str(artifact)})
        return 0
    if args.command == "paper" and args.paper_command == "init":
        _emit(
            initialize_paper(
                root,
                args.report,
                config,
                args.capital,
                anchor_root=args.anchor_root,
                anchor_store_id=args.anchor_store_id,
            )
        )
        return 0
    if args.command == "paper" and args.paper_command == "status":
        _emit(paper_status(root))
        return 0
    if args.command == "paper" and args.paper_command == "stop":
        _emit(stop_paper(root))
        return 0
    if args.command == "paper" and args.paper_command == "run-once":
        _emit(
            {
                "status": "BLOCKED",
                "reason": "PAPER_FORWARD_RUNNER_NOT_IMPLEMENTED",
                "live_execution": "UNAVAILABLE",
            }
        )
        return 3
    raise AssertionError("unreachable command")


if __name__ == "__main__":
    raise SystemExit(main())
