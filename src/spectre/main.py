from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from .client import SapientEdgeClient
from .config import load_config


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SPECTRE BSI Flex 335 v2 Edge Node")
    p.add_argument("--config", default="config/spectre.yaml")
    p.add_argument(
        "--node-id",
        default=None,
        help=(
            "Override node.node_id from the config file. Only needed when running "
            "multiple spectre instances in parallel against the same Fusion Node "
            "(each needs a distinct, stable UUID) - a single instance should keep "
            "the node_id from its config file across restarts."
        ),
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cfg = load_config(args.config)
    if args.node_id is not None:
        cfg["node"]["node_id"] = args.node_id

    config_path = Path(args.config).resolve()
    reg_path = Path(cfg["node"]["registration_file"])
    if not reg_path.is_absolute():
        # Resolve relative to current working directory first, matching CLI expectation.
        cfg["node"]["registration_file"] = str(reg_path.resolve())

    level = getattr(logging, cfg.get("logging", {}).get("level", "INFO").upper())
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    try:
        asyncio.run(SapientEdgeClient(cfg).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
