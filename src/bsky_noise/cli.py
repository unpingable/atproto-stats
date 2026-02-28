from __future__ import annotations

import argparse
import asyncio
import getpass
import json
import os
import sys
from pathlib import Path

from rich.console import Console

from . import atproto
from .candidates import (
    pick_candidates,
    refresh_candidates,
    render_poll_text,
    render_pollblue_text,
    score_candidates,
)
from .config import LOCK_PATH, SESSION_PATH, load_session, save_session
from .db import connect
from .locking import LockError, file_lock
from .report import build_summary, write_report
from .sync import sync_follows


console = Console()


def _load_watchlist(path: str | None) -> set[str]:
    if not path:
        return set()
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"Watchlist file not found: {p}")
    values: set[str] = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        values.add(raw.lstrip("@").lower())
    return values


def cmd_auth(args: argparse.Namespace) -> None:
    handle = (
        args.handle
        or os.environ.get("BSKY_HANDLE")
        or os.environ.get("BSKY_USERNAME")
    )
    if not handle:
        raise SystemExit("Handle required. Pass --handle or set BSKY_HANDLE.")

    if args.app_password == "-":
        app_password = sys.stdin.read().strip()
    elif args.app_password:
        app_password = args.app_password
    else:
        app_password = os.environ.get("BSKY_APP_PASSWORD") or getpass.getpass(
            "Bsky app password: "
        )

    async def run() -> None:
        session = await atproto.create_session(
            handle=handle, app_password=app_password, pds=args.pds
        )
        save_session(session.__dict__)
        console.print(f"Saved session for {session.handle} in {SESSION_PATH}")

    asyncio.run(run())


def cmd_sync(args: argparse.Namespace) -> None:
    if not SESSION_PATH.exists():
        raise SystemExit("No session found. Run `bsky_noise auth` first.")

    session = load_session()
    conn = connect()
    concurrency = args.concurrency
    if args.degraded and concurrency > 2:
        concurrency = 2

    client = atproto.XrpcClient(
        access_jwt=session.access_jwt,
        pds=session.pds,
        appview_base=args.appview_base,
        use_appview=args.use_appview,
        max_concurrency=concurrency,
        verbose=args.verbose,
        max_retries=args.max_retries,
        timeout=args.timeout,
        degraded=args.degraded,
        auto_degraded=args.auto_degraded,
    )
    try:
        with file_lock(LOCK_PATH):
            if args.dry_run:
                asyncio.run(
                    sync_follows(
                        conn,
                        client,
                        actor=session.did,
                        actor_handle=session.handle,
                        windows=args.window,
                        dry_run=True,
                    )
                )
                return
            asyncio.run(
                sync_follows(
                    conn,
                    client,
                    actor=session.did,
                    actor_handle=session.handle,
                    windows=args.window,
                )
            )
    except LockError as exc:
        raise SystemExit(exc.message) from exc


def cmd_report(args: argparse.Namespace) -> None:
    if not SESSION_PATH.exists():
        raise SystemExit("No session found. Run `bsky_noise auth` first.")

    conn = connect()
    weights = {
        "posts": args.w_posts,
        "replies": args.w_replies,
        "reposts": args.w_reposts,
    }
    watchlist = _load_watchlist(args.watchlist)
    try:
        with file_lock(LOCK_PATH):
            write_report(
                conn,
                windows=args.window,
                output_dir=Path(args.output),
                weights=weights,
                compare_prior=args.compare_prior,
                what_if_mute=args.what_if_mute,
                watchlist=watchlist,
                export_csv=Path(args.export_csv) if args.export_csv else None,
            )
    except LockError as exc:
        raise SystemExit(exc.message) from exc
    console.print(f"Wrote report to {args.output}")


def cmd_compute(args: argparse.Namespace) -> None:
    conn = connect()
    weights = {
        "posts": args.w_posts,
        "replies": args.w_replies,
        "reposts": args.w_reposts,
    }
    watchlist = _load_watchlist(args.watchlist)
    try:
        with file_lock(LOCK_PATH):
            summary = build_summary(
                conn,
                windows=args.window,
                weights=weights,
                compare_prior=args.compare_prior,
                what_if_mute=args.what_if_mute,
                watchlist=watchlist,
            )
    except LockError as exc:
        raise SystemExit(exc.message) from exc
    out = Path(args.summary_output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    console.print(f"Wrote computed summary to {out}")


def _make_client(args: argparse.Namespace, session) -> atproto.XrpcClient:
    concurrency = getattr(args, "concurrency", 4)
    if getattr(args, "degraded", False) and concurrency > 2:
        concurrency = 2
    return atproto.XrpcClient(
        access_jwt=session.access_jwt,
        pds=session.pds,
        appview_base=getattr(args, "appview_base", "https://public.api.bsky.app"),
        use_appview=getattr(args, "use_appview", False),
        max_concurrency=concurrency,
        verbose=getattr(args, "verbose", False),
        max_retries=getattr(args, "max_retries", 6),
        timeout=getattr(args, "timeout", 30.0),
        degraded=getattr(args, "degraded", False),
        auto_degraded=getattr(args, "auto_degraded", False),
    )


def cmd_candidates_refresh(args: argparse.Namespace) -> None:
    if not SESSION_PATH.exists():
        raise SystemExit("No session found. Run `bsky_noise auth` first.")
    session = load_session()
    conn = connect()
    client = _make_client(args, session)
    actor = args.actor or session.did
    asyncio.run(refresh_candidates(conn, client, actor))


def cmd_candidates_score(args: argparse.Namespace) -> None:
    if not SESSION_PATH.exists():
        raise SystemExit("No session found. Run `bsky_noise auth` first.")
    session = load_session()
    conn = connect()
    client = _make_client(args, session)
    actor = args.actor or session.did
    asyncio.run(
        score_candidates(
            conn,
            client,
            actor,
            mode=args.mode,
            sample_items=args.sample,
            daily_budget=args.daily_budget,
        )
    )


def cmd_candidates_pick(args: argparse.Namespace) -> None:
    if not SESSION_PATH.exists():
        raise SystemExit("No session found. Run `bsky_noise auth` first.")
    session = load_session()
    conn = connect()
    actor = args.actor or session.did
    picks = pick_candidates(conn, actor, mode=args.mode, k=args.k)
    console.print(render_poll_text(picks, args.mode))


def cmd_candidates_render(args: argparse.Namespace) -> None:
    if not SESSION_PATH.exists():
        raise SystemExit("No session found. Run `bsky_noise auth` first.")
    session = load_session()
    conn = connect()
    actor = args.actor or session.did
    picks = pick_candidates(conn, actor, mode=args.mode, k=args.k)
    if args.format == "pollblue":
        console.print(render_pollblue_text(picks, args.mode))
    else:
        console.print(render_poll_text(picks, args.mode))


def cmd_candidates_shortlist(args: argparse.Namespace) -> None:
    cmd_candidates_pick(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="bsky_noise")
    sub = parser.add_subparsers(dest="cmd", required=True)

    auth = sub.add_parser("auth", help="Authenticate and store session")
    auth.add_argument("--handle")
    auth.add_argument("--app-password")
    auth.add_argument("--pds", default=atproto.DEFAULT_PDS)
    auth.set_defaults(func=cmd_auth)

    sync = sub.add_parser("sync", help="Sync follow activity")
    sync.add_argument("--window", type=int, action="append", default=[30, 90])
    sync.add_argument("--concurrency", type=int, default=4)
    sync.add_argument("--dry-run", action="store_true")
    sync.add_argument("--verbose", action="store_true")
    sync.add_argument("--use-appview", action="store_true")
    sync.add_argument("--appview-base", default="https://public.api.bsky.app")
    sync.add_argument("--max-retries", type=int, default=6)
    sync.add_argument("--timeout", type=float, default=30.0)
    sync.add_argument("--degraded", action="store_true")
    sync.add_argument("--auto-degraded", action="store_true")
    sync.set_defaults(func=cmd_sync)

    report = sub.add_parser("report", help="Generate JSON and HTML reports")
    report.add_argument("--window", type=int, action="append", default=[30, 90])
    report.add_argument("--output", default=str(Path.cwd() / "output"))
    report.add_argument("--w-posts", type=float, default=1.0)
    report.add_argument("--w-replies", type=float, default=1.5)
    report.add_argument("--w-reposts", type=float, default=0.75)
    report.add_argument("--compare-prior", action="store_true")
    report.add_argument("--what-if-mute", type=int, action="append", default=[5, 10])
    report.add_argument("--export-csv")
    report.add_argument("--watchlist")
    report.set_defaults(func=cmd_report)

    compute = sub.add_parser("compute", help="Materialize derived summary JSON from local data")
    compute.add_argument("--window", type=int, action="append", default=[30, 90])
    compute.add_argument("--w-posts", type=float, default=1.0)
    compute.add_argument("--w-replies", type=float, default=1.5)
    compute.add_argument("--w-reposts", type=float, default=0.75)
    compute.add_argument("--compare-prior", action="store_true")
    compute.add_argument("--what-if-mute", type=int, action="append", default=[5, 10])
    compute.add_argument("--watchlist")
    compute.add_argument("--summary-output", default=str(Path.cwd() / "output" / "summary.json"))
    compute.set_defaults(func=cmd_compute)

    cand = sub.add_parser("candidates", help="Orbit candidate discovery workflow")
    cand_sub = cand.add_subparsers(dest="cand_cmd", required=True)

    cand_refresh = cand_sub.add_parser("refresh", help="Refresh followers-follows candidate pool")
    cand_refresh.add_argument("--actor")
    cand_refresh.add_argument("--use-appview", action="store_true", default=True)
    cand_refresh.add_argument("--appview-base", default="https://public.api.bsky.app")
    cand_refresh.add_argument("--verbose", action="store_true")
    cand_refresh.add_argument("--max-retries", type=int, default=6)
    cand_refresh.add_argument("--timeout", type=float, default=30.0)
    cand_refresh.add_argument("--degraded", action="store_true")
    cand_refresh.add_argument("--auto-degraded", action="store_true")
    cand_refresh.set_defaults(func=cmd_candidates_refresh)

    cand_score = cand_sub.add_parser("score", help="Score candidates")
    cand_score.add_argument("--actor")
    cand_score.add_argument("--mode", choices=["interaction", "poster"], default="interaction")
    cand_score.add_argument("--sample", type=int, default=75)
    cand_score.add_argument("--daily-budget", type=int, default=200)
    cand_score.add_argument("--use-appview", action="store_true", default=True)
    cand_score.add_argument("--appview-base", default="https://public.api.bsky.app")
    cand_score.add_argument("--concurrency", type=int, default=2)
    cand_score.add_argument("--verbose", action="store_true")
    cand_score.add_argument("--max-retries", type=int, default=6)
    cand_score.add_argument("--timeout", type=float, default=30.0)
    cand_score.add_argument("--degraded", action="store_true")
    cand_score.add_argument("--auto-degraded", action="store_true")
    cand_score.set_defaults(func=cmd_candidates_score)

    cand_pick = cand_sub.add_parser("pick", help="Pick candidates and render text (legacy alias)")
    cand_pick.add_argument("--actor")
    cand_pick.add_argument("--mode", choices=["interaction", "poster"], default="interaction")
    cand_pick.add_argument("--k", type=int, default=4)
    cand_pick.set_defaults(func=cmd_candidates_pick)

    cand_render = cand_sub.add_parser("render", help="Render text from current picks (legacy alias)")
    cand_render.add_argument("--actor")
    cand_render.add_argument("--mode", choices=["interaction", "poster"], default="interaction")
    cand_render.add_argument("--k", type=int, default=4)
    cand_render.add_argument("--format", choices=["text", "pollblue"], default="text")
    cand_render.set_defaults(func=cmd_candidates_render)

    cand_short = cand_sub.add_parser("shortlist", help="Pick orbit shortlist candidates")
    cand_short.add_argument("--actor")
    cand_short.add_argument("--mode", choices=["interaction", "poster"], default="interaction")
    cand_short.add_argument("--k", type=int, default=4)
    cand_short.set_defaults(func=cmd_candidates_shortlist)

    cand_render_short = cand_sub.add_parser(
        "render-shortlist", help="Render shortlist text in selected format"
    )
    cand_render_short.add_argument("--actor")
    cand_render_short.add_argument("--mode", choices=["interaction", "poster"], default="interaction")
    cand_render_short.add_argument("--k", type=int, default=4)
    cand_render_short.add_argument("--format", choices=["text", "pollblue"], default="text")
    cand_render_short.set_defaults(func=cmd_candidates_render)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
