from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys

from config.runtime_mode import build_runtime_decision_summary
from config.runtime_env import REPO_ROOT, RuntimeEnvLoadResult, load_runtime_env
from utils.logger import get_logger

log = get_logger("run_runtime")

PROCESS_SHUTDOWN_TIMEOUT_SECONDS = float(
    os.getenv("POLYMARKET_RUNTIME_PROCESS_SHUTDOWN_TIMEOUT_SECONDS", "45")
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Canonical startup path for the local Polymarket runtime."
    )
    parser.add_argument(
        "--env-file",
        default="",
        help="Explicit runtime env file. Defaults to .env.runtime, then legacy .env, then shell-only.",
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Load config, print the startup plan, validate toggles, and exit without launching services.",
    )
    return parser


def _bool_label(value: bool) -> str:
    return "enabled" if value else "disabled"


def _db_target_label(settings) -> str:
    if settings.DB_BACKEND == "postgres":
        return "postgres (POLYMARKET_DATABASE_URL configured)"
    return f"sqlite ({settings.DB_PATH})"


def _phase4_channels_label(settings) -> str:
    channels: list[str] = []
    if settings.ENABLE_PHASE4_TELEGRAM:
        channels.append("telegram")
    if settings.ENABLE_PHASE4_DISCORD:
        channels.append("discord")
    return ", ".join(channels) if channels else "none"


def _format_env_source(result: RuntimeEnvLoadResult) -> str:
    if result.env_file is None:
        primary = "shell-only environment"
    else:
        suffix = "loaded" if result.loaded else "already present in environment"
        primary = f"{result.env_file} ({result.source}, {suffix})"
    if result.secret_env_file is None:
        secret = "no secret env file auto-loaded"
    else:
        suffix = "loaded" if result.secret_loaded else "already present in environment"
        secret = f"{result.secret_env_file} ({result.secret_source}, {suffix})"
    return f"primary={primary}; secrets={secret}"


def _print_secret_posture(env_result: RuntimeEnvLoadResult) -> None:
    if env_result.secret_keys_in_primary_env:
        log.warning(
            "Secret posture warning: primary runtime env file contains secret-like keys: "
            f"{', '.join(env_result.secret_keys_in_primary_env)}"
        )
    if env_result.secret_keys_in_secret_env:
        log.info(
            "Secret env keys detected in the dedicated secret file: "
            f"{', '.join(env_result.secret_keys_in_secret_env)}"
        )
    for warning in env_result.warnings:
        log.warning(warning)


def _validate_runtime_plan(settings) -> list[str]:
    errors: list[str] = []
    if settings.DB_BACKEND == "postgres" and not settings.DATABASE_URL:
        errors.append("POLYMARKET_DB_BACKEND=postgres requires POLYMARKET_DATABASE_URL.")
    if not settings.ENABLE_PHASE3_DETECTOR and not settings.ALLOW_COLLECTOR_ONLY_RUNTIME:
        errors.append(
            "Canonical runtime requires POLYMARKET_ENABLE_PHASE3_DETECTOR=true. "
            "If you intentionally need collector-only maintenance mode, set "
            "POLYMARKET_ALLOW_COLLECTOR_ONLY_RUNTIME=true."
        )
    if settings.ENABLE_PHASE4_RUNTIME and not settings.ENABLE_PHASE3_DETECTOR:
        errors.append("Phase 4 live runtime requires POLYMARKET_ENABLE_PHASE3_DETECTOR=true.")
    if settings.ENABLE_PHASE6_LIVE_RUNTIME and not settings.ENABLE_PHASE3_DETECTOR:
        errors.append("Phase 6 live runtime requires POLYMARKET_ENABLE_PHASE3_DETECTOR=true.")
    if settings.ENABLE_PHASE6_LIVE_RUNTIME and not settings.ENABLE_PHASE4_RUNTIME:
        errors.append("Phase 6 live runtime requires POLYMARKET_ENABLE_PHASE4_RUNTIME=true.")
    if settings.ENABLE_PHASE6_LIVE_RUNTIME and not settings.ENABLE_PHASE6_SHADOW_MODE:
        errors.append(
            "Phase 6 live runtime requires POLYMARKET_ENABLE_PHASE6_SHADOW_MODE=true."
        )
    if settings.ENABLE_PHASE4_TELEGRAM:
        if not settings.PHASE4_TELEGRAM_BOT_TOKEN:
            errors.append("Telegram delivery is enabled, but POLYMARKET_PHASE4_TELEGRAM_BOT_TOKEN is missing.")
        if not settings.PHASE4_TELEGRAM_CHAT_ID:
            errors.append("Telegram delivery is enabled, but POLYMARKET_PHASE4_TELEGRAM_CHAT_ID is missing.")
    if settings.ENABLE_PHASE4_DISCORD and not settings.PHASE4_DISCORD_WEBHOOK_URL:
        errors.append("Discord delivery is enabled, but POLYMARKET_PHASE4_DISCORD_WEBHOOK_URL is missing.")
    return errors


def _print_runtime_plan(
    settings,
    env_result: RuntimeEnvLoadResult,
    decision_summary: dict[str, object],
    storage_summary,
) -> None:
    log.info("=" * 60)
    log.info("POLYMARKET CANONICAL RUNTIME PLAN")
    log.info("=" * 60)
    log.info(f"Environment source: {_format_env_source(env_result)}")
    log.info(f"Database backend: {settings.DB_BACKEND}")
    log.info(f"Database target: {_db_target_label(settings)}")
    log.info(
        "Runtime decision: "
        f"canonical_v1_mode={decision_summary['canonical_v1_operating_mode']}, "
        f"default_profile={decision_summary['canonical_default_runtime_profile']}, "
        f"configured_profile={decision_summary['configured_runtime_profile']}"
    )
    log.info(
        "Phase 3 detector: "
        f"{_bool_label(settings.ENABLE_PHASE3_DETECTOR)} "
        f"(state_backend={settings.PHASE3_STATE_BACKEND}, poll_seconds={settings.PHASE3_POLL_SECONDS}, "
        f"checkpoint_interval={settings.PHASE3_CHECKPOINT_INTERVAL}, "
        f"collector_only_override={settings.ALLOW_COLLECTOR_ONLY_RUNTIME})"
    )
    log.info(
        "Phase 4 runtime: "
        f"{_bool_label(settings.ENABLE_PHASE4_RUNTIME)} "
        f"(channels={_phase4_channels_label(settings)}, poll_seconds={settings.PHASE4_RUNTIME_POLL_SECONDS}, "
        f"max_candidate_age_minutes={settings.PHASE4_RUNTIME_MAX_CANDIDATE_AGE_MINUTES}, "
        f"delivery_min_severity={settings.PHASE4_ALERT_DELIVERY_MIN_SEVERITY}, "
        f"max_deliveries_per_pass={settings.PHASE4_ALERT_MAX_DELIVERIES_PER_PASS})"
    )
    log.info(
        "Phase 6 runtime: "
        f"{_bool_label(settings.ENABLE_PHASE6_LIVE_RUNTIME)} "
        f"(shadow_mode={_bool_label(settings.ENABLE_PHASE6_SHADOW_MODE)}, poll_seconds={settings.PHASE6_LIVE_POLL_SECONDS})"
    )
    log.info(
        "Storage guard: "
        f"status={storage_summary.status} "
        f"(free_gb={storage_summary.free_gb}, free_percent={storage_summary.free_percent}, "
        f"managed_gb={storage_summary.managed_gb}, prune_candidates={storage_summary.prune_candidate_count}, "
        f"min_free_gb={settings.PHASE11_RUNTIME_MIN_FREE_GB}, "
        f"min_free_percent={settings.PHASE11_RUNTIME_MIN_FREE_PERCENT})"
    )
    _print_secret_posture(env_result)


async def _terminate_process(name: str, process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    log.info(f"Stopping {name}...")
    process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=PROCESS_SHUTDOWN_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        log.warning(f"{name} did not stop in time; killing it.")
        process.kill()
        await process.wait()


async def _launch_process(name: str, script: str, *script_args: str) -> asyncio.subprocess.Process:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(REPO_ROOT / script),
        *script_args,
        cwd=str(REPO_ROOT),
        env=os.environ.copy(),
    )
    log.info(f"Started {name} (pid={process.pid})")
    return process


async def _run_supervisor(settings) -> int:
    processes: dict[str, asyncio.subprocess.Process] = {}
    shutdown_event = asyncio.Event()

    def _handle_signal(signum, frame) -> None:
        log.info(f"Signal {signum} received by runtime launcher; shutting down.")
        shutdown_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    processes["collector"] = await _launch_process("collector", "run_collector.py")
    if settings.ENABLE_PHASE4_RUNTIME:
        processes["phase4_live"] = await _launch_process("phase4_live", "run_phase4_live.py")
    if settings.ENABLE_PHASE6_LIVE_RUNTIME:
        processes["phase6_live"] = await _launch_process(
            "phase6_live",
            "run_phase6_shadow_live.py",
            "--iterations",
            "0",
        )

    wait_tasks = {
        asyncio.create_task(process.wait(), name=name): name
        for name, process in processes.items()
    }
    shutdown_task = asyncio.create_task(shutdown_event.wait(), name="runtime_shutdown")

    done, pending = await asyncio.wait(
        {shutdown_task, *wait_tasks.keys()},
        return_when=asyncio.FIRST_COMPLETED,
    )

    exit_code = 0
    if shutdown_task not in done:
        finished_task = next(task for task in done if task in wait_tasks)
        failed_name = wait_tasks[finished_task]
        exit_code = finished_task.result() or 0
        if exit_code == 0:
            log.warning(f"{failed_name} exited unexpectedly with code 0; stopping the runtime.")
            exit_code = 1
        else:
            log.error(f"{failed_name} exited with code {exit_code}; stopping the runtime.")

    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)

    await asyncio.gather(
        *[_terminate_process(name, process) for name, process in processes.items()],
        return_exceptions=True,
    )
    return exit_code


async def _main() -> int:
    args = build_parser().parse_args()
    env_result = load_runtime_env(args.env_file or None, override=True)
    os.environ["POLYMARKET_RUNTIME_LAUNCHED"] = "1"

    from config import settings
    from database.db_manager import apply_schema
    from phase6 import Phase6Repository
    from phase3.state_store import Phase3StateStoreConfigurationError, create_state_store
    from phase7.runtime_storage import build_runtime_storage_status

    apply_schema()
    decision_summary = build_runtime_decision_summary(settings=settings)
    storage_summary, storage_payload = build_runtime_storage_status()

    _print_runtime_plan(settings, env_result, decision_summary, storage_summary)
    errors = _validate_runtime_plan(settings)
    if settings.ENABLE_PHASE3_DETECTOR:
        try:
            state_context = await create_state_store(require_backend="durable", allow_fallback=False)
        except Phase3StateStoreConfigurationError as exc:
            errors.append(
                "Phase 3 state-backend preflight failed: "
                f"{exc}"
            )
        else:
            log.info(
                "Phase 3 state-backend preflight succeeded "
                f"(backend={state_context.backend_name})."
            )
            await state_context.store.aclose()
    if storage_summary.status == "blocked":
        errors.append(
            "Runtime storage guard blocked startup: "
            f"{storage_payload['status_reason']} "
            f"(free_gb={storage_summary.free_gb}, free_percent={storage_summary.free_percent}). "
            "Run run_runtime_storage_status.py before restarting long-running collection."
        )
    if settings.ENABLE_PHASE6_LIVE_RUNTIME:
        active_shadow_model = Phase6Repository().load_active_shadow_model()
        if active_shadow_model is None:
            errors.append(
                "Phase 6 live runtime is enabled, but no active shadow model is registered. "
                "Run run_phase6_activate_model.py first or disable POLYMARKET_ENABLE_PHASE6_LIVE_RUNTIME."
            )
    if errors:
        for error in errors:
            log.error(error)
        return 2

    if args.check_only:
        log.info("Runtime configuration check passed.")
        return 0

    return await _run_supervisor(settings)


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
