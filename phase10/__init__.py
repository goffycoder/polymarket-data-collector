from importlib import import_module


_EXPORT_TO_MODULE = {
    "PHASE10_TASK1_CONTRACT_VERSION": "phase10.real_provider_evidence",
    "run_phase10_task1_real_provider_evidence": "phase10.real_provider_evidence",
    "render_phase10_task1_markdown": "phase10.real_provider_evidence",
    "PHASE10_TASK2_CONTRACT_VERSION": "phase10.analyst_loop_expansion",
    "run_phase10_task2_analyst_loop_expansion": "phase10.analyst_loop_expansion",
    "render_phase10_task2_markdown": "phase10.analyst_loop_expansion",
    "PHASE10_TASK3_CONTRACT_VERSION": "phase10.heldout_validation_pack",
    "run_phase10_task3_heldout_validation_pack": "phase10.heldout_validation_pack",
    "PHASE10_TASK4_CONTRACT_VERSION": "phase10.heldout_model_completion",
    "run_phase10_task4_heldout_model_completion": "phase10.heldout_model_completion",
    "PHASE10_TASK5_CONTRACT_VERSION": "phase10.ops_governance_closeout",
    "run_phase10_task5_ops_governance_closeout": "phase10.ops_governance_closeout",
}


def __getattr__(name: str):
    module_name = _EXPORT_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module 'phase10' has no attribute {name!r}")
    module = import_module(module_name)
    value = getattr(module, name)
    globals()[name] = value
    return value


__all__ = list(_EXPORT_TO_MODULE)
