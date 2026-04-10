"""Reporting models for Person 2 Phase 1 validation outputs.

These models are intentionally lightweight and standard-library only so the
validation runner can produce stable output even in minimal environments.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class ValidationFinding:
    """Represent one validation outcome or defect sample."""

    check_name: str
    status: str
    severity: str
    message: str
    reason_code: str | None = None
    sample_identifier: str | None = None
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ValidationSummary:
    """Aggregate validation findings for one Phase 1 execution run."""

    run_label: str
    findings: list[ValidationFinding] = field(default_factory=list)
    aggregate_report: dict[str, Any] = field(default_factory=dict)

    def add_finding(self, finding: ValidationFinding) -> None:
        """Append a finding to the current summary."""

        self.findings.append(finding)

    def counts_by_status(self) -> dict[str, int]:
        """Return a compact status breakdown for future report rendering."""

        counts: dict[str, int] = {}
        for finding in self.findings:
            counts[finding.status] = counts.get(finding.status, 0) + 1
        return counts

    def add(
        self,
        check_name: str,
        status: str,
        severity: str,
        message: str,
        *,
        reason_code: str | None = None,
        sample_identifier: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        """Create and append a finding in one call."""

        self.add_finding(
            ValidationFinding(
                check_name=check_name,
                status=status,
                severity=severity,
                message=message,
                reason_code=reason_code,
                sample_identifier=sample_identifier,
                metrics=metrics or {},
            )
        )

    def overall_status(self) -> str:
        """Return the highest-priority status for the current run."""

        statuses = {finding.status for finding in self.findings}
        if "fail" in statuses:
            return "fail"
        if "warn" in statuses:
            return "warn"
        return "pass"

    def render_text(self) -> str:
        """Render a compact human-readable summary for CLI usage."""

        lines = [
            f"Phase 1 Validation Run: {self.run_label}",
            f"Overall status: {self.overall_status()}",
            f"Status counts: {self.counts_by_status()}",
        ]

        for finding in self.findings:
            detail = f"[{finding.status.upper()}] {finding.check_name}: {finding.message}"
            if finding.reason_code:
                detail += f" (reason={finding.reason_code})"
            if finding.metrics:
                detail += f" metrics={finding.metrics}"
            lines.append(detail)

        return "\n".join(lines)
