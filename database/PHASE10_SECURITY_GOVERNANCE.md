# Phase 10 Security and Governance Policy

## Secret Handling
- Secrets must not be committed to git.
- Runtime reports may only record whether a secret is configured, never the secret value itself.
- Environment variables, the OS keychain, or a local secret manager are the approved local storage paths.

## Wallet Redaction
- User-facing alert payloads must redact wallet-like identifiers by default.
- Internal database rows can retain raw runtime state needed for replay, but outward-facing payloads must stay redacted.

## Governance
- The canonical operating mode remains `rule_based_plus_shadow_ml`.
- No user-facing output may call a wallet or cluster an insider.
- Model activation and workflow-version changes must remain traceable through durable registry or workflow-version rows.
