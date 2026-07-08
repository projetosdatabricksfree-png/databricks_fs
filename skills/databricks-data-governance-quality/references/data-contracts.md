# Data Contracts by Layer

A data contract declares the guarantees a layer offers its consumers: schema, keys,
nullability, allowed values/ranges, freshness, and how violations are handled. Contracts
are enforced with SDP expectations (Silver/Gold) and documented in `docs/data-contracts.md`.

## Contract structure (per table)

```yaml
table: silver_sgs
layer: silver
grain: one row per (codigo_serie, data_ref)
owner: data_engineering
freshness_sla: daily
schema:
  - name: codigo_serie   type: INT      nullable: false  key: true
  - name: data_ref       type: DATE     nullable: false  key: true
  - name: valor          type: DOUBLE   nullable: false
  - name: _ingerido_em   type: TIMESTAMP nullable: false  technical: true
expectations:
  - name: valor_presente   rule: "valor IS NOT NULL"        on_violation: DROP
  - name: data_no_passado  rule: "data_ref <= current_date()" on_violation: WARN
  - name: serie_valida     rule: "codigo_serie > 0"          on_violation: FAIL
```

## Layer expectations

| Layer | Guarantees | Enforcement |
|-------|-----------|-------------|
| **Bronze** | Raw fidelity, `_rescued_data` present, immutable | No expectations; auditable append-only |
| **Silver** | Typed, deduplicated (1 row/key), validated ranges | Expectations WARN/DROP/FAIL + AUTO CDC |
| **Gold** | Reconciled measures, freshness/SLA, business semantics | WARN + monitoring/alerting |

## Naming conventions (project)

- Tables/columns: pt-BR `snake_case`.
- Technical columns prefixed with `_` (`_ingerido_em`, `_rescued_data`, `_arquivo_origem`).
- Schemas: `bcb_dev_{bronze|silver|gold}` (dev) / `bcb_{...}` (prod) via `env_prefix`.

## Violation handling policy

- **WARN** — observe emerging issues without blocking (freshness, soft ranges).
- **DROP** — quarantine bad rows out of the clean set (nulls in required fields).
- **FAIL** — hard-stop for contract breaches that must never reach downstream
  (unknown keys, impossible values). Pairs with the job's `alerta_qualidade` branch.
