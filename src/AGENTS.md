# AGENTS.md

## Commands
- Web app: `uvicorn apps.api_web.app:app --reload`
- Dashboard path: `python web.py`
- CLI path: `python main.py --symbol 005930 --market krx`

## Scope
- `src/` holds the real implementation behind the thin app entrypoints.
- `web_app.py` contains the real dashboard implementation.
- `kis_websocket.py` contains the KIS integration and schema-heavy mapping logic.
- `src/kis_websocket.py`: SUNSET shim, remove when KSXT program_trade StreamKind ships.
- `config.py` is a compatibility re-export over `packages.shared.config`.

## Always
- Keep `apps/api_web`, `web.py`, and `main.py` compatibility paths working.
- Preserve runtime credential checks; do not restore import-time hard failure for missing KIS credentials.
- When changing market mappings (`krx`, `nxt`, `total`), review all related TR IDs, schema maps, and rename maps together.
- Keep docs honest when live KIS credentials are absent.

## Ask First
- Large refactors of `src/web_app.py`.
- Breaking schema, field-name, or display rename-map changes in websocket processing.
- Entry changes that break `web.py`, `main.py`, or `apps.api_web.app:app` compatibility.

## Never
- Copy real dashboard implementation back into `apps/api_web/app.py`.
- Copy `.env` values or real credentials into code samples or docs.
- Describe market data behavior as live/complete when the runtime path is not actually wired with valid credentials.
