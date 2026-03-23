from prefect import flow, task, get_run_logger
from prefect.cache_policies import NO_CACHE
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
    LogFilter,
    LogFilterFlowRunId,
)
import anyio
from datetime import datetime, timedelta
from google import genai
import logging  # <-- para mapear niveles numéricos a texto

MAX_RUNS = 10
LOOKBACK_MIN = 180  # minutos
MODEL = "gemini-2.5-flash"

@task(cache_policy=NO_CACHE)
def fetch_failed_runs_and_logs():
    """
    Usa el cliente async desde sync con anyio.run() y devuelve un texto único
    con los logs de los últimos flow runs fallidos (FAILED) en la ventana LOOKBACK_MIN.
    """
    async def _fetch():
        since = datetime.utcnow() - timedelta(minutes=LOOKBACK_MIN)
        text_chunks = []

        async with get_client() as client:
            # Pedimos runs en FAILED con filtros tipados (no dicts)
            failed_state_filter = FlowRunFilterState(
                type=FlowRunFilterStateType(any_=["FAILED"])
            )
            runs = await client.read_flow_runs(
                limit=MAX_RUNS,
                flow_run_filter=FlowRunFilter(state=failed_state_filter)
            )  # Filtros tipados en v3  [2](https://github.com/PrefectHQ/prefect)

            for r in runs:
                if not r.start_time or r.start_time.replace(tzinfo=None) < since:
                    continue

                # Logs por flow_run_id usando LogFilter (modelo tipado)
                log_filter = LogFilter(flow_run_id=LogFilterFlowRunId(any_=[str(r.id)]))
                logs = await client.read_logs(log_filter=log_filter)  # firma correcta  [1](https://docs.prefect.io/v3/api-ref/rest-api/server/logs/read-logs)

                # 'level' es entero; mapeamos a nombre con logging.getLevelName(...)
                log_lines = []
                for l in logs:
                    level_name = logging.getLevelName(int(l.level)) if l.level is not None else "LEVEL?"
                    log_lines.append(f"[{l.timestamp}] {level_name}: {l.message}")

                header = (
                    f"\n\n### Flow: {r.name} | ID: {r.id} | "
                    f"Start: {r.start_time} | State: {getattr(r, 'state_name', getattr(r.state, 'name', 'UNKNOWN'))}\n"
                )
                text_chunks.append(header + "\n".join(log_lines))

        return "\n".join(text_chunks) if text_chunks else "No hay fallos recientes en la ventana."

    return anyio.run(_fetch)  # Cliente async desde tarea sync  [3](https://www.prefect.io/)


@task(cache_policy=NO_CACHE)
def summarize_with_gemini(all_logs_text: str) -> str:
    client = genai.Client()  # GEMINI_API_KEY desde entorno
    prompt = (
        "Eres SRE/Plataformas. Analiza estos logs de ejecuciones FALLIDAS de Prefect y devuelve en inglés:\n"
        "- Causas raíz probables\n- Flujos o deployments más problemáticos\n"
        "- Métricas rápidas (conteos por tipo)\n- Recomendaciones priorizadas y accionables\n\n"
        f"LOGS:\n{all_logs_text}"
    )
    resp = client.models.generate_content(model=MODEL, contents=prompt)
    return resp.text


@flow
def summarize_recent_failures_flow():
    logger = get_run_logger()
    all_logs = fetch_failed_runs_and_logs()
    summary = summarize_with_gemini(all_logs)
    logger.info("\n" + summary)