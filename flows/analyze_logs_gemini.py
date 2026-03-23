from prefect import flow, task, get_run_logger
from pathlib import Path
from google import genai

LOGS_DIR = Path("/opt/prefect/flows/sample_logs")  # monta aquí tus logs si quieres

@task
def load_logs(max_chars: int = 8000) -> str:
    """
    Carga logs de ejemplo (concatena varios ficheros y recorta).
    En tu caso, puedes leer de S3, filesystem, etc.
    """
    texts = []
    if LOGS_DIR.exists():
        for p in sorted(LOGS_DIR.glob("*.log")):
            try:
                texts.append(p.read_text(encoding="utf-8", errors="ignore"))
            except Exception:
                pass
    text = "\n\n---\n\n".join(texts)
    return text[:max_chars] if len(text) > max_chars else text

@task
def summarize_with_gemini(raw_text: str, model: str = "gemini-2.5-flash") -> str:
    """
    Llama a Gemini vía google-genai. El cliente toma GEMINI_API_KEY del entorno.
    """
    client = genai.Client()  # lee GEMINI_API_KEY automáticamente si está seteada
    prompt = (
        "Eres un ingeniero de plataforma. Resume estos logs destacando:\n"
        "- Errores críticos y su frecuencia\n"
        "- Jobs/etapas más problemáticos\n"
        "- Tendencias y recomendaciones accionables en bullets\n\n"
        f"Logs:\n{raw_text}"
    )
    resp = client.models.generate_content(model=model, contents=prompt)
    return resp.text

@flow
def analyze_logs_gemini_flow():
    logger = get_run_logger()
    raw = load_logs()
    summary = summarize_with_gemini(raw)
    logger.info("\n" + summary)

if __name__ == "__main__":
    analyze_logs_gemini_flow()