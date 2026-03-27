import json
import os

KNOWLEDGE_PATH = "/opt/prefect/flows/knowledge_base.json"
DOCS_DIR = "/opt/prefect/flows/business_docs/"

def load_knowledge_base():
    if not os.path.exists(KNOWLEDGE_PATH):
        return []

    with open(KNOWLEDGE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def load_business_document(doc_name: str) -> str:
    path = os.path.join(DOCS_DIR, doc_name)
    if not os.path.exists(path):
        return f"Patch failed ({path}), and no specific knowledge document was found for this issue."
    with open(path, "r", encoding="utf-8") as f:
        return f.read()
    
def decide_with_business_layer(analysis: str, logs: str):
    text = f"{analysis}\n{logs}".lower()
    knowledge = load_knowledge_base()

    for rule in knowledge:
        if rule["match"] in text:
            doc_file = rule.get("doc_file")
            doc_text = load_business_document(doc_file) if doc_file else None
            return {
                "action": rule["action"],
                "problem_type": rule["type"],
                "source": "knowledge_base",
                "assignment_group": rule.get("assignment_group"),
                "flow_template": rule.get("flow_template"),
                "doc_file": rule.get("doc_file"),
                "doc_text": doc_text,
            }

    return {
        "action": "create_pr",
        "problem_type": "code",
        "source": "default",
        "doc_text": "",
    }


def generate_knowledge_doc(filename: str, decision: dict, analysis: str) -> str:
    return f"""
FLOW: {filename}

DECISION:
- action: {decision.get("action")}
- type: {decision.get("problem_type")}
- source: {decision.get("source")}

ANALYSIS:
{analysis}

REUSABLE: YES
"""