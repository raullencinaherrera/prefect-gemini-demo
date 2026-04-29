# Prefect Gemini Demo

AI-assisted operational automation demo using Prefect, Google Gemini and GitHub.

This project demonstrates how an orchestration platform can detect failed automation flows, collect execution context, analyze logs with an LLM, and optionally create GitHub pull requests with suggested fixes.

The goal is not to replace engineers, but to show how AI can support infrastructure and automation teams by reducing troubleshooting time and converting repeated failures into actionable improvements.

---

## Concept

Modern operations teams run many scheduled and event-driven automations. When one fails, engineers usually need to:

- inspect the failed run  
- read logs  
- identify the source code involved  
- understand the failure  
- propose a fix  
- document or implement the correction  

This demo explores how that process can be partially automated using Prefect as the orchestration layer and Gemini as the reasoning engine.

---

## What this project does

The demo can:

- run Prefect flows locally using Docker Compose  
- monitor failed Prefect flow runs  
- retrieve logs and execution metadata  
- send the relevant context to Gemini  
- generate an analysis report  
- optionally suggest code changes  
- optionally create a GitHub pull request with the proposed fix  

---

## Architecture
Prefect Server
↓
Failed Flow Detection
↓
Log and Context Collector
↓
Gemini Analysis
↓
Report / Suggested Fix
↓
Optional GitHub Pull Request

---

## Why this matters

This project is a small proof of concept for AI-assisted operations.

It shows how deterministic automation and LLM reasoning can work together:

- Prefect provides orchestration and execution history  
- Python services collect context and control the workflow  
- Gemini analyzes the failure and proposes next steps  
- GitHub can be used as the delivery mechanism for code improvements  

---

## Technologies

- Python  
- Prefect 3  
- Docker Compose  
- PostgreSQL  
- Redis  
- Google Gemini API  
- GitHub API  

---

## Repository structure

├── deployments/ # Prefect deployment definitions
├── flows/ # Demo Prefect flows
├── worker/ # Worker image and runtime logic
├── docker-compose.yml # Local Prefect stack
└── README.md


---

## Use case

A typical scenario:

1. A scheduled automation flow fails.  
2. The monitoring flow detects the failed run.  
3. Logs and metadata are collected.  
4. Gemini receives the operational context.  
5. The system generates a technical analysis.  
6. If requested, a GitHub pull request is created with a proposed fix.  

---

## Design principles

- AI should assist engineers, not blindly execute changes.  
- Deterministic rules should be preferred for known issues.  
- LLMs should be used when context interpretation is needed.  
- Suggested changes should be reviewable before being applied.  
- Automation should remain auditable and controlled.  

---

## Future improvements

- Add long-term operational memory  
- Store known failure patterns  
- Promote repeated successful fixes into deterministic rules  
- Add Slack or Teams notifications  
- Add ServiceNow or Jira integration  
- Add a web dashboard for failed flow analysis  

---

## Security note

Do not commit API keys or credentials to the repository.

Use a `.env` file or a secret manager for values such as:
GEMINI_API_KEY=
GITHUB_TOKEN=
PREFECT_API_URL=

---

## Positioning

This demo is part of a broader idea: building an AI-assisted operations engine where automation platforms, observability systems, source control and LLMs work together to improve infrastructure reliability.
