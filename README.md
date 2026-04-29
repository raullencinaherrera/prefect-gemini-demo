# Prefect Gemini Demo

AI-assisted operational automation demo using Prefect, Google Gemini and GitHub.

This project demonstrates how an orchestration platform can detect failed automation flows, collect execution context, analyze logs with an LLM, and optionally create GitHub pull requests with suggested fixes.

The goal is not to replace engineers, but to show how AI can support infrastructure and automation teams by reducing troubleshooting time and converting repeated failures into actionable improvements.

## Concept

Modern operations teams run many scheduled and event-driven automations. When one fails, engineers usually need to:

- inspect the failed run
- read logs
- identify the source code involved
- understand the failure
- propose a fix
- document or implement the correction

This demo explores how that process can be partially automated using Prefect as the orchestration layer and Gemini as the reasoning engine.

## What this project does

The demo can:

- run Prefect flows locally using Docker Compose
- monitor failed Prefect flow runs
- retrieve logs and execution metadata
- send the relevant context to Gemini
- generate an analysis report
- optionally suggest code changes
- optionally create a GitHub pull request with the proposed fix

## Architecture

```text
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
