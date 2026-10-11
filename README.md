#  AgentSquad Workspace

> **Premium real-time AI Agent Command Center** — coordinate Grok (xAI), Dots (OpenAI), Muse (Meta), and a live News Scout from a single mission prompt.

![Dark mode command center with 4 agent cards](https://img.shields.io/badge/UI-Dark%20Mode%20Command%20Center-0B0F17?style=flat-square&labelColor=151D2A)
![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?style=flat-square&logo=fastapi&logoColor=white)
![Demo Mode](https://img.shields.io/badge/Demo%20Mode-Ready-FB923C?style=flat-square)

---

##  One-Command Startup

```bash
pip install -r requirements.txt && python main.py
```

Then open **http://localhost:8000** in your browser.

> **No API keys required** — the app runs in full Demo/Simulation mode out of the box with realistic streaming responses.

---

##  The Squad

| Agent | Provider | Specialty | Accent |
|-------|----------|-----------|--------|
|  **Grok** | xAI | Live X/social pulse, viral trends, real-time sentiment | Electric Cyan |
|  **Dots** | OpenAI | Deep strategic reasoning, enterprise synthesis, structured reporting | Emerald |
|  **Muse** | Meta | Concrete action plans, execution checklists, browser errands | Violet |
|  **News Scout** | RSS / NewsAPI | Breaking headlines, live wire feeds, global context | Amber |

---

##  Project Structure

```
AgentSquad/
├── main.py              ← FastAPI backend + orchestrator + SSE streaming
├── requirements.txt     ← Minimal pinned dependencies
├── .env.example         ← Environment config template
├── static/
│   └── index.html       ← Self-contained SPA (Tailwind CDN + Vanilla JS)
└── README.md
```

---

##  API Key Setup (Optional — for Live Mode)

1. Copy the example env file:
   ```bash
   cp .env.example .env
   ```

2. Fill in your keys in `.env`:
   ```env
   OPENAI_API_KEY=sk-...      # Enables Dots + Muse live mode
   XAI_API_KEY=xai-...        # Enables Grok live mode
   NEWS_API_KEY=...            # Enables NewsAPI (optional — falls back to RSS)
   ```

3. Restart the server — the nav badge switches from **Demo Mode** → **Live Mode** automatically.

---

##  Architecture

```
User Mission
     │
     ▼
┌─────────────┐
│ Orchestrator│  ← Decomposes mission into 4 subtasks
└──────┬──────┘
       │
  ┌────┴────────────────────────┐
  │                             │
  ▼                             │
 News Scout                   │ (Phase 1 — runs first for context)
  │                             │
  ├──→  Grok  ─┐              │
  │              ├─ Parallel ──→│ (Phase 2)
  └──→  Dots  ─┘              │
                                │
                     Muse ────┘ (Phase 3 — action planner)
                         │
                         ▼
                Executive Briefing
                  (Synthesized output)
```

**Streaming:** All agent outputs stream via **Server-Sent Events (SSE)** — agent cards light up in real time as work progresses.

---

##  UI Features

-  **Premium dark-mode** command center (Linear × Bloomberg Terminal aesthetic)
-  **Glassmorphism** agent cards with individual accent colors and pulsing status dots
-  **Live flow visualizer** showing mission decomposition and agent pipeline
-  **Real-time streaming** — tokens appear character-by-character in each agent card
-  **Unified Executive Briefing** with Copy and Export to Markdown buttons
-  **Preset mission suggestions** (clickable chips)
- ️ **Ctrl/Cmd + Enter** to deploy mission

---

##  Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.10+ / FastAPI |
| Streaming | Server-Sent Events (SSE) via `sse-starlette` |
| AI APIs | OpenAI GPT-4o, xAI Grok-beta |
| News | NewsAPI + feedparser (BBC, Reuters, TechCrunch RSS) |
| Frontend | Vanilla JS + Tailwind CSS (CDN) + marked.js |
| HTTP | httpx async client |

---

##  Dependencies

```
fastapi          — Web framework
uvicorn          — ASGI server
python-dotenv    — .env loading
httpx            — Async HTTP client (API calls)
openai           — OpenAI SDK
feedparser       — RSS feed parsing
pydantic         — Data validation
sse-starlette    — SSE streaming
aiofiles         — Async file I/O
```

---

##  Configuration

| Variable | Description | Required |
|----------|-------------|----------|
| `OPENAI_API_KEY` | Powers Dots + Muse agents | Optional |
| `XAI_API_KEY` | Powers Grok agent | Optional |
| `NEWS_API_KEY` | NewsAPI headlines (falls back to RSS) | Optional |
| `HOST` | Server bind address (default: `0.0.0.0`) | No |
| `PORT` | Server port (default: `8000`) | No |

---

##  Demo Mode

When no API keys are set, every agent runs in **simulation mode** with:
- Realistic streaming delays mimicking actual AI inference
- Structured, contextually-aware demo responses
- Full UI animations and status transitions

---

*Built with FastAPI + Tailwind CSS. Inspired by Linear, Vercel, and Bloomberg Terminal.*
