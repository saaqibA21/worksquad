# Security Policy — AgentSquad Workspace

## 1. Overview
AgentSquad Workspace is an enterprise autonomous multi-agent command center. This document specifies the threat model, security safeguards, data governance protocols, and incident reporting procedures.

---

## 2. Hardened Architecture & Protections

### 2.1 HTTP Security Headers
Every HTTP response issued by the FastAPI application includes strict defense-in-depth security headers:
- `X-Content-Type-Options: nosniff`: Prevents MIME-type sniffing attacks.
- `X-Frame-Options: DENY`: Mitigates clickjacking and framing attempts.
- `X-XSS-Protection: 1; mode=block`: Activates native browser XSS filtering.
- `Referrer-Policy: strict-origin-when-cross-origin`: Restricts sensitive URI referrer leakage.
- `Content-Security-Policy (CSP)`: Enforces origin verification for script, style, and media contexts.

### 2.2 Client-Side XSS Sanitization (DOMPurify)
All markdown streamed by agents or loaded from archives is parsed using `marked.js` and sanitized using `DOMPurify.sanitize()` before being injected into the DOM. This neutralizes prompt injection vectors attempting to render malicious `<script>` or `onerror` handlers.

### 2.3 DoS & Abuse Prevention (Sliding Window Rate Limiting)
- `/api/mission`: Enforces a sliding-window rate limit of **20 missions/minute per client IP**.
- `/api/keys/verify`: Enforces **15 key verification attempts/minute per client IP**.
- Excessive requests receive HTTP `429 Too Many Requests` with a deterministic `Retry-After` header.

### 2.4 API Key Isolation & Redaction
- **No Persistence in Backend:** User-supplied API keys (entered in the web console) are kept client-side in browser `localStorage` and transmitted only per-request over the active session. Keys are never logged to stdout, persisted to disk, or mirrored in responses.
- **Masking:** Status and verification responses redact all keys into safe formats (`sk-...xxxx`).

### 2.5 Resource Protection & Client Disconnection
- In the SSE endpoint (`/api/mission`), active streaming loops check `request.is_disconnected()`.
- When a user closes the browser tab or clicks "Abort Mission", background LLM streaming tasks are cancelled immediately, preventing runaway compute and token leakage.

### 2.6 Atomic File Persistence
Squad memories (`data/unified_memory.json`) and run archives (`data/mission_history.json`) are written to `.tmp` files first and committed using `os.replace()`. This prevents JSON corruption or partial file writes during server reboots or system crashes.

---

## 3. Deployment Security Checklist
1. **TLS / HTTPS:** Terminate SSL/TLS via a reverse proxy (e.g. NGINX, Caddy, Cloudflare, AWS ALB).
2. **Port Configuration:** Application binds to port `8500` by default. Do not expose unsecured ports directly to public networks.
3. **Container Security:** The official `Dockerfile` runs as an unprivileged user (`agentsquad`, non-root) inside a minimal Python slim base image.

---

## 4. Reporting Vulnerabilities
If you identify a security vulnerability in AgentSquad Workspace, please do not file a public GitHub issue. Send a detailed report directly to the security team with reproduction steps.
