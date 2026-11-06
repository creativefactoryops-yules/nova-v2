# NOVA v2 — AI Website Admin
❯ --force ./buildyou · terminal forest edition

Chat with NOVA and your website updates live. WebSocket streaming, RAG knowledge base, REST API.

## Quick Start

```bash
npm install
cp .env.example .env
# add your ANTHROPIC_API_KEY to .env
npm start
```

Open http://localhost:3001

## Env

```
ANTHROPIC_API_KEY=sk-ant-...
PORT=3001
ADMIN_TOKEN=          # optional — locks write endpoints
CORS_ORIGIN=*         # set to your frontend domain in production
DATA_DIR=./data       # JSON file storage
```

## Deploy (Railway / Render / Fly)

1. Push this folder to GitHub
2. Connect to Railway/Render — it auto-detects Node
3. Set `ANTHROPIC_API_KEY` in env vars
4. Deploy. WebSocket works out of the box.

## API

- `GET /api/site` — current site data
- `PATCH /api/site` — update fields (dot paths)
- `POST /api/site/reset` — reset to defaults
- `GET/POST/DELETE /api/docs` — knowledge base
- `GET/DELETE /api/history` — chat history
- `WS /ws` — streaming chat (`chat`, `ping`)

## Frontend

The admin UI lives in `public/index.html`. Served automatically by Express.
To point the admin at a different backend, edit the `WS_URL` and `API_URL`
constants at the top of the file.
