# Agent memory

A conversation window is finite. Once early turns fall out of it, an agent that
made a decision three hours ago has no way back to it unless someone wrote it
down.

`MEMORY.*` is that record. Messages live in ordinary hashes, and both the recent
tail and the older-but-relevant turns are one command away.

## Adding messages

```
MEMORY.ADD session role content [TAG tag ...]
```

```bash
redis-cli MEMORY.ADD session-42 user "the deploy key rotates every 90 days"
redis-cli MEMORY.ADD session-42 assistant "noted — I'll flag anything that assumes a longer lifetime" TAG ops
redis-cli MEMORY.ADD session-42 system "working directory is /srv/api"
```

`role` is one of `user`, `assistant`, `system`, `tool`. The reply is the message's
sequence number.

## The recent window

```
MEMORY.GET session [LIMIT n] [ROLE role]
```

```bash
redis-cli MEMORY.GET session-42 LIMIT 10
redis-cli MEMORY.GET session-42 ROLE user
```

Returns alternating `role`, `content` pairs in conversation order, oldest first.
`LIMIT` counts messages from the **newest** end, so `LIMIT 10` is the last ten
turns and not the first ten.

This is what you paste into a prompt as the conversation so far.

## Relevant older turns

```
MEMORY.SEARCH session text [K n]
```

```bash
redis-cli MEMORY.SEARCH session-42 "when does the deploy key rotate" K 3
```

Searches by meaning, across the whole session, and returns the closest messages —
including ones that scrolled out of the recent window long ago. Results come back
**most relevant first**, because this is a search.

Use `MEMORY.GET` for continuity and `MEMORY.SEARCH` for recall:

| Situation | Command |
|---|---|
| continue the conversation | `MEMORY.GET session LIMIT 20` |
| "what did we decide about X?" | `MEMORY.SEARCH session "decision about X" K 5` |
| find a fact stated early on | `MEMORY.SEARCH session "the API rate limit" K 3` |

## Maintaining sessions

```bash
redis-cli MEMORY.SESSIONS            # sessions holding messages
redis-cli MEMORY.INFO session-42     # message count, first and last timestamp
redis-cli MEMORY.CLEAR session-42    # remove a session and its messages
```

## What to remember

Recording every turn is noise; the search results get worse as the session fills
with chatter. Record what a future session would need:

| Remember | Skip |
|---|---|
| decisions and the reasoning behind them | play-by-play narration |
| user preferences and constraints | restating the question |
| facts about the environment (paths, versions, endpoints) | tool chatter and retries |
| corrections the user made | anything already in the wiki |

If a fact belongs to the project rather than the conversation, it belongs in a
[wiki page](llm-wiki) instead, where every session can find it.

## Pairing with the cache

Once a reworded question reliably produces a good answer, cache it so it is
cheap next time — see [Semantic cache](semantic-cache). Memory is about *this*
session's continuity; the cache is about not paying twice for the same answer
across all sessions.
