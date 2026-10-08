/**
 * nio-db.js — Ultra-lightweight client for NioDB (The Agentic Database)
 * Zero dependencies, sub-10KB, works in Node.js, Bun, Deno, Next.js, and browser.
 */

class NioDB {
  constructor(options = {}) {
    this.url = (options.url || "http://127.0.0.1:7432").replace(/\/+$/, "");
    if (options.token) {
      this.token = options.token;
    } else if (options.clientToken && options.secretToken) {
      this.token = `${options.clientToken}:${options.secretToken}`;
    } else if (options.clientToken || options.secretToken) {
      this.token = options.clientToken || options.secretToken;
    } else {
      this.token = null;
    }
    this.workspaceId = options.workspaceId || null;
    this.timeoutMs = options.timeoutMs ?? 30_000;
    this.maxResponseBytes = options.maxResponseBytes ?? 8 * 1024 * 1024;
    this.fetch = options.fetch || globalThis.fetch;
    if (!Number.isFinite(this.timeoutMs) || this.timeoutMs <= 0 || !Number.isSafeInteger(this.maxResponseBytes) || this.maxResponseBytes <= 0) {
      throw new TypeError("Positive timeoutMs and maxResponseBytes are required");
    }
  }

  async _request(path, method = "GET", body = null, extraHeaders = {}, options = {}) {
    const headers = {
      "Accept": "application/json",
      ...extraHeaders,
    };
    if (this.token) {
      headers["Authorization"] = `Bearer ${this.token}`;
    }
    if (body !== null && method !== "GET") {
      headers["Content-Type"] = "application/json";
    }

    let url = `${this.url}${path}`;
    if (this.workspaceId && !url.includes("workspace_id=")) {
      const sep = url.includes("?") ? "&" : "?";
      url += `${sep}workspace_id=${encodeURIComponent(this.workspaceId)}`;
    }

    const timeoutMs = options.timeoutMs ?? this.timeoutMs;
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new TypeError("timeoutMs must be positive");
    const controller = new AbortController();
    const abort = () => controller.abort(options.signal?.reason);
    if (options.signal?.aborted) abort();
    else options.signal?.addEventListener("abort", abort, { once: true });
    const timer = setTimeout(() => controller.abort(new Error("NioDB request deadline exceeded")), timeoutMs);
    let reader;
    try {
      const response = await this.fetch(url, {
        method, headers, signal: controller.signal,
        body: body !== null && method !== "GET" ? JSON.stringify(body) : undefined,
      });
      if (response.status === 204) return null;
      const declared = Number(response.headers.get("content-length"));
      if (declared > this.maxResponseBytes) throw new RangeError("NioDB response exceeds byte limit");
      if (!response.body?.getReader) throw new Error("NioDB requires readable response streams");
      reader = response.body.getReader();
      const chunks = [];
      let length = 0;
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        length += value.byteLength;
        if (length > this.maxResponseBytes) throw new RangeError("NioDB response exceeds byte limit");
        chunks.push(value);
      }
      const bytes = new Uint8Array(length);
      let offset = 0;
      for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
      const raw = new TextDecoder().decode(bytes);
      let data;
      try { data = JSON.parse(raw); }
      catch (cause) {
        if (response.ok) throw new Error("NioDB returned invalid JSON", { cause });
        data = null;
      }
      if (!response.ok) {
        const err = new Error(data?.error?.message || `Request failed with status ${response.status}`);
        err.status = response.status;
        err.code = data?.error?.code || "http_error";
        err.requestId = data?.request_id;
        err.data = data;
        throw err;
      }
      return data;
    } finally {
      clearTimeout(timer);
      options.signal?.removeEventListener("abort", abort);
      controller.abort();
      if (reader) { try { await reader.cancel(); } catch {} reader.releaseLock(); }
    }
  }

  collection(name) {
    const db = this;
    return {
      async insert(data, options = {}) {
        const payload = { collection: name, data: { ...data } };
        if (options.ttl) payload.data.ttl = options.ttl;
        const extraHeaders = {};
        if (options.idempotencyKey) {
          extraHeaders["idempotency-key"] = options.idempotencyKey;
        }
        return db._request("/api/v1/records", "POST", payload, extraHeaders, options);
      },

      async find(options = {}) {
        let path = `/api/v1/records?type=${encodeURIComponent(name)}`;
        if (options.limit) path += `&limit=${encodeURIComponent(options.limit)}`;
        if (options.cursor) path += `&cursor=${encodeURIComponent(options.cursor)}`;
        return db._request(path, "GET", null, {}, options);
      },

      async get(id, options = {}) {
        return db._request(`/api/v1/records/${encodeURIComponent(id)}`, "GET", null, {}, options);
      },

      async update(id, data, options = {}) {
        const payload = data && data.data ? data : { data };
        return db._request(`/api/v1/records/${encodeURIComponent(id)}`, "PATCH", payload, {}, options);
      },

      async delete(id, options = {}) {
        return db._request(`/api/v1/records/${encodeURIComponent(id)}`, "DELETE", null, {}, options);
      },

      async insertMany(records, options = {}) {
        const payload = {
          collection: name,
          action: "insert",
          records: records.map(r => (r && r.data ? r : { collection: name, data: r })),
        };
        return db._request("/api/v1/records/bulk", "POST", payload);
      },

      async bulkInsert(records, options = {}) {
        return this.insertMany(records, options);
      },

      async updateMany(records, options = {}) {
        const payload = {
          action: "update",
          records: records.map(r => ({ id: r.id || r.record_id, data: r.data || r })),
        };
        return db._request("/api/v1/records/bulk", "POST", payload);
      },

      async bulkUpdate(records, options = {}) {
        return this.updateMany(records, options);
      },

      async deleteMany(ids, options = {}) {
        const payload = {
          action: "delete",
          ids: ids.map(id => typeof id === "object" ? (id.id || id.record_id) : id),
        };
        return db._request("/api/v1/records/bulk", "POST", payload);
      },

      async bulkDelete(ids, options = {}) {
        return this.deleteMany(ids, options);
      },

      async searchVector(options = {}) {
        const payload = {
          collection: name,
          vector: options.vector || [],
          top_k: options.topK || options.top_k || 5,
          min_score: options.minScore || options.min_score || 0.0,
        };
        return db._request("/api/v1/records/search", "POST", payload);
      },

      watch(callback) {
        let path = `/api/v1/records/watch?collection=${encodeURIComponent(name)}`;
        let url = `${db.url}${path}`;
        if (db.workspaceId) url += `&workspace_id=${encodeURIComponent(db.workspaceId)}`;
        const headers = {};
        if (db.token) headers["Authorization"] = `Bearer ${db.token}`;

        const controller = new AbortController();
        fetch(url, { headers, signal: controller.signal })
          .then(async (res) => {
            if (!res.ok || !res.body) return;
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buffer = "";
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              buffer += decoder.decode(value, { stream: true });
              const lines = buffer.split("\n\n");
              buffer = lines.pop() || "";
              for (const block of lines) {
                const dataLine = block.split("\n").find(l => l.startsWith("data: "));
                if (dataLine) {
                  try {
                    const parsed = JSON.parse(dataLine.slice(6));
                    callback(parsed);
                  } catch {}
                }
              }
            }
          })
          .catch(() => {});

        return () => controller.abort();
      },
    };
  }

  async mcp(method, params = {}) {
    return this._request("/mcp", "POST", {
      jsonrpc: "2.0",
      id: Date.now(),
      method,
      params,
    });
  }

  async bulk(operations) {
    if (Array.isArray(operations)) {
      return this._request("/api/v1/records/bulk", "POST", { operations });
    }
    return this._request("/api/v1/records/bulk", "POST", operations);
  }

  async query(sql, parameters = [], options = {}) {
    return this._request("/api/v1/query", "POST", { sql, parameters }, {}, options);
  }

  async configureCollection(collection, constraints, options = {}) {
    return this._request(`/api/v1/collections/${encodeURIComponent(collection)}/constraints`, "PUT", constraints, {}, options);
  }

  async getCollectionConstraints(collection, options = {}) {
    return this._request(`/api/v1/collections/${encodeURIComponent(collection)}/constraints`, "GET", null, {}, options);
  }

  async mutate(request, options = {}) {
    return this._request("/api/v1/mutations", "POST", request, {}, options);
  }

  async getPersistenceStatus(options = {}) {
    return this._request("/api/v1/persistence/status", "GET", null, {}, options);
  }

  async startDeletionJob(rootId, idempotencyKey, options = {}) {
    return this._request("/api/v1/deletion-jobs", "POST", { root_id: rootId, idempotency_key: idempotencyKey }, {}, options);
  }

  async getDeletionJob(id, options = {}) {
    return this._request(`/api/v1/deletion-jobs/${encodeURIComponent(id)}`, "GET", null, {}, options);
  }

  async getLease(resource, options = {}) {
    return this._request(`/api/v1/leases?resource=${encodeURIComponent(resource)}`, "GET", null, {}, options);
  }

  async acquireLease(resource, owner, ttlMs = 30_000, options = {}) {
    return this._request("/api/v1/leases", "POST", { action: "acquire", resource, owner, ttl_ms: ttlMs }, {}, options);
  }

  async renewLease(proof, ttlMs = 30_000, options = {}) {
    const { resource, owner, fence } = proof;
    return this._request("/api/v1/leases", "POST", { action: "renew", resource, owner, fence, ttl_ms: ttlMs }, {}, options);
  }

  async releaseLease(proof, options = {}) {
    const { resource, owner, fence } = proof;
    return this._request("/api/v1/leases", "POST", { action: "release", resource, owner, fence }, {}, options);
  }

  async vacuum() {
    return this._request("/api/v1/admin/vacuum", "POST", {});
  }

  async getTools() {
    return this._request("/api/v1/agent/tools", "GET");
  }

  get sessions() {
    const db = this;
    return {
      async list() {
        return db._request("/api/v1/sessions", "GET");
      },

      async create(options = {}) {
        const payload = {
          title: options.title || "Untitled Session",
          goal: options.goal || "",
          agent: options.agent || "agy",
          model: options.model || undefined,
          persona: options.persona || "senior-engineer",
          model_tier: options.modelTier || options.model_tier || "strong",
          metadata: options.metadata || {},
        };
        return db._request("/api/v1/sessions", "POST", payload);
      },

      get(sessionId) {
        return db.session(sessionId);
      },
    };
  }

  session(sessionId) {
    const db = this;
    return {
      async create(options = {}) {
        const payload = {
          title: options.title || "Untitled Session",
          goal: options.goal || "",
          agent: options.agent || "agy",
          model: options.model || undefined,
          persona: options.persona || "senior-engineer",
          model_tier: options.modelTier || options.model_tier || "strong",
          metadata: options.metadata || {},
        };
        return db._request("/api/v1/sessions", "POST", payload);
      },

      async get() {
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}`, "GET");
      },

      async update(updates = {}) {
        const payload = {
          title: updates.title,
          goal: updates.goal,
          status: updates.status,
          active_agent: updates.activeAgent || updates.active_agent,
          model: updates.model,
          persona: updates.persona,
          model_tier: updates.modelTier || updates.model_tier,
          metadata: updates.metadata,
        };
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}`, "PATCH", payload);
      },

      async switch(options = {}) {
        const toAgent = typeof options === "string" ? options : (options.toAgent || options.agent || options.to_agent);
        const payload = {
          to_agent: toAgent,
          model: typeof options === "object" ? options.model : undefined,
          reason: typeof options === "object" ? options.reason : undefined,
          persona: typeof options === "object" ? options.persona : undefined,
          summary_of_work: typeof options === "object" ? (options.summaryOfWork || options.summary_of_work) : undefined,
        };
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}/switch`, "POST", payload);
      },

      async appendTurn(options = {}) {
        const payload = {
          agent: options.agent || "unknown",
          model: options.model || "unknown",
          summary: options.summary || "",
          files_touched: options.filesTouched || options.files_touched || [],
          tokens: options.tokens || null,
          cost_usd: options.costUsd || options.cost_usd || 0.0,
        };
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}/turns`, "POST", payload);
      },

      async getManifest() {
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}/manifest`, "GET");
      },

      async logDeadEnd(options = {}) {
        const payload = {
          hypothesis: options.hypothesis || "",
          reason: options.reason || "",
          agent: options.agent || "unknown",
        };
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}/dead-ends`, "POST", payload);
      },

      async getDeadEnds() {
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}/dead-ends`, "GET");
      },
    };
  }

  get bridge() {
    const db = this;
    return {
      async create(options = {}) {
        return db.sessions.create(options);
      },

      async get(sessionId) {
        return db.session(sessionId).get();
      },

      async update(sessionId, updates = {}) {
        return db.session(sessionId).update(updates);
      },

      async switch(sessionId, options = {}) {
        return db.session(sessionId).switch(options);
      },

      async appendTurn(sessionId, options = {}) {
        return db.session(sessionId).appendTurn(options);
      },

      async getManifest(sessionId) {
        return db.session(sessionId).getManifest();
      },

      async logDeadEnd(sessionId, options = {}) {
        return db.session(sessionId).logDeadEnd(options);
      },

      async getDeadEnds(sessionId) {
        return db.session(sessionId).getDeadEnds();
      },

      adapters: {
        agy: {
          name: "agy",
          formatPrompt(manifest, instructions) {
            const text = typeof manifest === "object" && manifest !== null ? (manifest.manifest_text || JSON.stringify(manifest, null, 2)) : String(manifest);
            return `[NioBridge Manifest]\n${text}\n\n[Instructions]\n${instructions}`;
          },
        },
        claude: {
          name: "claude",
          formatPrompt(manifest, instructions) {
            const text = typeof manifest === "object" && manifest !== null ? (manifest.manifest_text || JSON.stringify(manifest, null, 2)) : String(manifest);
            return `<nio_bridge_manifest>\n${text}\n</nio_bridge_manifest>\n\n${instructions}`;
          },
        },
        codex: {
          name: "codex",
          formatPrompt(manifest, instructions) {
            const text = typeof manifest === "object" && manifest !== null ? (manifest.manifest_text || JSON.stringify(manifest, null, 2)) : String(manifest);
            return `/* NIO_BRIDGE_MANIFEST\n${text}\n*/\n\n${instructions}`;
          },
        },
        cursor: {
          name: "cursor",
          formatPrompt(manifest, instructions) {
            const text = typeof manifest === "object" && manifest !== null ? (manifest.manifest_text || JSON.stringify(manifest, null, 2)) : String(manifest);
            return `# CONTEXT FROM NIOBRIDGE\n${text}\n\n# TASK\n${instructions}`;
          },
        },
      },
    };
  }

  get tasks() {
    const db = this;
    return {
      async list(options = {}) {
        let path = "/api/v1/tasks";
        const params = [];
        const sessionId = options.sessionId || options.session_id;
        if (sessionId) params.push(`session_id=${encodeURIComponent(sessionId)}`);
        if (options.status) params.push(`status=${encodeURIComponent(options.status)}`);
        const assignedAgent = options.assignedAgent || options.assigned_agent;
        if (assignedAgent) params.push(`assigned_agent=${encodeURIComponent(assignedAgent)}`);
        if (options.role) params.push(`role=${encodeURIComponent(options.role)}`);
        if (params.length > 0) path += `?${params.join("&")}`;
        return db._request(path, "GET");
      },

      async create(options = {}) {
        const payload = {
          title: options.title || "Task",
          prompt: options.prompt || "",
          priority: options.priority || "normal",
          session_id: options.sessionId || options.session_id || undefined,
          role: options.role || undefined,
          assigned_agent: options.assignedAgent || options.assigned_agent || undefined,
          dependencies: options.dependencies || [],
          metadata: options.metadata || {},
        };
        return db._request("/api/v1/tasks", "POST", payload);
      },

      async get(id) {
        return db._request(`/api/v1/tasks/${encodeURIComponent(id)}`, "GET");
      },

      async update(id, updates = {}) {
        const payload = {
          status: updates.status,
          progress: updates.progress,
          assigned_agent: updates.assignedAgent || updates.assigned_agent,
          log_line: updates.logLine || updates.log_line,
          result: updates.result,
        };
        return db._request(`/api/v1/tasks/${encodeURIComponent(id)}`, "PATCH", payload);
      },
    };
  }

  get assemble() {
    const db = this;
    return {
      nioDecideSwarm(goal = "", options = {}) {
        const lower = (goal || "").toLowerCase();
        let planner = { agent: "agy", model: options.plannerModel || options.model || "gemini-2.5-pro", persona: "Systems Architect" };
        let coder = { agent: "codex", model: options.coderModel || options.model || "gpt-4o", persona: "Core Implementer" };
        let tester = { agent: "claude", model: options.testerModel || options.model || "claude-3-7-sonnet", persona: "QA & Verification Lead" };
        let rationale = "Architectural planning with AGY, high-throughput code synthesis with Codex, adversarial verification with Claude.";

        if (lower.includes("frontend") || lower.includes("react") || lower.includes("ui") || lower.includes("css")) {
          planner = { agent: "claude", model: options.plannerModel || options.model || "claude-3-7-sonnet", persona: "Product & UI Architect" };
          coder = { agent: "agy", model: options.coderModel || options.model || "gemini-2.5-pro", persona: "UI & Component Specialist" };
          tester = { agent: "codex", model: options.testerModel || options.model || "gpt-4o", persona: "E2E & Integration Verifier" };
          rationale = "UI structure led by Claude, component drafting by AGY, E2E test verification by Codex.";
        } else if (lower.includes("security") || lower.includes("crypto") || lower.includes("merkle") || lower.includes("audit") || lower.includes("rust") || lower.includes("backend")) {
          planner = { agent: "agy", model: options.plannerModel || options.model || "gemini-2.5-pro", persona: "Cryptographic Systems Architect" };
          coder = { agent: "codex", model: options.coderModel || options.model || "gpt-4o", persona: "Rust Core Engineer" };
          tester = { agent: "claude", model: options.testerModel || options.model || "claude-3-7-sonnet", persona: "Formal Verification & Invariants" };
          rationale = "Formal invariant modeling with AGY, zero-overhead memory-safe Rust with Codex, tamper auditing with Claude.";
        }

        if (options.model) {
          planner.model = options.plannerModel || options.model;
          coder.model = options.coderModel || options.model;
          tester.model = options.testerModel || options.model;
        }

        return {
          lead: "nio",
          planner,
          coder,
          tester,
          rationale,
        };
      },

      async create(options = {}) {
        let decision = null;
        let agents;
        if (!options.agents || options.agents === "auto") {
          decision = this.nioDecideSwarm(options.goal || "", options);
          agents = {
            planner: decision.planner.agent,
            coder: decision.coder.agent,
            tester: decision.tester.agent,
          };
        } else {
          agents = options.agents;
        }

        const session = await db.bridge.create({
          title: options.title || "Assemble Swarm Session",
          goal: options.goal || "",
          agent: agents.planner || "agy",
          model: options.model || (decision ? decision.planner.model : undefined),
          persona: decision ? decision.planner.persona : "lead-architect",
          modelTier: options.modelTier || options.model_tier || "strong",
          metadata: {
            swarm_type: "assemble",
            lead: "nio",
            agents,
            decision: decision || undefined,
            ...(options.metadata || {}),
          },
        });
        return {
          session,
          sessionId: session.id,
          lead: "nio",
          agents,
          decision,
        };
      },

      async plan(sessionId, prompt, tasks = []) {
        await db.bridge.appendTurn(sessionId, {
          agent: "nio_assemble_lead",
          model: "lead",
          summary: `Swarm plan created: ${prompt}`,
          filesTouched: [],
        });

        const createdTasks = [];
        for (const t of tasks) {
          const taskRecord = await db.tasks.create({
            title: t.title,
            prompt: t.prompt,
            priority: t.priority || "normal",
            sessionId,
            role: t.role || "coder",
            assignedAgent: t.assignedAgent || t.assigned_agent,
            dependencies: t.dependencies || [],
            metadata: t.metadata || {},
          });
          createdTasks.push(taskRecord);
        }
        return {
          sessionId,
          tasks: createdTasks,
          count: createdTasks.length,
        };
      },

      async claimTask(taskId, agentName) {
        return db.tasks.update(taskId, {
          status: "in_progress",
          assignedAgent: agentName,
          logLine: `Task claimed by ${agentName}`,
        });
      },

      async completeTask(taskId, options = {}) {
        const result = typeof options === "string" ? { output: options } : (options.result || options);
        return db.tasks.update(taskId, {
          status: "completed",
          progress: 100,
          result,
          logLine: options.logLine || options.log_line || "Task completed successfully",
        });
      },

      async failTask(taskId, error) {
        const errorMsg = error instanceof Error ? error.message : String(error);
        return db.tasks.update(taskId, {
          status: "failed",
          logLine: `Task failed: ${errorMsg}`,
        });
      },

      async status(sessionId) {
        const session = await db.bridge.get(sessionId);
        const tasksRes = await db.tasks.list({ sessionId });
        const tasks = tasksRes.items || [];
        const deadEndsRes = await db.bridge.getDeadEnds(sessionId);

        const pending = tasks.filter(t => t.data?.status === "pending").length;
        const inProgress = tasks.filter(t => t.data?.status === "in_progress").length;
        const completed = tasks.filter(t => t.data?.status === "completed").length;
        const failed = tasks.filter(t => t.data?.status === "failed").length;

        return {
          sessionId,
          title: session.data?.title || session.title,
          goal: session.data?.goal || session.goal,
          activeAgent: session.data?.active_agent,
          totalTasks: tasks.length,
          pending,
          inProgress,
          completed,
          failed,
          allCompleted: tasks.length > 0 && completed === tasks.length,
          deadEndsCount: deadEndsRes.count || (deadEndsRes.dead_ends ? deadEndsRes.dead_ends.length : 0),
          tasks,
        };
      },
    };
  }

  get events() {
    const db = this;
    return {
      async publish(name, data = {}) {
        return db._request("/api/v1/events", "POST", { name, data });
      },
    };
  }

  get ledger() {
    const db = this;
    return {
      async verify() {
        return db._request("/api/v1/ledger/verify", "GET");
      },
      async root() {
        return db._request("/api/v1/ledger/root", "GET");
      },
    };
  }
}

function createNioDB(options = {}) {
  return new NioDB(options);
}

export { NioDB, createNioDB };
