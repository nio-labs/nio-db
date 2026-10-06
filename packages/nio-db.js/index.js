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
  }

  async _request(path, method = "GET", body = null, extraHeaders = {}) {
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

    const response = await fetch(url, {
      method,
      headers,
      body: body !== null && method !== "GET" ? JSON.stringify(body) : undefined,
    });

    if (!response.ok) {
      let errorData;
      try {
        errorData = await response.json();
      } catch {
        errorData = { error: { message: response.statusText, status: response.status } };
      }
      const err = new Error(errorData?.error?.message || `Request failed with status ${response.status}`);
      err.status = response.status;
      err.data = errorData;
      throw err;
    }

    if (response.status === 204) {
      return null;
    }
    return response.json();
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
        return db._request("/api/v1/records", "POST", payload, extraHeaders);
      },

      async find(options = {}) {
        let path = `/api/v1/records?type=${encodeURIComponent(name)}`;
        if (options.limit) path += `&limit=${encodeURIComponent(options.limit)}`;
        if (options.cursor) path += `&cursor=${encodeURIComponent(options.cursor)}`;
        return db._request(path, "GET");
      },

      async get(id) {
        return db._request(`/api/v1/records/${encodeURIComponent(id)}`, "GET");
      },

      async update(id, data, options = {}) {
        const payload = data && data.data ? data : { data };
        return db._request(`/api/v1/records/${encodeURIComponent(id)}`, "PATCH", payload);
      },

      async delete(id) {
        return db._request(`/api/v1/records/${encodeURIComponent(id)}`, "DELETE");
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

  async query(sql) {
    return this._request("/api/v1/query", "POST", { sql });
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
          model_tier: options.modelTier || options.model_tier || "strong",
          metadata: options.metadata || {},
        };
        return db._request("/api/v1/sessions", "POST", payload);
      },

      async get() {
        return db._request(`/api/v1/sessions/${encodeURIComponent(sessionId)}`, "GET");
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

  get tasks() {
    const db = this;
    return {
      async list() {
        return db._request("/api/v1/tasks", "GET");
      },

      async create(options = {}) {
        const payload = {
          title: options.title || "Task",
          prompt: options.prompt || "",
          priority: options.priority || "normal",
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
          log_line: updates.logLine || updates.log_line,
          result: updates.result,
        };
        return db._request(`/api/v1/tasks/${encodeURIComponent(id)}`, "PATCH", payload);
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

module.exports = {
  NioDB,
  createNioDB,
};
