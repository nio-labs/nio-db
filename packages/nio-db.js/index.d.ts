export interface RequestOptions { timeoutMs?: number; signal?: AbortSignal; }
export interface CollectionConstraints {
  unique?: string[];
  references?: Array<{ field: string; collection: string; target_field: string }>;
  lease?: { field: string; prefix: string } | null;
}
export interface LeaseProof { resource: string; owner: string; fence: number; }
export interface Lease extends LeaseProof { expires_at: number; }
export type Mutation =
  | { action: "create"; collection: string; data: Record<string, unknown> }
  | { action: "update"; id: string; data: Record<string, unknown>; expected_revision: number }
  | { action: "delete"; id: string; expected_revision: number };
export interface MutationRequest { idempotency_key: string; operations: Mutation[]; leases?: LeaseProof[]; }
export interface MutationRecord { id: string; workspace_id: string; kind: string; data: Record<string, unknown>; revision: number; created_at: string; updated_at: string; }
export interface MutationResult { records: MutationRecord[]; deleted: string[]; }
export interface DeletionJobStatus { id: string; root_id: string; status: "pending" | "complete"; deleted: number; total: number; created_at_ms: number; }

export interface NioDBOptions {
  url?: string;
  token?: string | null;
  workspaceId?: string | null;
  timeoutMs?: number;
  maxResponseBytes?: number;
  fetch?: typeof fetch;
}

export interface InsertOptions extends RequestOptions {
  ttl?: number;
  idempotencyKey?: string;
}

export interface FindOptions extends RequestOptions {
  limit?: number;
  cursor?: string;
}

export interface VectorSearchOptions {
  vector: number[];
  topK?: number;
  top_k?: number;
  minScore?: number;
  min_score?: number;
}

export interface CreateSessionOptions {
  title: string;
  goal?: string;
  agent?: string;
  model?: string;
  persona?: string;
  modelTier?: "fast" | "strong" | "free" | "local" | string;
  model_tier?: string;
  metadata?: Record<string, any>;
}

export interface AppendTurnOptions {
  agent: string;
  model: string;
  summary?: string;
  filesTouched?: string[];
  files_touched?: string[];
  tokens?: {
    prompt?: number;
    completion?: number;
    cached?: number;
    total?: number;
    cost_usd?: number;
  } | null;
  costUsd?: number;
  cost_usd?: number;
}

export interface DeadEndOptions {
  hypothesis: string;
  reason: string;
  agent?: string;
}

export interface CreateTaskOptions {
  title: string;
  prompt: string;
  priority?: "low" | "normal" | "high" | "urgent";
  metadata?: Record<string, any>;
}

export interface UpdateTaskOptions {
  status?: "pending" | "running" | "completed" | "failed";
  progress?: number;
  logLine?: string;
  log_line?: string;
  result?: any;
}

export interface VacuumStats {
  initial_bytes: number;
  compacted_bytes: number;
  active_artifacts: number;
  active_users: number;
  active_files: number;
  purged_expired: number;
}

export interface CollectionClient {
  insert(data: Record<string, any>, options?: InsertOptions): Promise<any>;
  find(options?: FindOptions): Promise<{ items: any[]; next_cursor?: string }>;
  get(id: string, options?: RequestOptions): Promise<any>;
  update(id: string, data: Record<string, any>, options?: any): Promise<any>;
  delete(id: string, options?: RequestOptions): Promise<{ success: boolean; deleted: boolean; id: string }>;
  insertMany(records: Array<Record<string, any>>, options?: any): Promise<{ success: boolean; inserted: number; records: any[] }>;
  bulkInsert(records: Array<Record<string, any>>, options?: any): Promise<{ success: boolean; inserted: number; records: any[] }>;
  updateMany(records: Array<{ id: string; data?: Record<string, any>; [key: string]: any }>, options?: any): Promise<{ success: boolean; updated: number; records: any[] }>;
  bulkUpdate(records: Array<{ id: string; data?: Record<string, any>; [key: string]: any }>, options?: any): Promise<{ success: boolean; updated: number; records: any[] }>;
  deleteMany(ids: string[] | Array<{ id: string }>, options?: any): Promise<{ success: boolean; deleted: number; deleted_ids: string[] }>;
  bulkDelete(ids: string[] | Array<{ id: string }>, options?: any): Promise<{ success: boolean; deleted: number; deleted_ids: string[] }>;
  searchVector(options: VectorSearchOptions): Promise<{ items: any[]; count: number }>;
  watch(callback: (event: { event: string; collection?: string; record?: any; id?: string }) => void): () => void;
}

export interface SwitchSessionOptions {
  toAgent?: string;
  agent?: string;
  to_agent?: string;
  reason?: string;
  persona?: string;
  summaryOfWork?: string;
  summary_of_work?: string;
}

export interface UpdateSessionOptions {
  title?: string;
  goal?: string;
  status?: string;
  activeAgent?: string;
  active_agent?: string;
  persona?: string;
  modelTier?: string;
  model_tier?: string;
  metadata?: Record<string, any>;
}

export interface AgentAdapter {
  name: string;
  formatPrompt(manifest: any, instructions: string): string;
}

export interface BridgeClient {
  create(options?: CreateSessionOptions): Promise<any>;
  get(sessionId: string): Promise<any>;
  update(sessionId: string, updates: UpdateSessionOptions): Promise<any>;
  switch(sessionId: string, options: string | SwitchSessionOptions): Promise<{
    success: boolean;
    session_id: string;
    previous_agent: string;
    active_agent: string;
    reason: string;
    manifest: any;
  }>;
  appendTurn(sessionId: string, options: AppendTurnOptions): Promise<{ success: boolean; session_id: string; turn: any }>;
  getManifest(sessionId: string): Promise<{ session_id: string; title: string; goal: string; turn_count: number; manifest_text: string }>;
  logDeadEnd(sessionId: string, options: DeadEndOptions): Promise<any>;
  getDeadEnds(sessionId: string): Promise<{ session_id: string; dead_ends: any[]; count: number }>;
  adapters: {
    agy: AgentAdapter;
    claude: AgentAdapter;
    codex: AgentAdapter;
    cursor: AgentAdapter;
  };
}

export interface AssembleTask {
  title: string;
  prompt: string;
  priority?: "low" | "normal" | "high" | "urgent";
  role?: string;
  assignedAgent?: string;
  assigned_agent?: string;
  dependencies?: string[];
  metadata?: Record<string, any>;
}

export interface AssembleCreateOptions {
  title?: string;
  goal?: string;
  model?: string;
  plannerModel?: string;
  coderModel?: string;
  testerModel?: string;
  agents?: "auto" | {
    planner?: string;
    coder?: string;
    tester?: string;
    [role: string]: string | undefined;
  };
  modelTier?: string;
  model_tier?: string;
  metadata?: Record<string, any>;
}

export interface AssembleStatus {
  sessionId: string;
  title?: string;
  goal?: string;
  activeAgent?: string;
  totalTasks: number;
  pending: number;
  inProgress: number;
  completed: number;
  failed: number;
  allCompleted: boolean;
  deadEndsCount: number;
  tasks: any[];
}

export interface AssembleClient {
  nioDecideSwarm(goal?: string, options?: AssembleCreateOptions): {
    lead: string;
    planner: { agent: string; model: string; persona: string };
    coder: { agent: string; model: string; persona: string };
    tester: { agent: string; model: string; persona: string };
    rationale: string;
  };
  create(options?: AssembleCreateOptions): Promise<{
    session: any;
    sessionId: string;
    lead?: string;
    agents: Record<string, string>;
    decision?: any;
  }>;
  plan(sessionId: string, prompt: string, tasks?: AssembleTask[]): Promise<{
    sessionId: string;
    tasks: any[];
    count: number;
  }>;
  claimTask(taskId: string, agentName: string): Promise<any>;
  completeTask(taskId: string, options?: any): Promise<any>;
  failTask(taskId: string, error: any): Promise<any>;
  status(sessionId: string): Promise<AssembleStatus>;
}

export interface SessionClient {
  create(options: CreateSessionOptions): Promise<any>;
  get(): Promise<any>;
  update(updates: UpdateSessionOptions): Promise<any>;
  switch(options: string | SwitchSessionOptions): Promise<any>;
  appendTurn(options: AppendTurnOptions): Promise<{ success: boolean; session_id: string; turn: any }>;
  getManifest(): Promise<{ session_id: string; title: string; goal: string; turn_count: number; manifest_text: string }>;
  logDeadEnd(options: DeadEndOptions): Promise<any>;
  getDeadEnds(): Promise<{ session_id: string; dead_ends: any[]; count: number }>;
}

export interface TasksListOptions {
  sessionId?: string;
  session_id?: string;
  status?: string;
  assignedAgent?: string;
  assigned_agent?: string;
  role?: string;
}

export interface TasksClient {
  list(options?: TasksListOptions): Promise<{ items: any[]; count: number }>;
  create(options: CreateTaskOptions): Promise<any>;
  get(id: string): Promise<any>;
  update(id: string, updates: UpdateTaskOptions): Promise<any>;
}

export interface EventsClient {
  publish(name: string, data?: Record<string, any>): Promise<any>;
}

export interface LedgerReport {
  verified: boolean;
  total_frames: number;
  root_hash: string;
  genesis_hash: string;
  latest_seq: number;
  tampering_detected: boolean;
  error?: string | null;
}

export interface LedgerClient {
  verify(): Promise<LedgerReport>;
  root(): Promise<{ root_hash: string; latest_seq: number }>;
}

export interface SessionsClient {
  list(): Promise<{ items: any[]; count: number }>;
  create(options: CreateSessionOptions): Promise<any>;
  get(sessionId: string): SessionClient;
}

export class NioDB {
  constructor(options?: NioDBOptions);
  collection(name: string): CollectionClient;
  query(sql: string, parameters?: unknown[], options?: RequestOptions): Promise<{ items: any[]; metrics?: any; engine?: string }>;
  configureCollection(collection: string, constraints: CollectionConstraints, options?: RequestOptions): Promise<{ constraints: CollectionConstraints }>;
  getCollectionConstraints(collection: string, options?: RequestOptions): Promise<{ constraints: CollectionConstraints | null }>;
  mutate(request: MutationRequest, options?: RequestOptions): Promise<MutationResult>;
  getPersistenceStatus(options?: RequestOptions): Promise<{ active_receipts: number; receipt_limit: number; receipt_retention_days: number; spent_keys: number; active_leases: number; active_lease_limit: number; pending_deletion_jobs: number; indexed_keys: number }>;
  startDeletionJob(rootId: string, idempotencyKey: string, options?: RequestOptions): Promise<{ job: DeletionJobStatus }>;
  getDeletionJob(id: string, options?: RequestOptions): Promise<{ job: DeletionJobStatus }>;
  getLease(resource: string, options?: RequestOptions): Promise<{ lease: Lease | null; active: boolean }>;
  acquireLease(resource: string, owner: string, ttlMs?: number, options?: RequestOptions): Promise<{ lease: Lease }>;
  renewLease(proof: LeaseProof, ttlMs?: number, options?: RequestOptions): Promise<{ lease: Lease }>;
  releaseLease(proof: LeaseProof, options?: RequestOptions): Promise<{ lease: Lease }>;
  vacuum(): Promise<{ success: boolean; stats: VacuumStats }>;
  getTools(): Promise<{ tools: any[] }>;
  bulk(operations: any): Promise<any>;
  mcp(method: string, params?: Record<string, any>): Promise<any>;
  readonly sessions: SessionsClient;
  session(sessionId: string): SessionClient;
  readonly bridge: BridgeClient;
  readonly assemble: AssembleClient;
  readonly tasks: TasksClient;
  readonly events: EventsClient;
  readonly ledger: LedgerClient;
}

export function createNioDB(options?: NioDBOptions): NioDB;


