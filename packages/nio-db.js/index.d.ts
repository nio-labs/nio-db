export interface NioDBOptions {
  url?: string;
  token?: string | null;
  workspaceId?: string | null;
}

export interface InsertOptions {
  ttl?: number;
  idempotencyKey?: string;
}

export interface FindOptions {
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
  get(id: string): Promise<any>;
  update(id: string, data: Record<string, any>, options?: any): Promise<any>;
  delete(id: string): Promise<{ success: boolean; deleted: boolean; id: string }>;
  insertMany(records: Array<Record<string, any>>, options?: any): Promise<{ success: boolean; inserted: number; records: any[] }>;
  bulkInsert(records: Array<Record<string, any>>, options?: any): Promise<{ success: boolean; inserted: number; records: any[] }>;
  updateMany(records: Array<{ id: string; data?: Record<string, any>; [key: string]: any }>, options?: any): Promise<{ success: boolean; updated: number; records: any[] }>;
  bulkUpdate(records: Array<{ id: string; data?: Record<string, any>; [key: string]: any }>, options?: any): Promise<{ success: boolean; updated: number; records: any[] }>;
  deleteMany(ids: string[] | Array<{ id: string }>, options?: any): Promise<{ success: boolean; deleted: number; deleted_ids: string[] }>;
  bulkDelete(ids: string[] | Array<{ id: string }>, options?: any): Promise<{ success: boolean; deleted: number; deleted_ids: string[] }>;
  searchVector(options: VectorSearchOptions): Promise<{ items: any[]; count: number }>;
}

export interface SessionClient {
  create(options: CreateSessionOptions): Promise<any>;
  get(): Promise<any>;
  appendTurn(options: AppendTurnOptions): Promise<{ success: boolean; session_id: string; turn: any }>;
  getManifest(): Promise<{ session_id: string; title: string; goal: string; turn_count: number; manifest_text: string }>;
  logDeadEnd(options: DeadEndOptions): Promise<any>;
  getDeadEnds(): Promise<{ session_id: string; dead_ends: any[]; count: number }>;
}

export interface TasksClient {
  list(): Promise<{ items: any[]; count: number }>;
  create(options: CreateTaskOptions): Promise<any>;
  get(id: string): Promise<any>;
  update(id: string, updates: UpdateTaskOptions): Promise<any>;
}

export interface EventsClient {
  publish(name: string, data?: Record<string, any>): Promise<any>;
}

export interface SessionsClient {
  list(): Promise<{ items: any[]; count: number }>;
  create(options: CreateSessionOptions): Promise<any>;
  get(sessionId: string): SessionClient;
}

export class NioDB {
  constructor(options?: NioDBOptions);
  collection(name: string): CollectionClient;
  query(sql: string): Promise<{ items: any[]; metrics?: any }>;
  vacuum(): Promise<{ success: boolean; stats: VacuumStats }>;
  getTools(): Promise<{ tools: any[] }>;
  readonly sessions: SessionsClient;
  session(sessionId: string): SessionClient;
  readonly tasks: TasksClient;
  readonly events: EventsClient;
}

export function createNioDB(options?: NioDBOptions): NioDB;
