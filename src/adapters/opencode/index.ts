/**
 * True-Mem OpenCode Adapter
 */
const BUILD_TIME = "2026-02-23T09:45:00.000Z";

import type { PluginInput, Hooks, Event, Message, Part } from '../../types.js';
import type { PsychMemConfig, MemoryUnit, RoleAwareContext, RoleAwareLine, MessageRole } from '../../types.js';
import { DEFAULT_CONFIG } from '../../config.js';
import { migrateIfNeeded } from '../../config/migration.js';
import { MemoryDatabase, createMemoryDatabase } from '../../storage/database.js';
import { log } from '../../logger.js';
import {
  shouldStoreMemory,
  classifyWithExplicitIntent,
  classifyWithRoleAwareness,
  calculateRoleWeightedScore,
} from '../../memory/classifier.js';
import { matchAllPatterns, hasGlobalScopeKeyword, isMemoryListRequest, detectProjectSignals, extractProjectTerms, shouldBeProjectScope } from '../../memory/patterns.js';
import { setLastInjectedMemories, getLastInjectedMemories } from '../../state.js';
import { getExtractionQueue } from '../../extraction/queue.js';
import { registerShutdownHandler } from '../../shutdown.js';
import { parseConversationLines } from '../../memory/role-patterns.js';
import { getAtomicMemories, wrapMemories, selectMemoriesForInjection, type InjectionState } from './injection.js';
import { getVersion } from '../../utils/version.js';
import { EmbeddingService } from '../../memory/embeddings-nlp.js';
import { 
  markSessionCreated, 
  hasInjected, 
  markInjected, 
  ensureSessionTracked,
  shouldInjectResumedSession 
} from './injection-tracker.js';

// V2 child-session registry (populated by the V2 adapter from session.created
// events that carry a parentID). Used to skip sub-agent extraction when the V2
// session-id format does not include the legacy "-task-" substring.
const childSessionIds = new Set<string>();

export function markChildSession(sessionId: string): void {
  if (!sessionId) return;
  childSessionIds.add(sessionId);
  if (childSessionIds.size > 4096) {
    let removed = 0;
    for (const id of childSessionIds) {
      childSessionIds.delete(id);
      if (++removed >= 2048) break;
    }
  }
}

// Per-instance disposal registry.
//
// Every createTrueMemoryPlugin() call opens its OWN SQLite connection, and the
// host (OpenCode V2) can instantiate the plugin again for the same location
// (hot reload / takeover). executeShutdown() is never called, so without this
// registry each reload would leak one connection. Disposal is scoped to the
// instance it belongs to: it closes only THAT instance's connection and never
// one a newer instance is using.
const instanceDisposers = new WeakMap<Hooks, () => void>();

/**
 * Dispose a plugin instance created by createTrueMemoryPlugin().
 * Releases the instance's database connection. Safe to call twice.
 */
export function disposeTrueMemoryPlugin(hooks: Hooks): void {
  const disposer = instanceDisposers.get(hooks);
  if (!disposer) return;
  disposer();
}

// Debounce state for message.updated events
let messageDebounceTimer: ReturnType<typeof setTimeout> | null = null;
let pendingMessageEvent: { properties: unknown } | null = null;

// Global extraction debounce to prevent rapid-fire duplicate extractions
let lastExtractionTime = 0;
const MIN_EXTRACTION_INTERVAL = 2000; // 2 seconds minimum between extractions

// Bounded retry for extraction jobs dropped by the global debounce.
// A debounced idle may carry the LAST turn's messages, so instead of
// silently dropping the job we re-enqueue it once after the debounce window,
// for a bounded number of attempts (never an infinite loop).
const EXTRACTION_RETRY_DELAY_MS = MIN_EXTRACTION_INTERVAL + 500;
const MAX_EXTRACTION_RETRIES = 3;
const extractionRetryAttempts = new Map<string, number>();
const extractionRetryTimers = new Map<string, ReturnType<typeof setTimeout>>();

/**
 * Clear any pending debounce-retry for a session (both the counter and the timer).
 */
function clearExtractionRetry(sessionId: string): void {
  extractionRetryAttempts.delete(sessionId);
  const timer = extractionRetryTimers.get(sessionId);
  if (timer) {
    clearTimeout(timer);
    extractionRetryTimers.delete(sessionId);
  }
}

/**
 * Schedule ONE delayed re-enqueue of a debounced extraction job.
 * Bounded by MAX_EXTRACTION_RETRIES per session, and at most one pending
 * retry timer per session, so this can never loop forever.
 */
function scheduleExtractionRetry(
  state: TrueMemoryAdapterState,
  sessionId: string
): void {
  const attempts = extractionRetryAttempts.get(sessionId) ?? 0;

  if (attempts >= MAX_EXTRACTION_RETRIES) {
    log(`Skipping extraction: retry limit reached for ${sessionId} (${attempts}/${MAX_EXTRACTION_RETRIES})`);
    clearExtractionRetry(sessionId);
    return;
  }

  // One pending retry per session: extra debounced idles are absorbed here.
  if (extractionRetryTimers.has(sessionId)) {
    log(`Skipping extraction: retry already pending for ${sessionId}`);
    return;
  }

  // Keep the retry bookkeeping bounded (defensive; entries are removed on
  // success or when the retry limit is reached).
  if (extractionRetryAttempts.size > 1024) {
    for (const key of extractionRetryAttempts.keys()) {
      extractionRetryAttempts.delete(key);
      break;
    }
  }

  const attempt = attempts + 1;
  extractionRetryAttempts.set(sessionId, attempt);
  log(`Extraction debounced: scheduling retry ${attempt}/${MAX_EXTRACTION_RETRIES} for ${sessionId} in ${EXTRACTION_RETRY_DELAY_MS}ms`);

  const timer = setTimeout(() => {
    extractionRetryTimers.delete(sessionId);
    log(`Re-enqueueing debounced extraction for ${sessionId} (retry ${attempt}/${MAX_EXTRACTION_RETRIES})`);
    queueExtractionJob(state, sessionId);
  }, EXTRACTION_RETRY_DELAY_MS);

  // Do not hold the event loop open just for a retry.
  if (typeof (timer as { unref?: () => void }).unref === 'function') {
    (timer as { unref: () => void }).unref();
  }
  extractionRetryTimers.set(sessionId, timer);
}

// Cache for context extraction (avoid repeated API calls)
const contextCache = new Map<string, { context: string; timestamp: number }>();
const CACHE_TTL = 5000; // 5 seconds
const MAX_CONTEXT_CACHE_ENTRIES = 100;

// Persist worktree across plugin restarts (OpenCode lifecycle issue)
// Using file-based persistence because module-level variables don't survive plugin reloads
import { readFileSync, writeFileSync, existsSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';
import type { StorageLocation } from '../../types.js';
import { getStorageDir } from '../../config/paths.js';

/**
 * Get worktree cache file path based on storage location.
 * Note: Worktree cache is hardcoded to legacy location for backward compatibility
 * with existing users. The worktree cache persists across plugin restarts and
 * contains OpenCode worktree information that should survive storage location changes.
 */
function getWorktreeCacheFile(storageLocation: StorageLocation = 'legacy'): string {
  return join(getStorageDir(storageLocation), '.worktree-cache');
}

function getPersistedWorktree(): string | null {
  // Use legacy location for backward compatibility
  const worktreeCacheFile = getWorktreeCacheFile('legacy');
  try {
    if (existsSync(worktreeCacheFile)) {
      const cached = readFileSync(worktreeCacheFile, 'utf-8').trim();
      if (cached && cached !== '/' && cached !== '\\' && cached.length > 0) {
        return cached;
      }
    }
  } catch (err) {
    // Silently ignore - will use ctx values instead
  }
  return null;
}

function setPersistedWorktree(worktree: string): void {
  // Use legacy location for backward compatibility
  const worktreeCacheFile = getWorktreeCacheFile('legacy');
  try {
    writeFileSync(worktreeCacheFile, worktree, 'utf-8');
  } catch (err) {
    // Silently ignore - non-critical feature
  }
}

/**
 * Extract query context from conversation messages with caching
 * Used for semantic memory retrieval when embeddings are enabled
 */
async function extractQueryContextFromInput(
  client: PluginInput['client'],
  sessionId: string | undefined
): Promise<string> {
  if (!sessionId) return '';
  
  // Check cache
  const cached = contextCache.get(sessionId);
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    log('Using cached context');
    return cached.context;
  }
  
  try {
    // Use the same API as processSessionIdle
    const response = await client.session.messages({ path: { id: sessionId } });
    if (response.error || !response.data) return '';
    
    const messages = response.data;
    const recentMessages = messages.slice(-5); // Last 5 messages
    
    const contextParts: string[] = [];
    for (const msg of recentMessages) {
      for (const part of msg.parts) {
        if (part.type === 'text' && 'text' in part) {
          contextParts.push((part as { text: string }).text);
        }
      }
    }
    
    const fullContext = contextParts.join(' | ');
    const truncatedContext = fullContext.slice(-500); // Truncate to 500 chars
    
    // Update cache - sweep expired entries first so the map cannot grow
    // one entry per session forever (entries were only TTL-checked on read).
    const now = Date.now();
    for (const [key, entry] of contextCache) {
      if (now - entry.timestamp >= CACHE_TTL) {
        contextCache.delete(key);
      }
    }
    contextCache.set(sessionId, { context: truncatedContext, timestamp: now });

    // Hard cap for bursts that fit inside the TTL window (insertion order = oldest first).
    while (contextCache.size > MAX_CONTEXT_CACHE_ENTRIES) {
      const oldestKey = contextCache.keys().next().value;
      if (oldestKey === undefined) break;
      contextCache.delete(oldestKey);
    }
    
    return truncatedContext;
  } catch (error) {
    log('Failed to extract query context:', error);
    return '';
  }
}

/**
 * Check if enough time has passed since last extraction
 * Prevents race conditions when multiple triggers fire in quick succession
 */
function canExtract(): boolean {
  const now = Date.now();
  if (now - lastExtractionTime < MIN_EXTRACTION_INTERVAL) {
    log(`Skipping extraction: too soon after last extraction (${now - lastExtractionTime}ms < ${MIN_EXTRACTION_INTERVAL}ms)`);
    return false;
  }
  return true;
}

// Message container type matching SDK response
interface MessageContainer {
  info: Message;
  parts: Part[];
}

// Session ID extraction helper
function getSessionIdFromEvent(properties?: Record<string, unknown>): string | undefined {
  if (!properties) return undefined;
  const info = properties.info as Record<string, unknown> | undefined;
  if (info && typeof info.id === 'string') return info.id;
  if (typeof properties.sessionID === 'string') return properties.sessionID;
  if (typeof properties.id === 'string') return properties.id;
  return undefined;
}

// Sub-agent detection helper
function isSubAgentSession(sessionId: string): boolean {
  // V1 heuristic: sub-agent sessions contained "-task-" in the ID.
  // V2: also consult the child-session registry (sessions created with a parentID).
  return sessionId.includes('-task-') || childSessionIds.has(sessionId);
}

// Debounce helper for message.updated events
function debounceMessageUpdate(
  state: TrueMemoryAdapterState,
  eventProps: unknown,
  handler: (state: TrueMemoryAdapterState, props: Record<string, unknown> | undefined) => Promise<void>
) {
  pendingMessageEvent = { properties: eventProps };

  if (messageDebounceTimer) {
    clearTimeout(messageDebounceTimer);
  }

  messageDebounceTimer = setTimeout(() => {
    if (pendingMessageEvent) {
      handler(state, pendingMessageEvent.properties as Record<string, unknown> | undefined)
        .catch(err => log(`Message processing error: ${err}`));
    }
    pendingMessageEvent = null;
    messageDebounceTimer = null;
  }, 500); // 500ms debounce
}

// Adapter state
interface TrueMemoryAdapterState {
  db: MemoryDatabase;
  config: PsychMemConfig;
  currentSessionId: string | null;
  worktree: string;
  client: PluginInput['client'];
}

/**
 * Create OpenCode plugin hooks
 */
export async function createTrueMemoryPlugin(
  ctx: PluginInput,
  configOverrides: Partial<PsychMemConfig> = {}
): Promise<Hooks> {
  log('createTrueMemoryPlugin called');
  
  // Run migration if needed (idempotent)
  migrateIfNeeded();
  
  const config: PsychMemConfig = {
    ...DEFAULT_CONFIG,
    ...configOverrides,
  };
  
  // Initialize database
  const db = await createMemoryDatabase(config);
  log('Database initialized');

  // Per-instance disposal: the V2 adapter calls disposeTrueMemoryPlugin() when
  // the plugin instance is torn down, so the connection is released instead of
  // leaking one on every hot reload.
  const disposeInstance = () => {
    try {
      db.close();
    } catch (err) {
      log(`Database dispose error: ${err}`);
    }
  };

  // Register shutdown handler for embeddings
  registerShutdownHandler('embeddings', () => {
    const embeddingService = EmbeddingService.getInstance();
    embeddingService.cleanup();
  });

  // Resolve project root with explicit validation
  // P3-1: Prevent falling back to "/" which matches all memories
  const isValidPath = (path: string | undefined): boolean => {
    return !!(path && path !== '/' && path !== '\\' && path.trim().length > 0);
  };

  // FIX #1: Invert cache priority - ctx > directory > cache (fallback)
  // This ensures switching projects works correctly
  // Previous logic gave priority to cache, causing project-scoped memories to leak
  let worktree: string;

  if (isValidPath(ctx.worktree)) {
    worktree = ctx.worktree;
    setPersistedWorktree(worktree);
    log(`Worktree from context: ${worktree}`);
  } else if (isValidPath(ctx.directory)) {
    worktree = ctx.directory;
    setPersistedWorktree(worktree);
    log(`Worktree from directory: ${worktree}`);
  } else {
    const persistedWorktree = getPersistedWorktree();
    if (persistedWorktree && isValidPath(persistedWorktree)) {
      worktree = persistedWorktree;
      log(`Worktree from cache (fallback): ${worktree}`);
    } else {
      worktree = `unknown-project-${Date.now()}`;
      log(`WARNING: Could not determine worktree, using fallback`);
    }
  }

  // FIX #3: Cache invalidation logging - detect project changes for debugging
  const previousWorktree = getPersistedWorktree();
  if (previousWorktree && previousWorktree !== worktree && !worktree.startsWith('unknown-project')) {
    log(`Project changed: ${previousWorktree} -> ${worktree}`);
  }
  
  const state: TrueMemoryAdapterState = {
    db,
    config,
    currentSessionId: null,
    worktree,
    client: ctx.client,
  };

  log(`True-Mem initialized — worktree=${worktree}, maxMemories=${config.maxMemories}`);

  // Extract project name and create professional startup message
  const projectName = worktree.split(/[/\\]/).pop() || 'Unknown';
  const version = getVersion();
  const startupMessage = `🧠 True-Mem: Plugin loaded successfully | v${version} [${BUILD_TIME}] | Mode: Jaccard Similarity | Project: ${projectName}`;

  // Log to file-based logger only (to avoid overwriting OpenCode TUI during lazy initialization)
  log(startupMessage);

  const hooks: Hooks = {
    event: async ({ event }) => {
      // Skip noisy events
      const silentEvents = new Set(['message.part.delta', 'message.part.updated', 'session.diff']);
      if (silentEvents.has(event.type)) return;

      log(`Event: ${event.type}`);
      const sessionId = getSessionIdFromEvent(event.properties);

      switch (event.type) {
        case 'session.created':
          await handleSessionCreated(state, sessionId);
          // Track session for injection mode 1
          if (sessionId) {
            markSessionCreated(sessionId);
          }
          break;
        case 'session.idle':
          // Add extraction job to queue for sequential processing
          queueExtractionJob(state, sessionId);
          break;
        case 'session.deleted':
        case 'session.error':
          await handleSessionEnd(state, event.type, sessionId);
          break;
        case 'message.updated':
          if (state.config.opencode.extractOnMessage) {
            // Debounce message updates to avoid blocking UI
            debounceMessageUpdate(state, event.properties, handleMessageUpdated);
          }
          break;
        case 'server.instance.disposed':
          // OpenCode is disposing the server instance - persist worktree to file for next init
          if (state.worktree && !state.worktree.startsWith('unknown-project')) {
            setPersistedWorktree(state.worktree);
          }
          log('Server instance disposed - worktree preserved for next init');
          break;
      }
    },

    "chat.message": async (input, output) => {
      // Extract user text from parts
      const textParts: string[] = [];
      for (const part of output.parts) {
        if (part.type === 'text' && 'text' in part) {
          textParts.push((part as { text: string }).text);
        }
      }
      const userText = textParts.join(' ');

      if (!userText) return;

      // Check if this is a memory list request
      if (isMemoryListRequest(userText)) {
        const memories = getLastInjectedMemories();

        if (memories.length > 0) {
          const memoryList = formatMemoryListForResponse(memories);

          // Find the first text part and replace it with a new part containing the memory list
          // Using a new object prevents mutation persistence across prompts
          const firstTextPartIndex = output.parts.findIndex(part => part.type === 'text' && 'text' in part);
          if (firstTextPartIndex !== -1) {
            const originalPart = output.parts[firstTextPartIndex]!;
            if ('text' in originalPart) {
              (output.parts[firstTextPartIndex] as any) = {
                ...originalPart,
                text: `${originalPart.text}\n\n[TRUE-MEM] Ecco le memorie iniettate in questo prompt:\n${memoryList}`
              };
            }
          }

          log(`Memory list request detected: injected ${memories.length} memories`);
        } else {
          // Find the first text part and replace it with a new part containing the no-memories message
          const firstTextPartIndex = output.parts.findIndex(part => part.type === 'text' && 'text' in part);
          if (firstTextPartIndex !== -1) {
            const originalPart = output.parts[firstTextPartIndex]!;
            if ('text' in originalPart) {
              (output.parts[firstTextPartIndex] as any) = {
                ...originalPart,
                text: `${originalPart.text}\n\n[TRUE-MEM] Nessuna memoria iniettata in questo prompt.`
              };
            }
          }
        }
      }
    },

    'tool.execute.before': async (input, output) => {
      const toolInput = input as { tool: string; sessionID: string; callID: string };
      const toolName = toolInput.tool;
      log(`tool.execute.before: ${toolName}`);

      // Only inject for task and background_task tools
      if (toolName !== 'task' && toolName !== 'background_task') {
        return;
      }

      // NEW: Check sub-agent mode config
      const subAgentMode = state.config.opencode.injection?.subAgentMode ?? 1;
      if (subAgentMode === 0) {
        log('Sub-agent injection disabled by config');
        return;
      }

      // Extract prompt from output args
      const outputWithArgs = output as { args: { prompt?: string } };
      const originalPrompt = outputWithArgs.args?.prompt;
      if (!originalPrompt) {
        return;
      }

      // Retrieve relevant memories using atomic injection
      try {
        const injectionState: InjectionState = {
          db: state.db,
          worktree: state.worktree,
        };

        const memories = await getAtomicMemories(injectionState, originalPrompt, 10);

        if (memories.length > 0) {
          const wrappedContext = wrapMemories(memories, state.worktree, 'project');

          // Update the prompt in output args
          outputWithArgs.args.prompt = `${wrappedContext}\n\n${originalPrompt}`;

          log(`Atomic injection: ${memories.length} memories injected for ${toolName}`);
        }
      } catch (error) {
        log(`Atomic injection failed for ${toolName}: ${error}`);
        // Continue without injection on error
      }
    },

    'tool.execute.after': async (input, output) => {
      log(`tool.execute.after: ${input.tool}`);

      // Only adopt the session id from a TOP-LEVEL session. Without this guard a
      // concurrent child/sub-agent session running in parallel would overwrite
      // the tracked id and all subsequent bookkeeping would target the sub-agent.
      if (!state.currentSessionId && input.sessionID && !isSubAgentSession(input.sessionID)) {
        state.currentSessionId = input.sessionID;
      }

      if (!state.currentSessionId) return;

      await handlePostToolUse(state, input, output);
    },

    'experimental.chat.system.transform': async (input, output) => {
      const sessionId = input.sessionID ?? state.currentSessionId ?? undefined;
      const injectionMode = state.config.opencode.injection?.mode ?? 0;

      // FIX #4: Runtime worktree validation - update if ctx changed mid-session
      // This handles the case where user switches projects within the same session
      // Note: ctx is from the outer plugin scope, not the input object
      if (isValidPath(ctx.worktree) && ctx.worktree !== state.worktree) {
        log(`Worktree changed mid-session: ${state.worktree} -> ${ctx.worktree}`);
        state.worktree = ctx.worktree;
        setPersistedWorktree(ctx.worktree);
      }
      
      // Ensure session is tracked
      if (sessionId) {
        ensureSessionTracked(sessionId);
      }

      // Mode 0: SESSION_START - Inject only once per session (default)
      if (injectionMode === 0 && sessionId) {
        if (hasInjected(sessionId)) {
          log(`Skipping injection: already injected for session ${sessionId.slice(0, 8)}...`);
          return;
        }
        
        // Check if this is a resumed session that already has memory context
        const shouldInject = await shouldInjectResumedSession(state.client, sessionId);
        if (!shouldInject) {
          markInjected(sessionId); // Mark immediately to prevent race condition
          log(`Skipping injection: resumed session already has memory context`);
          return;
        }
      }
      
      // Mode 1: ALWAYS - Continue with injection (legacy behavior)
      log(`Injecting memories (mode=${injectionMode})`);

      try {
        // Extract context from conversation (convert null to undefined for type safety)
        const sessionId = input.sessionID ?? state.currentSessionId ?? undefined;
        const queryContext = await extractQueryContextFromInput(
          state.client,
          sessionId
        );
        
        // Check if embeddings are enabled
        const embeddingsEnabled = process.env.TRUE_MEM_EMBEDDINGS === '1';
        
        // Use smart selection instead of getMemoriesByScope
        const allMemories = await selectMemoriesForInjection(
          state.db,
          state.worktree,
          queryContext,
          embeddingsEnabled,
          state.config.maxMemories,
          state.config.maxTokensForMemories
        );

        // Save to global state for "list memories" feature
        setLastInjectedMemories(allMemories);

        if (allMemories.length > 0) {
          const wrappedContext = wrapMemories(allMemories, state.worktree, 'global');

          // Handle system as string[] - append to the last element
          const systemArray = Array.isArray(output.system) ? output.system : [output.system];
          const lastElement = systemArray[systemArray.length - 1] || '';
          systemArray[systemArray.length - 1] = `${lastElement}\n\n${wrappedContext}`;

          output.system = systemArray;

          // Mark as injected after successful injection (mode 0 = session-start)
          if (injectionMode === 0 && sessionId) {
            markInjected(sessionId);
          }
          
          log(`Global injection: ${allMemories.length} memories injected into system prompt [embeddings=${embeddingsEnabled}]`);
        }
      } catch (error) {
        log(`Global injection failed: ${error}`);
        // Continue without injection on error
      }
    },

    'experimental.session.compacting': async (input, output) => {
      log('Compaction hook triggered');

      const sessionId = input.sessionID ?? state.currentSessionId;

      if (state.config.opencode.injectOnCompaction) {
        const memories = await getRelevantMemories(state, state.config.opencode.maxCompactionMemories);

        if (memories.length > 0) {
          const memoryContext = formatMemoriesForInjection(memories, state.worktree);
          output.prompt = buildCompactionPrompt(memoryContext);
          log(`Injected ${memories.length} memories into compaction`);
        } else {
          output.prompt = buildCompactionPrompt(null);
        }
      }
    },
  };

  // Bind this instance's disposer to its hooks object so the host can release
  // exactly this instance's resources.
  instanceDisposers.set(hooks, disposeInstance);

  return hooks;
}

// Queue helper for session idle processing
function queueExtractionJob(
  state: TrueMemoryAdapterState,
  sessionId?: string
): void {
  const queue = getExtractionQueue();

  queue.add({
    description: `session:${sessionId ?? state.currentSessionId}`,
    execute: async () => {
      await processSessionIdle(state, sessionId);
    },
  });
}

// Session handlers
async function handleSessionCreated(
  state: TrueMemoryAdapterState,
  sessionId?: string
): Promise<void> {
  if (!sessionId) return;

  state.currentSessionId = sessionId;
  log(`Session created: ${sessionId}`);

  // ✅ Sola creazione sessione - nessun maintenance bloccante
  state.db.createSession(sessionId, state.worktree, { agentType: 'opencode' });
}

async function processSessionIdle(
  state: TrueMemoryAdapterState,
  sessionId?: string
): Promise<void> {
  const effectiveSessionId = sessionId ?? state.currentSessionId;
  if (!effectiveSessionId) return;

  if (sessionId && !state.currentSessionId) {
    state.currentSessionId = sessionId;
  }

  // Skip extraction for sub-agent sessions to avoid duplicate extraction
  if (isSubAgentSession(effectiveSessionId)) {
    log(`Skipping extraction: sub-agent session detected (${effectiveSessionId})`);
    return;
  }

  // Self-heal: if 'session.created' was never delivered for this session, no
  // row exists, so getMessageWatermark() returns 0 and updateMessageWatermark()
  // silently no-ops -> the entire history would be re-extracted every turn.
  if (!state.db.getSession(effectiveSessionId)) {
    state.db.createSession(effectiveSessionId, state.worktree, { agentType: 'opencode' });
    log(`Self-healed missing session row: ${effectiveSessionId}`);
  }

  // Global debounce: prevent rapid-fire extractions from multiple triggers.
  // A debounced idle may carry the LAST turn's messages, so it must not be
  // dropped silently: re-enqueue it once (bounded) after the debounce window.
  if (!canExtract()) {
    scheduleExtractionRetry(state, effectiveSessionId);
    return;
  }

  // Debounce window is open again - drop any pending retry bookkeeping.
  clearExtractionRetry(effectiveSessionId);

  const storedWatermark = state.db.getMessageWatermark(effectiveSessionId);

  let messages: MessageContainer[];
  try {
    const response = await state.client.session.messages({ path: { id: effectiveSessionId } });
    if (response.error) {
      log(`Failed to fetch messages: ${response.error}`);
      return;
    }
    messages = response.data ?? [];
  } catch (error) {
    log(`Failed to fetch messages: ${error}`);
    return;
  }

  // The host can prune the transcript (e.g. after compaction), so the message
  // count can drop BELOW the stored watermark. Comparing against the stale
  // value would silently skip extraction forever. Rescan from the start and
  // let the per-pass text dedupe collapse anything already seen.
  let watermark = storedWatermark;
  if (messages && messages.length < storedWatermark) {
    log(`Watermark reset: transcript pruned (${storedWatermark} > ${messages.length} messages), rescanning from 0`);
    watermark = 0;
  }

  if (!messages || messages.length <= watermark) return;

  const newMessages = messages.slice(watermark);
  const { text: conversationText, lines: roleLines } = extractConversationTextWithRoles(newMessages);

  log('Debug: Clean conversation text (start):', conversationText.slice(0, 200));
  log('Debug: Role-aware lines extracted:', roleLines.length);

  if (!conversationText.trim()) {
    state.db.updateMessageWatermark(effectiveSessionId, messages.length);
    return;
  }

  // Check for injection markers as a final safety net before processing
  const injectionMarkers = [
    /## Relevant Memories from Previous Sessions/i,
    /### User Preferences & Constraints/i,
    /### .* Context/i,
    /## Compaction Instructions/i,
    /\[LTM\]/i,
    /\[STM\]/i,
    // XML tag removal
    /<true_memory_context[^>]*>/gi,
    /<\/true_memory_context>/gi,
    /<memories[^>]*>/gi,
    /<\/memories>/gi,
  ];

  const hasInjectedContent = injectionMarkers.some(marker => marker.test(conversationText));
  if (hasInjectedContent) {
    log(`WARNING: Conversation contains injection markers (safety check), extractConversationText should have filtered them out`);
    // Don't skip extraction - let the filtered conversationText be processed
  }

  // Extract memories using role-aware classifier
  log(`Processing ${newMessages.length} new messages, ${roleLines.length} lines with role info`);

  // Get signals from patterns (applied to full conversation)
  const signals = matchAllPatterns(conversationText);
  log('Debug: Detected signals:', JSON.stringify(signals));

  let extractionAttempted = false;
  let messagesProcessed = 0; // Track successfully processed messages

  if (signals.length > 0) {
    extractionAttempted = true;

    try {
      // Process each Human message with role-aware classification
      const humanMessages = roleLines.filter(line => line.role === 'user');
      log(`Debug: Processing ${humanMessages.length} Human messages for memory extraction`);

      // Dedupe identical user text within this pass (retries / duplicated fetch)
      const seenTextsForPass = new Set<string>();

      for (const humanMsg of humanMessages) {
        const { text, role } = humanMsg;

        if (text && seenTextsForPass.has(text)) continue;
        if (text) seenTextsForPass.add(text);

        // Get signals specific to this message
        const msgSignals = matchAllPatterns(text);
        if (msgSignals.length === 0) {
          continue; // No signals in this message, skip
        }

        log(`Debug: Processing Human message (${msgSignals.length} signals):`, text.slice(0, 100));

        // Calculate base signal score (average weight of matched signals)
        const baseSignalScore = msgSignals.reduce((sum, s) => sum + s.weight, 0) / msgSignals.length;

        // Apply role weighting (10x for Human messages)
        const roleWeightedScore = calculateRoleWeightedScore(baseSignalScore, role, text);
        log(`Debug: Role-weighted score: ${roleWeightedScore.toFixed(2)} (base: ${baseSignalScore.toFixed(2)})`);

        // Build role-aware context
        const roleAwareContext: RoleAwareContext = {
          primaryRole: role,
          roleWeightedScore,
          hasAssistantContext: roleLines.some(line => line.role === 'assistant'),
          fullConversation: conversationText,
        };

        // Classify with role-awareness
        const { classification, confidence, isolatedContent, roleValidated, validationReason } = classifyWithRoleAwareness(
          text,
          msgSignals,
          roleAwareContext
        );

        // Pre-filter: Skip memories with overly long URLs (>150 chars) or excessive content (>500 chars)
        // This prevents storing stack traces, API dumps, or accidental clipboard content
        if (/https?:\/\/[^\s]{150,}/.test(isolatedContent)) {
          log(`Skipped memory: URL too long`);
          continue;
        }

        // Pre-filter: skip content > 500 chars (before truncation)
        if (isolatedContent.length > 500) {
          log(`Skipped memory: content too long (${isolatedContent.length} chars)`);
          continue;
        }

        log(`Debug: Classification result: ${classification}, confidence: ${confidence.toFixed(2)}, roleValidated: ${roleValidated}, reason: ${validationReason}`);

        if (classification && roleValidated) {
          // Apply three-layer defense
          const result = shouldStoreMemory(isolatedContent, classification, baseSignalScore);

          if (result.store) {
            // Determine scope with contextual awareness
            // - User-level classifications: check context for project signals
            // - Explicit intent WITH global keyword ("sempre", "ovunque", etc.): global scope
            //   NOTE: Check full text, not isolatedContent, because keywords can be in the marker
            // - Explicit intent WITHOUT global keyword: check context
            // - Everything else: project scope
            const userLevelClassifications = ['constraint', 'preference', 'learning', 'procedural'];
            const isExplicitIntent = confidence >= 0.85;
            const hasGlobalKeyword = hasGlobalScopeKeyword(text);
            // FIX: Simplified isUserLevel logic - only check classification type
            // Global keyword check happens inside shouldBeProjectScope()
            const isUserLevel = userLevelClassifications.includes(classification);
            
            // NEW: Contextual scope detection for user-level memories
            let scope: string | null;
            if (isUserLevel) {
              // Build conversation context from recent messages
              // FIX: Increased context window from 10 to 20 messages for better detection
              const recentMessages = roleLines
                .slice(-20) // Last 20 messages for context
                .map(line => line.text);
              
              const context = {
                recentMessages,
                worktree: state.worktree,
                projectTerms: extractProjectTerms(state.worktree),
              };
              
              // Use contextual detection to determine if this should be project-scoped
              const scopeDecision = shouldBeProjectScope(text, context, hasGlobalKeyword);
              scope = scopeDecision.isProjectScope ? state.worktree : null;
              
              log(`Debug: Contextual scope detection: ${scopeDecision.isProjectScope ? 'PROJECT' : 'GLOBAL'} (confidence: ${scopeDecision.confidence.toFixed(2)}, reason: ${scopeDecision.reason})`);
            } else {
              // Non-user-level memories default to project scope
              scope = state.worktree;
            }

            // Determine store: STM vs LTM
            // - Episodic memories ALWAYS go to STM (they decay by nature, 7-day half-life)
            // - Explicit intent (confidence >= 0.85) → LTM for non-episodic (user explicitly said "remember this")
            // - Auto-promote classifications → LTM (learning, decision)
            // - Everything else → STM
            const autoPromoteClassifications = ['learning', 'decision'];
            const shouldPromoteToLtm = classification !== 'episodic' &&
              (isExplicitIntent || autoPromoteClassifications.includes(classification));
            const store = shouldPromoteToLtm ? 'ltm' : 'stm';

            // Store memory (no embeddings - using Jaccard similarity)
            await state.db.createMemory(
              store,
              classification as any,
              extractCleanSummary(isolatedContent), // Clean summary without prefixes
              [],
              {
                sessionId: effectiveSessionId,
                projectScope: scope,
                importance: confidence, // Use confidence from classifyWithRoleAwareness
                confidence: confidence,
              }
            );

            log(`Stored ${classification} memory in ${store.toUpperCase()} (confidence: ${confidence.toFixed(2)}, role: ${role}, reason: ${result.reason})`);
            messagesProcessed++; // Track successful memory storage
          } else {
            log(`Skipped ${classification} memory: ${result.reason}`);
          }
        } else if (classification && !roleValidated) {
          log(`Skipped ${classification} memory: ${validationReason}`);
        }
      }

      // extractionSucceeded flag removed - now tracking messagesProcessed
    } catch (error) {
      log(`Extraction failed with critical error: ${error}`);
      // Update watermark even on error to prevent re-processing same messages
      state.db.updateMessageWatermark(effectiveSessionId, messages.length);
      return;
    }
  }

  // ALWAYS update watermark to prevent infinite loop
  state.db.updateMessageWatermark(effectiveSessionId, messages.length);

  // Update lastExtractionTime AFTER successful extraction (not before)
  lastExtractionTime = Date.now();
}

async function handleSessionEnd(
  state: TrueMemoryAdapterState,
  eventType: string,
  sessionId?: string
): Promise<void> {
  const effectiveSessionId = sessionId ?? state.currentSessionId;
  if (!effectiveSessionId) return;

  const reason = eventType === 'session.error' ? 'abandoned' : 'normal';

  // Maintenance runs at the end of a TOP-LEVEL session only: it is
  // O(all memories). V2 reports sub-agent sessions via session.deleted too,
  // so use the same guard as processSessionIdle to skip the sweep for them
  // (the parent session's end will still perform it).
  if (isSubAgentSession(effectiveSessionId)) {
    log(`Skipping maintenance: sub-agent session detected (${effectiveSessionId})`);
  } else {
    try {
      const decayed = state.db.applyDecay();
      const promoted = state.db.runConsolidation();
      if (decayed > 0 || promoted > 0) {
        log(`Maintenance: decayed ${decayed} memories, promoted ${promoted} to LTM`);
      }
    } catch (err) {
      log(`Maintenance error: ${err}`);
    }
  }

  state.db.endSession(effectiveSessionId, reason === 'abandoned' ? 'abandoned' : 'completed');
  state.currentSessionId = null;
  log(`Session ended: ${effectiveSessionId} (${reason})`);
}

async function handleMessageUpdated(
  state: TrueMemoryAdapterState,
  eventProps: Record<string, unknown> | undefined
): Promise<void> {
  const info = eventProps?.info as { sessionID?: string; role?: string; parts?: Part[] } | undefined;
  const sessionId = info?.sessionID ?? (eventProps?.sessionID as string | undefined) ?? state.currentSessionId;
  if (!sessionId) return;

  if (!state.currentSessionId && sessionId) {
    state.currentSessionId = sessionId;
  }

  // Lazy injection disabled - using atomic injection via tool.execute.before and experimental.chat.system.transform
  // This avoids duplicate injections and provides more context-aware memory retrieval
  // const role = info?.role ?? (eventProps?.role as string | undefined);
  // if (role === 'user' && !state.injectedSessions.has(sessionId)) {
  //   state.injectedSessions.add(sessionId);
  //   log(`Lazy injection for session ${sessionId}`);
  //
  //   // Extract user's message content for contextual retrieval
  //   let userQuery: string | undefined;
  //   const parts = info?.parts ?? (eventProps?.parts as Part[] | undefined);
  //   if (parts && parts.length > 0) {
  //     for (const part of parts) {
  //       if (part.type === 'text' && 'text' in part) {
  //         userQuery = (part as { text: string }).text;
  //         break;
  //       }
  //     }
  //   }
  //
  //   const memories = await getRelevantMemories(state, state.config.opencode.maxSessionStartMemories, userQuery);
  //   if (memories.length > 0) {
  //     const memoryContext = formatMemoriesForInjection(memories, state.worktree);
  //     await injectContext(state, sessionId, memoryContext);
  //     log(`Lazy injection: ${memories.length} memories`);
  //   }
  // }
}

async function handlePostToolUse(
  state: TrueMemoryAdapterState,
  input: { tool: string; sessionID: string; callID: string; args: unknown },
  output: { title: string; output: string; metadata: unknown }
): Promise<void> {
  const sessionId = state.currentSessionId;
  if (!sessionId) return;
  
  const truncate = (s: string): string =>
    s.length > 2000 ? s.slice(0, 2000) + '...[truncated]' : s;
  const toolOutput = truncate(output.output ?? '');
  const toolInput = truncate(JSON.stringify(input.args) ?? '');

  // Self-heal: 'session.created' may never have been delivered, so the session
  // row might not exist yet (only processSessionIdle healed that case).
  if (!state.db.getSession(sessionId)) {
    state.db.createSession(sessionId, state.worktree, { agentType: 'opencode' });
    log(`Self-healed missing session row: ${sessionId}`);
  }

  state.db.createEvent(sessionId, 'PostToolUse', '', {
    toolName: input.tool,
    toolInput,
    toolOutput,
  });
}

// Helpers

/**
 * Extract a clean summary from conversation text.
 * Removes "Human:" / "Assistant:" prefixes and trims to reasonable length.
 */
function extractCleanSummary(conversationText: string, maxLength: number = 500): string {
  // Remove role prefixes
  let cleaned = conversationText
    .replace(/^(Human|Assistant):\s*/gm, '')
    .replace(/\n+/g, ' ')
    .trim();

  // Remove any remaining injection markers (second line of defense)
  const injectionMarkers = [
    /## Relevant Memories from Previous Sessions/gi,
    /### User Preferences & Constraints/gi,
    /### .* Context/gi,
    /## Compaction Instructions/gi,
    /\[LTM\]/gi,
    /\[STM\]/gi,
    /\[TRUE-MEM\]/gi,  // Filter out memory list responses to prevent auto-reference loop
    // XML tag removal
    /<true_memory_context[^>]*>/gi,
    /<\/true_memory_context>/gi,
    /<memories[^>]*>/gi,
    /<\/memories>/gi,
  ];

  for (const marker of injectionMarkers) {
    cleaned = cleaned.replace(marker, '');
  }

  // Normalize whitespace after marker removal
  cleaned = cleaned.replace(/\s+/g, ' ').trim();

  // Truncate if necessary, try to break at word boundaries
  if (cleaned.length <= maxLength) {
    return cleaned;
  }

  // Find last complete word within limit
  const truncated = cleaned.slice(0, maxLength);
  const lastSpace = truncated.lastIndexOf(' ');
  if (lastSpace > maxLength * 0.8) {
    return truncated.slice(0, lastSpace) + '...';
  }
  return truncated + '...';
}

function extractConversationText(messages: MessageContainer[]): string {
  const lines: string[] = [];

  // Regex patterns that indicate injected content (case-insensitive, should be filtered out)
  const injectionMarkers = [
    /## Relevant Memories from Previous Sessions/i,
    /### User Preferences & Constraints/i,
    /### .* Context/i,  // Matches "### ProjectName Context" pattern
    /## Compaction Instructions/i,
    /\[LTM\]/i,
    /\[STM\]/i,
    /\[TRUE-MEM\]/i,  // Filter out memory list responses to prevent auto-reference loop
    // XML tag removal
    /<true_memory_context[^>]*>/gi,
    /<\/true_memory_context>/gi,
    /<memories[^>]*>/gi,
    /<\/memories>/gi,
  ];

  // Regex patterns that indicate tool execution or results (should be filtered out)
  const toolMarkers = [
    /\[Tool:\s*\w+\]/i,
    /^Tool Result:/i,
    /^Tool Error:/i,
    /<tool_use>[\s\S]*?<\/tool_use>/gi,  // Strip <tool_use> blocks
    /<tool_result>[\s\S]*?<\/tool_result>/gi,  // Strip <tool_result> blocks
    /```json[\s\S]*?"tool"[\s\S]*?```/gi,  // Strip JSON blobs with tool
  ];

  for (const msg of messages) {
    const role = msg.info.role === 'user' ? 'Human' : 'Assistant';

    for (const part of msg.parts) {
      if (part.type === 'text' && 'text' in part) {
        const text = (part as { text: string }).text;

        // Skip parts that contain any injection marker (prevents re-extracting injected content)
        const hasInjectionMarker = injectionMarkers.some(marker => {
          marker.lastIndex = 0;
          return marker.test(text);
        });
        if (hasInjectionMarker) {
          continue; // Skip this part entirely
        }

        // Skip parts that look like tool execution or results
        const hasToolMarker = toolMarkers.some(marker => {
          marker.lastIndex = 0;
          return marker.test(text);
        });
        if (hasToolMarker) {
          continue; // Skip this part entirely
        }

        lines.push(`${role}: ${text}`);
      } else if (part.type === 'tool') {
        const toolPart = part as { tool?: string; state?: { status?: string; output?: string; error?: string } };
        if (toolPart.state?.status === 'completed' || toolPart.state?.status === 'error') {
          lines.push(`Assistant: [Tool: ${toolPart.tool}]`);
          if (toolPart.state.output) lines.push(`Tool Result: ${toolPart.state.output.slice(0, 2000)}`);
          if (toolPart.state.error) lines.push(`Tool Error: ${toolPart.state.error}`);
        }
      }
    }
  }

  return lines.join('\n');
}

/**
 * Extract conversation text with role information
 * Returns both the text and role-aware line information
 */
function extractConversationTextWithRoles(messages: MessageContainer[]): {
  text: string;
  lines: RoleAwareLine[];
} {
  const textLines: string[] = [];
  const roleLines: RoleAwareLine[] = [];

  // Regex patterns that indicate injected content (case-insensitive, should be filtered out)
  const injectionMarkers = [
    /## Relevant Memories from Previous Sessions/i,
    /### User Preferences & Constraints/i,
    /### .* Context/i,  // Matches "### ProjectName Context" pattern
    /## Compaction Instructions/i,
    /\[LTM\]/i,
    /\[STM\]/i,
    /\[TRUE-MEM\]/i,  // Filter out memory list responses to prevent auto-reference loop
    // XML tag removal
    /<true_memory_context[^>]*>/gi,
    /<\/true_memory_context>/gi,
    /<memories[^>]*>/gi,
    /<\/memories>/gi,
  ];

  // Regex patterns that indicate tool execution or results (should be filtered out)
  const toolMarkers = [
    /\[Tool:\s*\w+\]/i,
    /^Tool Result:/i,
    /^Tool Error:/i,
    /<tool_use>[\s\S]*?<\/tool_use>/gi,  // Strip <tool_use> blocks
    /<tool_result>[\s\S]*?<\/tool_result>/gi,  // Strip <tool_result> blocks
    /```json[\s\S]*?"tool"[\s\S]*?```/gi,  // Strip JSON blobs with tool
  ];

  for (const msg of messages) {
    const role: MessageRole = msg.info.role === 'user' ? 'user' : 'assistant';
    const roleLabel = role === 'user' ? 'Human' : 'Assistant';

    for (const part of msg.parts) {
      if (part.type === 'text' && 'text' in part) {
        const text = (part as { text: string }).text;

        // Skip parts that contain any injection marker (prevents re-extracting injected content)
        const hasInjectionMarker = injectionMarkers.some(marker => {
          marker.lastIndex = 0;
          return marker.test(text);
        });
        if (hasInjectionMarker) {
          continue; // Skip this part entirely
        }

        // Skip parts that look like tool execution or results
        const hasToolMarker = toolMarkers.some(marker => {
          marker.lastIndex = 0;
          return marker.test(text);
        });
        if (hasToolMarker) {
          continue; // Skip this part entirely
        }

        textLines.push(`${roleLabel}: ${text}`);
        roleLines.push({
          text,
          role,
          lineNumber: textLines.length - 1,
        });
      } else if (part.type === 'tool') {
        const toolPart = part as { tool?: string; state?: { status?: string; output?: string; error?: string } };
        if (toolPart.state?.status === 'completed' || toolPart.state?.status === 'error') {
          const toolText = `Assistant: [Tool: ${toolPart.tool}]`;
          textLines.push(toolText);
          roleLines.push({
            text: toolText,
            role: 'assistant',
            lineNumber: textLines.length - 1,
          });

          if (toolPart.state.output) {
            const outputText = `Tool Result: ${toolPart.state.output.slice(0, 2000)}`;
            textLines.push(outputText);
            roleLines.push({
              text: outputText,
              role: 'assistant',
              lineNumber: textLines.length - 1,
            });
          }
          if (toolPart.state.error) {
            const errorText = `Tool Error: ${toolPart.state.error}`;
            textLines.push(errorText);
            roleLines.push({
              text: errorText,
              role: 'assistant',
              lineNumber: textLines.length - 1,
            });
          }
        }
      }
    }
  }

  return {
    text: textLines.join('\n'),
    lines: roleLines,
  };
}

async function getRelevantMemories(state: TrueMemoryAdapterState, limit: number, query?: string): Promise<MemoryUnit[]> {
  if (query) {
    // Use Jaccard similarity search (text-based, no embeddings)
    return state.db.vectorSearch(query, state.worktree, limit);
  } else {
    // Fall back to scope-based retrieval
    return state.db.getMemoriesByScope(state.worktree, limit);
  }
}

function formatMemoriesForInjection(memories: MemoryUnit[], currentProject?: string): string {
  const lines: string[] = ['## Relevant Memories from Previous Sessions', ''];
  
  const userLevelClassifications = ['constraint', 'preference', 'learning', 'procedural'];
  const userLevel = memories.filter(m => userLevelClassifications.includes(m.classification));
  const projectLevel = memories.filter(m => !userLevelClassifications.includes(m.classification));
  
  if (userLevel.length > 0) {
    lines.push('### User Preferences & Constraints');
    lines.push('_These apply across all projects_');
    lines.push('');
    for (const mem of userLevel) {
      const storeLabel = mem.store === 'ltm' ? '[LTM]' : '[STM]';
      lines.push(`- ${storeLabel} [${mem.classification}] ${mem.summary}`);
    }
    lines.push('');
  }
  
  if (projectLevel.length > 0) {
    const projectName = currentProject ? currentProject.split(/[/\\]/).pop() : 'Current Project';
    lines.push(`### ${projectName} Context`);
    lines.push('');
    for (const mem of projectLevel) {
      const storeLabel = mem.store === 'ltm' ? '[LTM]' : '[STM]';
      lines.push(`- ${storeLabel} [${mem.classification}] ${mem.summary}`);
    }
    lines.push('');
  }
  
  return lines.join('\n');
}

async function injectContext(state: TrueMemoryAdapterState, sessionId: string, context: string): Promise<void> {
  try {
    await state.client.session.prompt({
      path: { id: sessionId },
      body: {
        noReply: true,
        parts: [{ type: 'text', text: context }],
      },
    });
  } catch (error) {
    log(`Failed to inject context: ${error}`);
  }
}

function buildCompactionPrompt(memoriesMarkdown: string | null): string {
  const sections: string[] = [];
  
  if (memoriesMarkdown) {
    sections.push(memoriesMarkdown);
  }
  
  sections.push(`## Compaction Instructions

You are compacting a conversation. Preserve:

### MUST PRESERVE
- Current task/goal
- User constraints, preferences, requirements
- Decisions and rationale
- Errors and solutions
- Files modified and why
- Current state of in-progress work

### CAN DISCARD
- Verbose tool outputs (summarize)
- Intermediate reasoning
- Exploratory discussions
- Repetitive information

### OUTPUT FORMAT
Write a structured summary: task, accomplishments, remaining work, critical context.`);
  
  return sections.join('\n\n');
}

/**
 * Format memories for response to user
 * Groups by scope (Global/Project) then by store (LTM/STM)
 */
function formatMemoryListForResponse(memories: MemoryUnit[]): string {
  const lines: string[] = [];

  // Separate by scope
  const globalMemories = memories.filter(m => !m.projectScope);
  const projectMemories = memories.filter(m => m.projectScope);

  // Global scope section
  if (globalMemories.length > 0) {
    lines.push('**GLOBAL SCOPE:**');
    
    const ltm = globalMemories.filter(m => m.store === 'ltm');
    const stm = globalMemories.filter(m => m.store === 'stm');

    if (ltm.length > 0) {
      lines.push('**LTM:**');
      for (const mem of ltm) {
        lines.push(`• [${mem.classification}] ${mem.summary}`);
      }
    }

    if (stm.length > 0) {
      lines.push('**STM:**');
      for (const mem of stm) {
        lines.push(`• [${mem.classification}] ${mem.summary}`);
      }
    }
    lines.push('');
  }

  // Project scope section
  if (projectMemories.length > 0) {
    lines.push('**PROJECT SCOPE:**');
    
    const ltm = projectMemories.filter(m => m.store === 'ltm');
    const stm = projectMemories.filter(m => m.store === 'stm');

    if (ltm.length > 0) {
      lines.push('**LTM:**');
      for (const mem of ltm) {
        lines.push(`• [${mem.classification}] ${mem.summary}`);
      }
    }

    if (stm.length > 0) {
      lines.push('**STM:**');
      for (const mem of stm) {
        lines.push(`• [${mem.classification}] ${mem.summary}`);
      }
    }
  }

  return lines.join('\n');
}

export default createTrueMemoryPlugin;
