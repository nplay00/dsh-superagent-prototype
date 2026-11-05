/** DingTalk user-identity Source (`dingtalk-user`): DWS event subscriptions, Trigger intake, Policy, and plugin entry. */
import type { Context } from '@deepseek-ai/cordis'
import { spawn } from 'node:child_process'
import { createInterface } from 'node:readline'
import { setTimeout as delay } from 'node:timers/promises'
import { z } from 'zod'
import { threadKey, type Trigger } from '../core/index.ts'
import { CliError } from '../infra/commands.ts'
import { KV } from '../kv.ts'
import { sendDingTalkUser, dingtalkUserLink } from './dingtalk-user-channel.ts'
import { SOURCE, dingtalkUserEventLine, dingtalkUserTrigger, dingtalkUserTriggerId, isMessageEvent, type Delivery } from './dingtalk-user-trigger.ts'
import { health, sourceContext, type SourceContext } from './host.ts'
import type { SourceKind } from './kind.ts'

export const DingTalkUserConfigSchema = z.object({
  /** The `dws` command or absolute path; every call uses the same dedicated profile. */
  cli: z.string().trim().default('dws'),
  profile: z.string().trim().max(200).default(''),
  /** Set only when `dws auth status` does not print the account's openDingtalkId: the account must never answer itself. */
  selfOpenDingtalkId: z.string().trim().max(200).default(''),
  groups: z.array(z.object({ conversationId: z.string().trim().min(1).max(200), name: z.string().trim().max(60).optional(),
    listen: z.enum(['mentions', 'all']) }).strict())
    .refine(groups => new Set(groups.map(group => group.conversationId)).size === groups.length, 'Duplicate conversation IDs')
    .default([]),
  directMessages: z.boolean().default(false),
  otherGroups: z.enum(['none', 'mentions']).default('none'),
  reconnectSeconds: z.number().int().min(1).max(300).default(5),
}).strict()
export type DingTalkUserConfig = z.infer<typeof DingTalkUserConfigSchema>

/** One long-running `dws event +listen-im` subscription. */
export interface DingTalkUserSubscription { kind: 'at-me' | 'group' | 'all-direct'; chatId?: string }
export const subscriptionId = (subscription: DingTalkUserSubscription): string =>
  subscription.kind === 'group' ? `group:${subscription.chatId ?? ''}` : subscription.kind
/**
 * The fewest subscriptions that cover the configuration: @-me everywhere for mention-only groups and other groups,
 * one all-message subscription per group that listens to everything, and all direct messages when enabled.
 */
export function subscriptionsOf(config: DingTalkUserConfig): DingTalkUserSubscription[] {
  const subscriptions: DingTalkUserSubscription[] = []
  if (config.directMessages) subscriptions.push({ kind: 'all-direct' })
  const mentions = config.groups.some(group => group.listen === 'mentions')
  if (mentions || config.otherGroups === 'mentions') subscriptions.push({ kind: 'at-me' })
  for (const group of config.groups) if (group.listen === 'all') subscriptions.push({ kind: 'group', chatId: group.conversationId })
  return subscriptions
}
/** The DWS command line of one subscription. DWS keeps the transport envelope without `--flatten`, which this command does not take. */
export function listenArgs(config: DingTalkUserConfig, subscription: DingTalkUserSubscription): string[] {
  return ['event', '+listen-im', '--kind', subscription.kind, '--events', 'message', '--format', 'ndjson',
    ...subscription.chatId ? ['--chat-id', subscription.chatId] : [], ...config.profile ? ['--profile', config.profile] : []]
}
const deliveryOf = (subscription: DingTalkUserSubscription): Delivery =>
  subscription.kind === 'at-me' ? 'mention' : subscription.kind === 'all-direct' ? 'direct' : 'observed'

/** The account's own openDingtalkId, searched in the JSON `dws auth status` returns (its shape varies by version). */
export function findOpenDingtalkId(value: unknown, depth = 0): string | undefined {
  if (depth > 6 || !value || typeof value !== 'object') return undefined
  for (const [key, item] of Object.entries(value)) {
    if (typeof item === 'string' && /^open_?ding?talk_?id$/i.test(key)) return item
    const found = findOpenDingtalkId(item, depth + 1)
    if (found) return found
  }
  return undefined
}
const AuthStatus = z.looseObject({ authenticated: z.boolean().default(false), success: z.boolean().optional() })
/**
 * Checks that the dedicated account is signed in and resolves its openDingtalkId, which filters the account's own messages.
 * The id can be configured instead, because `dws` does not print it in every version.
 */
export async function dingtalkUserAccount(host: Pick<SourceContext, 'commands'>, config: DingTalkUserConfig):
Promise<{ authenticated: boolean; selfOpenDingtalkId: string }> {
  const output = await host.commands.run(config.cli, ['auth', 'status', '--readonly', '--format', 'json', ...config.profile ? ['--profile', config.profile] : []])
  let value: unknown
  try { value = JSON.parse(output.slice(output.indexOf('{'))) }
  catch { throw new CliError('dws auth status returned no readable result') }
  const status = AuthStatus.parse(value)
  return { authenticated: status.authenticated, selfOpenDingtalkId: config.selfOpenDingtalkId || findOpenDingtalkId(value) || '' }
}
interface ListenStatus { live: Set<string>; errors: Map<string, string>; lastEventAt?: string; events: number }
const newStatus = (lastEventAt?: string): ListenStatus => ({ live: new Set(), errors: new Map(), lastEventAt, events: 0 })
/** Reports ready while any listener is connected; otherwise the last error, or `starting` before the first answer. */
export function reportListen(ctx: Pick<SourceContext, 'report'>, status: ListenStatus): void {
  const at = new Date().toISOString()
  if (status.live.size) {
    const last = status.lastEventAt ? `; last event ${status.lastEventAt}` : ''
    ctx.report(HEALTH, { state: 'ready', detail: `${status.live.size} listener(s) connected; ${status.events} event(s)${last}`, at })
    return
  }
  const error = [...status.errors.values()].at(-1)
  ctx.report(HEALTH, error ? { state: 'error', detail: error, at } : { state: 'starting', detail: 'Connecting to the dws event stream', at })
}
const HEALTH = 'dingtalk-user'
/**
 * Takes in one NDJSON line from a listener. An at-me subscription skips a conversation that also has an all-message
 * subscription: the all-message copy is the one that represents an unaddressed message, and both share a Trigger ID.
 */
export async function ingestLine(ctx: SourceContext, line: string, subscription: DingTalkUserSubscription, config: DingTalkUserConfig,
  status: ListenStatus, seen: Set<string>): Promise<'stored' | 'filtered' | 'duplicate' | 'ignored' | 'unreadable'> {
  if (!line.trim()) return 'ignored'
  let event: unknown
  try { event = dingtalkUserEventLine(line) }
  catch { return 'unreadable' }
  if (!isMessageEvent(event)) return 'ignored'
  const identity = event as { message_id?: unknown; conversation_id?: unknown }
  if (typeof identity.message_id !== 'string' || typeof identity.conversation_id !== 'string') return 'ignored'
  const messageId = identity.message_id, conversationId = identity.conversation_id
  if (seen.has(messageId)) return 'ignored'
  // An all-message subscription delivers this mention too; the at-me copy must not use up the Trigger ID first.
  if (subscription.kind === 'at-me' && config.groups.some(group => group.conversationId === conversationId && group.listen === 'all')) return 'ignored'
  const remember = (): void => { seen.add(messageId); if (seen.size > 2000) seen.delete(seen.values().next().value!) }
  const trigger = dingtalkUserTrigger(event, deliveryOf(subscription), { selfOpenDingtalkId: config.selfOpenDingtalkId, cli: config.cli, profile: config.profile })
  if (!trigger) { remember(); return 'ignored' }
  // A failed store keeps the message out of the seen set and is retried, so a transient database error does not lose it.
  let result: 'stored' | 'duplicate' | 'filtered' = 'stored'
  for (let attempt = 0; ; attempt++) {
    try { result = await ctx.receive(trigger); break }
    catch (error) { if (attempt >= 2) throw error; await delay(500) }
  }
  remember()
  if (result === 'filtered' || result === 'duplicate') return result
  status.events += 1
  status.lastEventAt = new Date().toISOString()
  ctx.state.set(KV.dingtalkUser.lastEvent, status.lastEventAt)
  describe(ctx, trigger, config)
  return result
}
/** Publishes the group name or the direct correspondent on the Thread, for the panel and assessments. */
function describe(ctx: SourceContext, trigger: Trigger, config: DingTalkUserConfig): void {
  const conversation = String(trigger.meta.conversationId ?? '')
  const group = config.groups.find(item => item.conversationId === conversation)
  const label = group?.name || (trigger.meta.direct === true ? `DingTalk direct ${String(trigger.meta.sender || trigger.actor.id)}` : conversation)
  for (const ref of trigger.threads) ctx.threads.describe(threadKey(ref), { label })
}
/** Runs one subscription until the signal aborts, restarting a listener that exits after `reconnectSeconds`. */
async function supervise(ctx: SourceContext, config: DingTalkUserConfig, subscription: DingTalkUserSubscription, selfOpenDingtalkId: string,
  status: ListenStatus, seen: Set<string>, signal: AbortSignal): Promise<void> {
  const id = subscriptionId(subscription)
  let failures = 0
  while (!signal.aborted) {
    status.live.delete(id)
    try {
      // A detached process group lets a stop end the CLI and anything it started, so a pipe cannot stay open.
      const child = spawn(config.cli, listenArgs(config, subscription), { stdio: ['ignore', 'pipe', 'pipe'], env: process.env, detached: process.platform !== 'win32' })
      const abort = (): void => {
        if (process.platform !== 'win32' && child.pid) { try { process.kill(-child.pid, 'SIGTERM'); return } catch { /* Fall back to the process alone. */ } }
        child.kill('SIGTERM')
      }
      signal.addEventListener('abort', abort, { once: true })
      if (signal.aborted) abort()
      const closed = new Promise<void>(resolve => {
        child.once('error', (error: unknown) => { status.errors.set(id, `${id}: ${error instanceof Error ? error.message : String(error)}`); resolve() })
        child.once('exit', (code, signalName) => {
          if (!signal.aborted) status.errors.set(id, `${id}: dws exited ${signalName ? `by ${signalName}` : `with code ${code}`}`)
          resolve()
        })
      })
      if (child.stderr) void (async () => {
        for await (const line of createInterface({ input: child.stderr! })) {
          if (line.includes('[event] ready')) { status.live.add(id); status.errors.delete(id); failures = 0; reportListen(ctx, status) }
          else if (/\b(error|failed)\b/i.test(line)) status.errors.set(id, `${id}: ${line.slice(0, 300)}`)
        }
      })().catch((error: unknown) => { status.errors.set(id, `${id}: ${error instanceof Error ? error.message : String(error)}`) })
      if (child.stdout) for await (const line of createInterface({ input: child.stdout })) {
        if (signal.aborted) break
        // A message line also proves the listener is connected, for a CLI that does not print its ready marker.
        status.live.add(id); status.errors.delete(id); failures = 0
        const active = ctx.config<DingTalkUserConfig>(SOURCE)
        if (!active) break
        try { await ingestLine(ctx, line, subscription, { ...active, selfOpenDingtalkId }, status, seen) }
        catch (error) { status.errors.set(id, `${id}: ${error instanceof Error ? error.message : String(error)}`) }
        reportListen(ctx, status)
      }
      await closed
      signal.removeEventListener('abort', abort)
    } catch (error) {
      status.errors.set(id, `${id}: ${error instanceof Error ? error.message : String(error)}`)
    }
    status.live.delete(id)
    reportListen(ctx, status)
    // A listener that keeps failing backs off to at most a minute, so a broken account is not respawned in a tight loop.
    failures += 1
    const seconds = Math.min(config.reconnectSeconds * 2 ** (failures - 1), 60)
    if (!signal.aborted) await delay(seconds * 1000, undefined, { signal }).catch(() => {})
  }
}
/** Stops a group of listeners and waits for them to end. */
async function stop(controller: AbortController | undefined, tasks: Promise<void>[]): Promise<void> {
  controller?.abort()
  await Promise.allSettled(tasks)
}
/**
 * Keeps the listeners matched to the saved configuration and the signed-in account. A change of the account, the CLI or
 * the subscription set restarts the listeners; a signed-out account reports an error and is retried every 30 seconds.
 */
export async function watch(ctx: SourceContext, signal: AbortSignal): Promise<void> {
  await ctx.ready
  if (!ctx.configured()) { ctx.report(HEALTH, health('paused', 'setupPending')); return }
  const seen = new Set<string>()
  let status = newStatus(ctx.state.get(KV.dingtalkUser.lastEvent))
  let running: { signature: string; controller: AbortController; tasks: Promise<void>[] } | undefined
  let identity: { authenticated: boolean; selfOpenDingtalkId: string } | undefined
  let checkedAt = 0
  try {
    while (!signal.aborted) {
      const config = ctx.config<DingTalkUserConfig>(SOURCE)
      if (!config) {
        await stop(running?.controller, running?.tasks ?? []); running = undefined; identity = undefined
        ctx.report(HEALTH, health('paused', 'sourceOff'))
        await delay(5000, undefined, { signal }).catch(() => {})
        continue
      }
      if (!identity || Date.now() - checkedAt > 60_000) {
        checkedAt = Date.now()
        try { identity = await dingtalkUserAccount(ctx, config) }
        catch (error) { identity = undefined; status.errors.set('auth', `auth: ${error instanceof Error ? error.message : String(error)}`) }
        if (identity && !identity.authenticated) { identity = undefined; status.errors.set('auth', 'auth: dws is not signed in with the dedicated account') }
        if (identity && !identity.selfOpenDingtalkId) { identity = undefined; status.errors.set('auth', 'auth: dws did not report the account openDingtalkId; set it in the Source settings') }
      }
      if (!identity) {
        // A failed check must not tear down a stream that still works; a signed-out account's listeners exit on their own.
        reportListen(ctx, status)
        await delay(30_000, undefined, { signal }).catch(() => {})
        continue
      }
      const subscriptions = subscriptionsOf(config)
      const self = identity.selfOpenDingtalkId
      const signature = JSON.stringify({ cli: config.cli, profile: config.profile, subscriptions, self })
      if (!running || running.signature !== signature) {
        await stop(running?.controller, running?.tasks ?? [])
        status = newStatus(status.lastEventAt)
        const controller = new AbortController()
        const childSignal = AbortSignal.any([signal, controller.signal])
        running = { signature, controller, tasks: [] }
        // A signed-out account has no subscription, so the connection health stays `error` until it signs in.
        running.tasks = subscriptions.length
          ? subscriptions.map(subscription => supervise(ctx, config, subscription, self, status, seen, childSignal))
          : []
        if (!subscriptions.length) status.errors.set('config', 'config: no group or direct message is enabled')
        reportListen(ctx, status)
      }
      reportListen(ctx, status)
      await delay(5000, undefined, { signal }).catch(() => {})
    }
  } finally {
    await stop(running?.controller, running?.tasks ?? [])
  }
}
/** The DingTalk kind: user-identity messages from approved groups and direct messages, replied through `dws`. */
export const dingtalkUserKind: SourceKind<DingTalkUserConfig> = {
  kind: SOURCE,
  title: { zh: '钉钉（专用账号）', en: 'DingTalk (dedicated account)' },
  description: { zh: '以专用钉钉账号接收批准的群消息和单聊，@ 或私聊发起工作，结果回到原会话。', en: 'Receives approved group messages and direct messages as a dedicated DingTalk account; mentions and direct messages start work, and results return to the same conversation.' },
  schema: DingTalkUserConfigSchema,
  fields: [
    { key: 'cli', type: 'text', label: { zh: 'dws 命令', en: 'dws command' },
      hint: { zh: '命令名或绝对路径，指向安装好的钉钉 Workspace CLI；Source 以专用 profile 调用它，执行器不保存 token。', en: 'A command name or absolute path of the installed DingTalk Workspace CLI; the Source calls it with the dedicated profile, and the Executor stores no token.' } },
    { key: 'profile', type: 'text', advanced: true, label: { zh: 'DWS profile', en: 'DWS profile' },
      hint: { zh: '与专用账号登录态绑定的 profile；留空使用 dws 的当前 profile。不能与操作者本人的日常 profile 混用。', en: 'The profile bound to the dedicated account login; empty uses the current dws profile. Never share the operator\'s personal profile.' } },
    { key: 'groups', type: 'rows', label: { zh: '接收的群', en: 'Groups' },
      hint: { zh: '“仅 @”只接收 @ 专用账号的消息；“全部消息”接收群里所有消息，未 @ 的消息只能按策略接收表成为推断候选。只列批准的群。', en: '“@ only” receives messages that mention the dedicated account; “All” receives every message, and unaddressed ones can only become inferred candidates through the Policy admission table. List approved groups only.' },
      columns: [
        { key: 'conversationId', type: 'text', label: { zh: 'openConversationId', en: 'openConversationId' }, pattern: '[^\\s]+' },
        { key: 'name', type: 'text', label: { zh: '群名', en: 'Name' }, optional: true },
        { key: 'listen', type: 'select', label: { zh: '接收', en: 'Receive' }, options: [{ value: 'mentions', label: { zh: '仅 @', en: '@ only' } }, { value: 'all', label: { zh: '全部消息', en: 'All' } }] },
      ] },
    { key: 'otherGroups', type: 'select', label: { zh: '其他群', en: 'Other groups' }, options: [{ value: 'none', label: { zh: '不接收', en: 'Not received' } }, { value: 'mentions', label: { zh: '仅 @', en: '@ only' } }],
      hint: { zh: '账号已加入、但没列在上面的群。“仅 @”只接收那里 @ 账号的消息；谁的消息能发起工作看策略的准入表。', en: 'Groups the account has joined that are not listed above. “@ only” receives mentions there; whose messages may start work is the Policy admission table.' } },
    { key: 'directMessages', type: 'switch', label: { zh: '接收单聊', en: 'Direct messages' },
      hint: { zh: '开启即接收专用账号的全部单聊，需要单独的隐私评审；默认关闭。', en: 'On receives every direct message of the dedicated account and needs a separate privacy review; off by default.' } },
    { key: 'selfOpenDingtalkId', type: 'text', advanced: true, label: { zh: '专用账号的 openDingtalkId', en: 'The account openDingtalkId' },
      hint: { zh: '通常留给空，由 dws auth status 读出；读不到时填写，用来丢弃账号自己发出的消息，避免自问自答循环。', en: 'Usually empty and read from dws auth status; fill it when that does not print it. It drops the account\'s own messages so the agent cannot answer itself in a loop.' } },
    { key: 'reconnectSeconds', type: 'number', advanced: true, min: 1, max: 300, label: { zh: '断线重连间隔 / 秒', en: 'Reconnect interval / seconds' } },
  ],
  health: [HEALTH],
  scope({ meta }, config) {
    if (meta.direct === true) return config.directMessages ? { listened: true, observed: false, row: 'direct' } : { listened: false, reason: 'direct_messages_not_allowed' }
    const group = config.groups.find(item => item.conversationId === meta.conversationId)
    if (group) return { listened: true, observed: group.listen === 'all', row: group.conversationId }
    return config.otherGroups === 'mentions' ? { listened: true, observed: false, row: 'other' } : { listened: false, reason: 'group_not_allowed' }
  },
  channel: {
    allowed: (address, config) => dingtalkUserKind.scope!({ meta: { conversationId: address.conversationId ?? '', direct: address.direct === 'true' } }, config).listened,
    send: sendDingTalkUser, link: dingtalkUserLink,
  },
  repliesTo: trigger => typeof trigger.meta.quoteId === 'string' ? dingtalkUserTriggerId(trigger.meta.quoteId) : undefined,
  policy: {
    unnamed: { person: { zh: '钉钉成员', en: 'DingTalk member' }, bot: { zh: '钉钉机器人', en: 'DingTalk bot' } },
    schema: z.object({ allowed: z.array(z.object({ id: z.string().trim().min(1).max(200), name: z.string().trim().max(100).optional() }).strict())
      .refine(users => new Set(users.map(user => user.id)).size === users.length, 'Duplicate openDingtalkIds').default([]) }).strict(),
    fields: [{ key: 'allowed', type: 'rows', label: { zh: '能发起工作的钉钉成员', en: 'DingTalk members who may originate work' },
      columns: [{ key: 'id', type: 'text', label: { zh: 'openDingtalkId', en: 'openDingtalkId' } }, { key: 'name', type: 'text', label: { zh: '名字', en: 'Name' }, optional: true }] }],
    explicit: { zh: '名单内的人在批准的群里 @ 专用账号，或私聊它。', en: 'Someone on the list mentions the dedicated account in an approved group or messages it directly.' },
    rows(stored) {
      const config = DingTalkUserConfigSchema.parse(stored)
      const mentions = { zh: '仅 @', en: '@ only' }
      return [
        ...config.groups.map(group => ({ id: group.conversationId, label: group.name || group.conversationId, note: group.listen === 'all' ? { zh: '全部消息', en: 'All' } : mentions,
          cells: { listedUnaddressed: group.listen === 'all', unlistedAddressed: true, unlistedUnaddressed: group.listen === 'all' } })),
        { id: 'other', label: { zh: '其他群', en: 'Other groups' }, note: config.otherGroups === 'mentions' ? mentions : { zh: '不接收', en: 'Not received' },
          cells: { listedUnaddressed: false, unlistedAddressed: config.otherGroups === 'mentions', unlistedUnaddressed: false } },
        { id: 'direct', label: { zh: '单聊', en: 'Direct messages' }, note: config.directMessages ? { zh: '接收', en: 'Received' } : { zh: '不接收', en: 'Not received' },
          cells: { listedUnaddressed: false, unlistedAddressed: config.directMessages, unlistedUnaddressed: false } },
      ]
    },
  },
  environment: {
    variables: [
      { key: 'DSH_SUPERAGENT_DINGTALK_USER', description: { zh: '专用钉钉账号的 openDingtalkId；工单不是从钉钉来的时为空', en: 'openDingtalkId of the dedicated DingTalk account; empty when the ticket did not start on DingTalk' } },
      { key: 'DSH_SUPERAGENT_DINGTALK_GROUP', description: { zh: '接下工单的钉钉群 openConversationId；单聊或不是从钉钉来的时为空', en: 'openConversationId of the DingTalk group that started the ticket; empty for a direct message or a ticket from another Source' } },
    ],
    values: ({ config, trigger }) => ({ DSH_SUPERAGENT_DINGTALK_USER: trigger?.source === SOURCE ? String(config.selfOpenDingtalkId ?? '') : '',
      DSH_SUPERAGENT_DINGTALK_GROUP: trigger?.source === SOURCE && trigger.meta.direct !== true ? String(trigger.meta.conversationId ?? '') : '' }),
  },
}
export const name = 'dsh-superagent-dingtalk-user'
export const inject = ['orchestrator']
export function apply(ctx: Context): void {
  ctx.effect(() => ctx.orchestrator.sources.register(dingtalkUserKind), 'dingtalk-user: source kind')
  ctx.effect(() => {
    const source = sourceContext(ctx.orchestrator, dingtalkUserKind)
    const controller = new AbortController()
    // An unhandled rejection ends the DSH process, so a failure escaping the loop becomes the Source's health.
    const running = watch(source, controller.signal).catch((error: unknown) => { source.report(HEALTH, { state: 'error', detail: String(error) }) })
    return async () => { controller.abort(); await running }
  })
}
