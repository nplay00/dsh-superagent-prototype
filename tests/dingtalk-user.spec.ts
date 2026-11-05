/** The DingTalk user-identity Source: event translation, subscriptions, scope, Policy, Channel and the listener loop. */
import { chmodSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi } from 'vitest'
import { key, type OutboundMessage, type Trigger } from '../src/core/index.ts'
import { CliError } from '../src/infra/commands.ts'
import { KV } from '../src/kv.ts'
import type { SourceContext } from '../src/sources/host.ts'
import { DingTalkUserConfigSchema, dingtalkUserAccount, dingtalkUserKind, findOpenDingtalkId, ingestLine, listenArgs,
  subscriptionId, subscriptionsOf, watch, type DingTalkUserConfig, type DingTalkUserSubscription } from '../src/sources/dingtalk-user.ts'
import { sendDingTalkUser } from '../src/sources/dingtalk-user-channel.ts'
import { dingtalkUserChannel, dingtalkUserEventLine, dingtalkUserText, dingtalkUserTime, dingtalkUserTrigger, dingtalkUserTriggerId, isMessageEvent }
  from '../src/sources/dingtalk-user-trigger.ts'

const TIME = '1762348800000'
const MESSAGE = { message_id: 'msg_1', conversation_id: 'cid_1', sender: 'Alice', sender_open_dingtalk_id: 'open_alice',
  content: 'please fix the margin', create_time: TIME }
const config = (overrides: Record<string, unknown> = {}): DingTalkUserConfig => DingTalkUserConfigSchema.parse({
  cli: '/opt/dws', groups: [{ conversationId: 'cid_1', name: 'Team', listen: 'mentions' }], ...overrides })
const event = (overrides: Record<string, unknown> = {}) => ({ ...MESSAGE, ...overrides })
/** The few SourceContext members these functions read; a cast keeps the unit tests on the Source's own behavior. */
function host(configValue: DingTalkUserConfig = config()) {
  const state = new Map<string, unknown>()
  const reports: { entry: string; health: { state: string; detail: string } }[] = []
  const receive = vi.fn(async (_trigger: Trigger) => 'stored' as const)
  const describe = vi.fn()
  let current = configValue
  const ctx = { ready: Promise.resolve(), configured: () => true, config: () => current, receive,
    report: (entry: string, value: { state: string; detail: string }) => { reports.push({ entry, health: value }) },
    state: { get: (name: string) => state.get(name), set: (name: string, value: unknown) => { state.set(name, value) }, keys: () => [] },
    threads: { describe }, commands: { run: vi.fn() } } as unknown as SourceContext
  return { ctx, state, reports, receive, describe, setConfig: (next: DingTalkUserConfig) => { current = next } }
}
const subscription = (kind: DingTalkUserSubscription['kind'], chatId?: string): DingTalkUserSubscription => ({ kind, ...chatId ? { chatId } : {} })
const reply = (direct = false, profile = ''): OutboundMessage => ({ id: 'reply-1', kind: 'reply', text: 'Done',
  channel: dingtalkUserChannel('cid_1', 'msg_1', 'open_alice', direct, '/opt/dws', profile) })

describe('dingtalk-user trigger', () => {
  it('maps a mention into a stable conversation Thread, with the quote as context', () => {
    const trigger = dingtalkUserTrigger(event({ quoted_message: { message_id: 'msg_0', sender: 'Bob', content: 'the old margin' } }), 'mention',
      { selfOpenDingtalkId: 'open_self', cli: '/opt/dws', profile: 'corp:assistant' })!
    expect(trigger).toMatchObject({ id: key('dingtalk-user', 'msg_1'), source: 'dingtalk-user',
      threads: [{ source: 'dingtalk-user', id: 'cid_1' }], actor: { source: 'dingtalk-user', id: 'open_alice' }, human: true,
      addressed: 'mention', at: dingtalkUserTime(TIME), text: 'please fix the margin\n\n> the old margin',
      channel: { source: 'dingtalk-user', thread: 'dingtalk-user:cid_1', address: { conversationId: 'cid_1', messageId: 'msg_1',
        senderOpenDingtalkId: 'open_alice', direct: 'false', cli: '/opt/dws', profile: 'corp:assistant' } },
      meta: { conversationId: 'cid_1', messageId: 'msg_1', senderOpenDingtalkId: 'open_alice', direct: false, quoteId: 'msg_0',
        quoteText: 'the old margin' } })
    expect(dingtalkUserKind.repliesTo!(trigger)).toBe(key('dingtalk-user', 'msg_0'))
  })
  it('marks a direct message and leaves an observed group message unaddressed', () => {
    expect(dingtalkUserTrigger(event(), 'direct', { cli: 'dws', profile: '' })).toMatchObject({ addressed: 'direct',
      channel: { address: { direct: 'true' } }, meta: { direct: true } })
    expect(dingtalkUserTrigger(event(), 'observed', { cli: 'dws', profile: '' })).not.toHaveProperty('addressed')
  })
  it('drops the account\'s own messages and unreadable events', () => {
    expect(dingtalkUserTrigger(event({ sender_open_dingtalk_id: 'open_self' }), 'mention', { selfOpenDingtalkId: 'open_self', cli: 'dws', profile: '' })).toBeUndefined()
    expect(dingtalkUserTrigger({ content: 'no ids' }, 'mention', { cli: 'dws', profile: '' })).toBeUndefined()
    expect(dingtalkUserTime(1762348800)).toBe(new Date(1762348800000).toISOString())
    expect(() => dingtalkUserTime('not a time')).toThrow('Unreadable')
  })
  it('reads plain, JSON-object and rich-text message bodies, and a quoted message sent as a JSON string', () => {
    expect(dingtalkUserText('hello')).toBe('hello')
    expect(dingtalkUserText('{"content":"hello"}')).toBe('hello')
    expect(dingtalkUserText('[{"text":"a"},{"text":"b"}]')).toBe('ab')
    expect(dingtalkUserText('{not json')).toBe('{not json')
    const trigger = dingtalkUserTrigger(event({ quoted_message: JSON.stringify({ message_id: 'msg_0', sender: 'Bob', content: 'old' }) }), 'mention', { cli: 'dws', profile: '' })!
    expect(trigger.meta.quoteId).toBe('msg_0')
    expect(trigger.text).toBe('please fix the margin\n\n> old')
  })
  it('unwraps the DWS transport envelope and recognizes only message events', () => {
    const inner = event()
    expect(dingtalkUserEventLine(JSON.stringify({ type: 'message', data: JSON.stringify(inner) }))).toMatchObject({ message_id: 'msg_1' })
    expect(dingtalkUserEventLine(JSON.stringify(inner))).toMatchObject({ message_id: 'msg_1' })
    expect(isMessageEvent({ type: 'message', message_id: 'm' })).toBe(true)
    expect(isMessageEvent({ type: 'read', message_id: 'm' })).toBe(false)
    expect(isMessageEvent({ type: 'recall' })).toBe(false)
  })
})

describe('dingtalk-user subscriptions and scope', () => {
  it('subscribes to exactly what the configuration receives', () => {
    expect(subscriptionsOf(config())).toEqual([{ kind: 'at-me' }])
    const all = config({ groups: [{ conversationId: 'cid_all', listen: 'all' }, { conversationId: 'cid_1', listen: 'mentions' }], directMessages: true, otherGroups: 'mentions' })
    expect(subscriptionsOf(all)).toEqual([{ kind: 'all-direct' }, { kind: 'at-me' }, { kind: 'group', chatId: 'cid_all' }])
    expect(subscriptionId({ kind: 'group', chatId: 'cid_all' })).toBe('group:cid_all')
    expect(listenArgs(config({ profile: 'corp:assistant' }), { kind: 'group', chatId: 'cid_all' })).toEqual(['event', '+listen-im', '--kind', 'group',
      '--events', 'message', '--format', 'ndjson', '--chat-id', 'cid_all', '--profile', 'corp:assistant'])
  })
  it('listens to a listed group by its receive mode, and to other groups and direct messages only as configured', () => {
    const mentions = config()
    expect(dingtalkUserKind.scope!({ meta: { conversationId: 'cid_1' } }, mentions)).toEqual({ listened: true, observed: false, row: 'cid_1' })
    expect(dingtalkUserKind.scope!({ meta: { conversationId: 'cid_1' } }, config({ groups: [{ conversationId: 'cid_1', listen: 'all' }] })))
      .toEqual({ listened: true, observed: true, row: 'cid_1' })
    expect(dingtalkUserKind.scope!({ meta: { conversationId: 'elsewhere' } }, config())).toEqual({ listened: false, reason: 'group_not_allowed' })
    expect(dingtalkUserKind.scope!({ meta: { conversationId: 'elsewhere' } }, config({ otherGroups: 'mentions' })))
      .toEqual({ listened: true, observed: false, row: 'other' })
    expect(dingtalkUserKind.scope!({ meta: { direct: true } }, config())).toEqual({ listened: false, reason: 'direct_messages_not_allowed' })
    expect(dingtalkUserKind.scope!({ meta: { direct: true } }, config({ directMessages: true }))).toEqual({ listened: true, observed: false, row: 'direct' })
  })
  it('offers the admission rows and the allowed list of the Source Policy', () => {
    const rows = dingtalkUserKind.policy!.rows!(config({ otherGroups: 'mentions', directMessages: true }))
    expect(rows.map(row => row.id)).toEqual(['cid_1', 'other', 'direct'])
    expect(rows[0]!.cells).toEqual({ listedUnaddressed: false, unlistedAddressed: true, unlistedUnaddressed: false })
    const part = dingtalkUserKind.policy!.schema.parse({ allowed: [{ id: 'open_alice', name: 'Alice' }] })
    expect(part.allowed).toEqual([{ id: 'open_alice', name: 'Alice' }])
    expect(() => dingtalkUserKind.policy!.schema.parse({ allowed: [{ id: 'a' }, { id: 'a' }] })).toThrow('Duplicate')
  })
})

describe('dingtalk-user intake', () => {
  it('retries a failed store instead of losing the message', async () => {
    const h = host()
    h.receive.mockRejectedValueOnce(new Error('database busy')).mockResolvedValueOnce('stored')
    const status = { live: new Set<string>(), errors: new Map<string, string>(), events: 0 }
    expect(await ingestLine(h.ctx, JSON.stringify(event()), { kind: 'at-me' }, config(), status, new Set())).toBe('stored')
    expect(h.receive).toHaveBeenCalledTimes(2)
  })
  it('stores a message once and records its last event', async () => {
    const h = host()
    const status = { live: new Set<string>(), errors: new Map<string, string>(), events: 0 }
    const seen = new Set<string>()
    expect(await ingestLine(h.ctx, JSON.stringify({ type: 'message', data: JSON.stringify(event()) }), { kind: 'at-me' }, config(), status, seen)).toBe('stored')
    expect(h.receive).toHaveBeenCalledTimes(1)
    expect(h.receive.mock.calls[0]![0]).toMatchObject({ id: dingtalkUserTriggerId('msg_1'), addressed: 'mention' })
    expect(h.describe).toHaveBeenCalledWith('dingtalk-user:cid_1', { label: 'Team' })
    expect(h.state.get(KV.dingtalkUser.lastEvent)).toBeTruthy()
    expect(status.events).toBe(1)
    expect(await ingestLine(h.ctx, JSON.stringify(event()), { kind: 'at-me' }, config(), status, seen)).toBe('ignored')
    expect(h.receive).toHaveBeenCalledTimes(1)
  })
  it('leaves a mention in an all-message group to that group\'s listener, and drops the account\'s own message', async () => {
    const h = host(config({ groups: [{ conversationId: 'cid_1', listen: 'all' }] }))
    const status = { live: new Set<string>(), errors: new Map<string, string>(), events: 0 }
    const seen = new Set<string>()
    expect(await ingestLine(h.ctx, JSON.stringify(event()), { kind: 'at-me' }, config({ groups: [{ conversationId: 'cid_1', listen: 'all' }] }), status, seen)).toBe('ignored')
    expect(await ingestLine(h.ctx, JSON.stringify(event()), { kind: 'group', chatId: 'cid_1' }, config({ groups: [{ conversationId: 'cid_1', listen: 'all' }] }), status, seen)).toBe('stored')
    expect(h.receive.mock.calls[0]![0]).not.toHaveProperty('addressed')
    expect(await ingestLine(h.ctx, JSON.stringify(event({ sender_open_dingtalk_id: 'open_self' })), { kind: 'at-me' },
      config({ selfOpenDingtalkId: 'open_self' }), status, new Set())).toBe('ignored')
  })
})

describe('dingtalk-user channel', () => {
  const called = () => vi.fn(async (_cli: string, _args: string[]) => '{"success":true,"data":{"openTaskId":"t1"}}')
  it('quote-replies in a group with the sender mentioned and the Outbox ID as idempotency key', async () => {
    const run = called()
    expect(await sendDingTalkUser({ commands: { run } } as never, reply())).toBe('sent')
    const args = run.mock.calls[0]![1] as string[]
    expect(args.slice(0, 2)).toEqual(['chat', '+messages-reply'])
    expect(args).toEqual(expect.arrayContaining(['--group', 'cid_1', '--message-id', 'msg_1', '--idempotency-key', 'reply-1',
      '--at-open-dingtalk-ids', 'open_alice', '--wait-delivery']))
  })
  it('sends a direct message as Markdown, and falls back to a group send when the quoted message is gone', async () => {
    const direct = called()
    expect(await sendDingTalkUser({ commands: { run: direct } } as never, reply(true, 'corp:assistant'))).toBe('sent')
    expect(direct.mock.calls[0]![1]).toEqual(expect.arrayContaining(['+messages-send', '--open-dingtalk-id', 'open_alice', '--markdown', '--profile', 'corp:assistant']))
    const run = vi.fn(async (_cli: string, args: string[]) => args[1] === '+messages-reply' ? '{"error":{"code":3,"message":"未找到精确匹配的源消息 ID；未执行写入"}}' : '{"success":true}')
    expect(await sendDingTalkUser({ commands: { run } } as never, reply())).toBe('sent')
    expect(run).toHaveBeenCalledTimes(2)
    expect(run.mock.calls[1]![1]).toEqual(expect.arrayContaining(['+messages-send', '--group', 'cid_1']))
  })
  it('retries a group reply without the mention when the CLI refuses the mention target', async () => {
    const run = vi.fn(async (_cli: string, args: string[]) => args.includes('--at-open-dingtalk-ids')
      ? '{"error":{"code":3,"message":"--at-open-dingtalk-ids 收到的值不符合当前 D 版本 openDingTalkId 格式"}}' : '{"success":true}')
    expect(await sendDingTalkUser({ commands: { run } } as never, reply())).toBe('sent')
    expect(run).toHaveBeenCalledTimes(2)
    expect(run.mock.calls[1]![1]).not.toContain('--at-open-dingtalk-ids')
  })
  it('answers gone only for a vanished target and throws other failures for the Outbox to retry', async () => {
    const gone = vi.fn(async () => { throw new CliError('group not found (404)', 404) })
    expect(await sendDingTalkUser({ commands: { run: gone } } as never, reply())).toBe('gone')
    const failed = vi.fn(async () => { throw new CliError('rate limited', 130101) })
    await expect(sendDingTalkUser({ commands: { run: failed } } as never, reply())).rejects.toThrow('rate limited')
    const errorJson = vi.fn(async () => '{"error":{"code":404,"message":"conversation not found"}}')
    await expect(sendDingTalkUser({ commands: { run: errorJson } } as never, reply())).resolves.toBe('gone')
  })
})

describe('dingtalk-user account and listener', () => {
  it('reads the dedicated account from auth status, accepting a configured override', async () => {
    const run = vi.fn(async () => JSON.stringify({ success: true, authenticated: true, data: { identity: { openDingtalkId: 'open_self' } } }))
    expect(await dingtalkUserAccount({ commands: { run } } as never, config())).toEqual({ authenticated: true, selfOpenDingtalkId: 'open_self' })
    expect(findOpenDingtalkId({ nested: [{ open_dingtalk_id: 'open_x' }] })).toBe('open_x')
    expect(await dingtalkUserAccount({ commands: { run } } as never, config({ selfOpenDingtalkId: 'open_override' })))
      .toEqual({ authenticated: true, selfOpenDingtalkId: 'open_override' })
    const signedOut = vi.fn(async () => '{"success":true,"authenticated":false,"message":"未登录"}')
    expect(await dingtalkUserAccount({ commands: { run: signedOut } } as never, config())).toEqual({ authenticated: false, selfOpenDingtalkId: '' })
  })
  it('spawns a listener, takes in its line, and stops on abort', async () => {
    const directory = mkdtempSync(join(tmpdir(), 'dingtalk-user-'))
    const cli = join(directory, 'dws')
    writeFileSync(cli, ['#!/bin/sh', 'if [ "$1" = "auth" ]; then', '  echo \'{"success":true,"authenticated":true,"data":{"openDingtalkId":"open_self"}}\'',
      '  exit 0', 'fi', 'echo "[event] ready" >&2', `echo '${JSON.stringify({ type: 'message', data: JSON.stringify(event()) })}'`, 'sleep 60', ''].join('\n'))
    chmodSync(cli, 0o755)
    try {
      const h = host(config({ cli, groups: [{ conversationId: 'cid_1', listen: 'all' }] }))
      vi.mocked(h.ctx.commands.run).mockResolvedValue('{"success":true,"authenticated":true,"data":{"openDingtalkId":"open_self"}}')
      const controller = new AbortController()
      const running = watch(h.ctx, controller.signal)
      await vi.waitFor(() => expect(h.receive).toHaveBeenCalledTimes(1), { timeout: 5000 })
      const trigger = h.receive.mock.calls[0]![0] as Trigger
      expect(trigger.id).toBe(dingtalkUserTriggerId('msg_1'))
      expect(trigger).not.toHaveProperty('addressed')
      expect(h.reports.some(report => report.health.state === 'ready')).toBe(true)
      controller.abort()
      await running
    } finally { rmSync(directory, { recursive: true, force: true }) }
  })
  it('reports a signed-out account instead of listening', async () => {
    const h = host()
    vi.mocked(h.ctx.commands.run).mockResolvedValue('{"success":true,"authenticated":false,"message":"未登录"}')
    const controller = new AbortController()
    const running = watch(h.ctx, controller.signal)
    await vi.waitFor(() => expect(h.reports.some(report => report.health.state === 'error' && /signed in/.test(report.health.detail))).toBe(true), { timeout: 5000 })
    controller.abort()
    await running
  })
})
