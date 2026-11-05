/** Normalizes DWS `event +listen-im` messages into domain Triggers for the DingTalk user-identity Source. */
import { z } from 'zod'
import { key, type ChannelRef, type Trigger } from '../core/index.ts'

export const SOURCE = 'dingtalk-user'

/**
 * The business message fields DWS prints inside the transport envelope. `sender` and the quoted fields are display data;
 * only `message_id`, `conversation_id` and `sender_open_dingtalk_id` are identities.
 */
const QuotedSchema = z.looseObject({
  message_id: z.string().min(1), conversation_id: z.string().optional(), sender: z.string().optional(),
  sender_open_dingtalk_id: z.string().optional(), content: z.string().optional(), create_time: z.union([z.string(), z.number()]).optional(),
})
export const DingTalkUserEventSchema = z.looseObject({
  type: z.string().optional(), event_id: z.string().optional(),
  message_id: z.string().min(1), conversation_id: z.string().min(1),
  sender: z.string().optional(), sender_open_dingtalk_id: z.string().min(1),
  content: z.string().default(''),
  create_time: z.union([z.string(), z.number()]).optional(), event_time: z.union([z.string(), z.number()]).optional(),
  quoted_message: QuotedSchema.nullish(), forward_messages: z.array(z.unknown()).optional(),
})
export type DingTalkUserEvent = z.infer<typeof DingTalkUserEventSchema>

/** How the subscription that delivered a message addressed the account; `all-group` messages are not addressed. */
export type Delivery = 'mention' | 'direct' | 'observed'

/** Reply destination for one DingTalk conversation; the CLI and profile travel with it because a Channel send receives no configuration. */
export interface DingTalkUserAddress { conversationId: string; messageId: string; senderOpenDingtalkId: string; direct: string
  cli: string; profile: string }
export const dingtalkUserChannel = (conversationId: string, messageId: string, senderOpenDingtalkId: string, direct: boolean,
  cli: string, profile: string): ChannelRef => ({ source: SOURCE, thread: `${SOURCE}:${conversationId}`,
  address: { conversationId, messageId, senderOpenDingtalkId, direct: direct ? 'true' : 'false', cli, ...profile ? { profile } : {} } })
/** Deterministic Trigger ID for one DingTalk message; re-delivery of the same message can never open a second Ticket. */
export const dingtalkUserTriggerId = (messageId: string): string => key(SOURCE, messageId)

/**
 * Parses a DWS timestamp: epoch seconds or milliseconds, or an ISO/local date string. DWS has printed epoch milliseconds;
 * an unreadable value fails the single message instead of inventing a time.
 */
export function dingtalkUserTime(value: string | number | undefined): string {
  if (value === undefined) throw new Error('The DingTalk message has no create_time')
  if (typeof value === 'number' || /^\d+$/.test(value)) {
    const number = Number(value)
    const milliseconds = number < 1e11 ? number * 1000 : number
    if (!Number.isFinite(milliseconds)) throw new Error(`Unreadable DingTalk message time: ${value}`)
    return new Date(milliseconds).toISOString()
  }
  const time = Date.parse(value)
  if (!Number.isFinite(time)) throw new Error(`Unreadable DingTalk message time: ${value}`)
  return new Date(time).toISOString()
}
/** Quotes a message body as a Markdown block; used only for context, never as a reply instruction. */
const quotedText = (content: string | undefined): string => {
  const text = content?.trim()
  return text ? text.split('\n').map(line => `> ${line}`).join('\n') : ''
}
/**
 * Converts one DWS event into a Trigger. A message the account itself sent produces none, so the agent's own replies cannot
 * re-trigger it; an event without the three stable identities is unreadable and produces none either.
 */
export function dingtalkUserTrigger(input: unknown, delivery: Delivery, account: { selfOpenDingtalkId?: string; cli: string; profile: string }): Trigger | undefined {
  const parsed = DingTalkUserEventSchema.safeParse(input)
  if (!parsed.success) return undefined
  const event = parsed.data
  if (account.selfOpenDingtalkId && event.sender_open_dingtalk_id === account.selfOpenDingtalkId) return undefined
  const direct = delivery === 'direct'
  const quote = event.quoted_message ?? undefined
  const body = event.content.trim()
  const text = [body, quotedText(quote?.content)].filter(Boolean).join('\n\n')
  return { id: dingtalkUserTriggerId(event.message_id), source: SOURCE,
    threads: [{ source: SOURCE, id: event.conversation_id }],
    actor: { source: SOURCE, id: event.sender_open_dingtalk_id }, human: true,
    ...(delivery === 'mention' ? { addressed: 'mention' as const } : direct ? { addressed: 'direct' as const } : {}),
    at: dingtalkUserTime(event.create_time ?? event.event_time), text,
    channel: dingtalkUserChannel(event.conversation_id, event.message_id, event.sender_open_dingtalk_id, direct, account.cli, account.profile),
    meta: { conversationId: event.conversation_id, messageId: event.message_id, senderOpenDingtalkId: event.sender_open_dingtalk_id,
      direct, sender: event.sender ?? '', delivery,
      ...(quote?.message_id ? { quoteId: quote.message_id, ...(quote.content ? { quoteText: quote.content.slice(0, 500) } : {}) } : {}),
      ...(event.forward_messages?.length ? { forwardCount: event.forward_messages.length } : {}),
      ...(event.event_id ? { eventId: event.event_id } : {}) } }
}
/**
 * Unwraps one NDJSON line of `dws event +listen-im`, whose default output keeps the transport envelope and prints the
 * business payload as a JSON string in `data`. A line that already holds business fields is returned as is.
 */
export function dingtalkUserEventLine(line: string): unknown {
  const parsed: unknown = JSON.parse(line)
  if (!parsed || typeof parsed !== 'object') return undefined
  const envelope = parsed as { data?: unknown; type?: unknown; event_type?: unknown }
  const payload = typeof envelope.data === 'string' ? JSON.parse(envelope.data) as unknown
    : envelope.data && typeof envelope.data === 'object' ? envelope.data : undefined
  if (!payload || typeof payload !== 'object') return parsed
  const inner = payload as Record<string, unknown>
  // A transport envelope's own event type and id are the reliable ones for filtering.
  return { ...inner, ...(inner.type === undefined && typeof envelope.type === 'string' ? { type: envelope.type } : {}),
    ...(inner.event_type === undefined && envelope.event_type !== undefined ? { event_type: envelope.event_type } : {}) }
}
/** True for the message events the listener subscribes to; DWS can deliver read, reaction and recall events on the same stream. */
export const isMessageEvent = (event: unknown): boolean => {
  if (!event || typeof event !== 'object') return false
  const type = (event as { type?: unknown; event_type?: unknown }).type ?? (event as { event_type?: unknown }).event_type
  // The listener subscribes to message events only; read, recall, reaction and lifecycle events never start work.
  return type === undefined || !/read|recall|reaction|member|title|dismiss|lifecycle/i.test(String(type))
}
