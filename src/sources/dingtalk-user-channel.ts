/** DingTalk user-identity Channel: sends and quote-replies through the `dws` CLI with an idempotency key. */
import type { OutboundMessage } from '../core/index.ts'
import { CliError } from '../infra/commands.ts'
import { checkedImText } from '../infra/text.ts'
import type { SourceContext } from './host.ts'
import type { ChannelLink, ChannelLinks } from './kind.ts'
import type { DingTalkUserAddress } from './dingtalk-user-trigger.ts'
import { dingtalkUserTriggerId } from './dingtalk-user-trigger.ts'

/** Errors that mean the target can never receive a reply; everything else is retried by the Outbox. */
const GONE = /(group|chat|conversation|message).*(not found|does not exist|no longer|dismissed|disbanded|dissolved)|群(聊)?(不存在|已解散)|会话不存在|机器人(不在|已被移出)|not a member|invalid\.openConversationId/i
const MISSING_SOURCE = /(source|original|quoted) message.*(not found|does not exist|missing)|原消息.*(不存在|已撤回)|未找到精确匹配的源消息|invalid.*message.*id/i
/** A mention target the CLI cannot accept; the reply is still worth sending without the mention. */
const BAD_MENTION = /--at-open-dingtalk-ids|openDingTalkId.*(格式|format)|target_type_mismatch/i
const gone = (error: unknown): boolean => error instanceof CliError && (error.code === 404 || GONE.test(error.message))
/** The CLI reports some failures as a JSON `error` object while exiting 0, so every answer is inspected. */
export function dingtalkUserResult(output: string): Record<string, unknown> {
  let value: unknown
  try { value = JSON.parse(output.slice(output.indexOf('{'))) }
  catch { throw new CliError('dws returned no readable result') }
  if (!value || typeof value !== 'object') throw new CliError('dws returned no readable result')
  const result = value as { error?: { code?: number; message?: string } }
  if (result.error) {
    const code = typeof result.error.code === 'number' ? result.error.code : undefined
    throw new CliError(`${result.error.message ?? 'dws command failed'}${code ? ` (${code})` : ''}`, code)
  }
  return value as Record<string, unknown>
}
/** Sends one Outbox message once. `message.id` is the DWS idempotency key, so an Outbox retry cannot duplicate it. */
export async function sendDingTalkUser(host: Pick<SourceContext, 'commands'>, message: OutboundMessage): Promise<'sent' | 'gone'> {
  const address = message.channel.address as Partial<DingTalkUserAddress>
  const conversationId = address.conversationId ?? ''
  const sender = address.senderOpenDingtalkId ?? ''
  const cli = address.cli || 'dws'
  if (!conversationId || !sender) throw new Error('Missing DingTalk reply target')
  const text = checkedImText(message.text)
  const tail = ['--as', 'user', '--idempotency-key', message.id, '--wait-delivery', '--format', 'json',
    ...address.profile ? ['--profile', address.profile] : []]
  const direct = address.direct === 'true'
  const quoted = address.messageId ?? ''
  const sendArgs = (): string[] => direct
    ? ['chat', '+messages-send', '--open-dingtalk-id', sender, '--markdown', text, ...tail]
    : ['chat', '+messages-send', '--group', conversationId, '--markdown', text, ...tail]
  // A group reply quotes the request and truly @s its author; the fallbacks keep the result deliverable.
  const replyArgs = (mention: boolean): string[] => ['chat', '+messages-reply', '--group', conversationId, '--message-id', quoted,
    '--content', text, ...mention ? ['--at-open-dingtalk-ids', sender] : [], ...tail]
  try {
    const args = direct || !quoted ? sendArgs() : replyArgs(true)
    try { dingtalkUserResult(await host.commands.run(cli, args)) }
    catch (error) {
      if (direct || !quoted || !(error instanceof CliError)) throw error
      if (MISSING_SOURCE.test(error.message)) { dingtalkUserResult(await host.commands.run(cli, sendArgs())); return 'sent' }
      if (BAD_MENTION.test(error.message)) { dingtalkUserResult(await host.commands.run(cli, replyArgs(false))); return 'sent' }
      throw error
    }
    return 'sent'
  } catch (error) {
    if (gone(error)) return 'gone'
    throw error
  }
}
/** Links a recorded conversation; DingTalk's clients have no stable public chat URL, so the label alone identifies it. */
export const dingtalkUserLink = (address: Record<string, string>, _links: ChannelLinks): ChannelLink => ({
  url: '', label: { zh: `钉钉会话 ${address.conversationId ?? ''}`, en: `DingTalk conversation ${address.conversationId ?? ''}` },
  ...address.messageId ? { triggerId: dingtalkUserTriggerId(address.messageId) } : {},
})
