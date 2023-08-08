import fetch from 'node-fetch'
import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

const fetchUsers = process.env.FETCH_USERS?.split(',') || []
const result: string[] = []

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  private SUPER_ACCOUNTS: string[]
  private SELECTIVE_ACCOUNTS: string[]
  private SELECTIVE_EMOJIS: string[]
  private SELECTIVE_PHRASES: string[]

  async loadConfig(): Promise<void> {
    const fetchUsers = process.env.FETCH_USERS?.split(',') || []
    const selectiveUsers = process.env.FETCH_SELECTIVE_USERS?.split(',') || []

    this.SELECTIVE_EMOJIS = process.env.FETCH_SELECTIVE_EMOJI?.split(',') || []
    this.SELECTIVE_PHRASES =
      process.env.FETCH_SELECTIVE_PHRASES?.split(',') || []
    this.SUPER_ACCOUNTS = await this.loadAccounts(fetchUsers)
    this.SELECTIVE_ACCOUNTS = await this.loadAccounts(selectiveUsers)
  }

  async loadAccounts(fetchUsers: string[]): Promise<string[]> {
    const result: string[] = []
    const promises = fetchUsers.map((user) => {
      return (async () => {
        const res = await fetch(
          `https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle=${user}`,
        )
        const did = ((await res.json()) as any).did
        result.push(did)
      })()
    })
    await Promise.all(promises)
    return result
  }

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only posts that meet one of our critera
        return (
          this.SUPER_ACCOUNTS.includes(create.author) ||
          this.filterPostByAuthorAndEmojis(create.record.text, create.author)
        )
        //
      })
      .map((create) => {
        // map alf-related posts to a db row
        return {
          uri: create.uri,
          cid: create.cid,
          replyParent: create.record?.reply?.parent.uri ?? null,
          replyRoot: create.record?.reply?.root.uri ?? null,
          indexedAt: new Date().toISOString(),
        }
      })

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }

  private filterPostByAuthorAndEmojis(text: string, author: string): boolean {
    return (
      this.SELECTIVE_ACCOUNTS.includes(author) &&
      (this.filterTextForEmojis(text, this.SELECTIVE_EMOJIS) ||
        this.filterTextForPhrases(text, this.SELECTIVE_PHRASES))
    )
  }

  private filterTextForEmojis(text: string, emojis: string[]): boolean {
    for (const char of text) {
      if (emojis.includes(char)) {
        return true
      }
    }
    return false
  }

  private filterTextForPhrases(text: string, phrases: string[]): boolean {
    for (const phrase of phrases) {
      if (text.toLowerCase().includes(phrase)) {
        return true
      }
    }
    return false
  }
}
