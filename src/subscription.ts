import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

const SUPER_ACCOUNTS = [
  'did:plc:ul3dm3b76hu2cqla3qxb4v6u',
  'did:plc:nyxviptfyic2lvuorkr5yy3y',
  'did:plc:k4jt6heuiamymgi46yeuxtpt',
  'did:plc:uewxgchsjy4kmtu7dcxa77us',
  'did:plc:eclio37ymobqex2ncko63h4r',
  'did:plc:k5nskatzhyxersjilvtnz4lh',
  'did:plc:lia4ywzl2c2kt4dn3kzbywog',
  'did:plc:k5nskatzhyxersjilvtnz4lh',
  'did:plc:uzyjut26gy6mcnkf3hfv25sp',
  'did:plc:ank6pz6hhgvrtmwg7rmmw4fo',
  'did:plc:yun5yzvbtyt6oge4h35rg2jd',
  'did:plc:ivseckn32uuldbxyt3qqlsyv',
  'did:plc:rjtifgmgwnacwa24lqp4tpsy',
  'did:plc:n2kurkne2vwpccuqwz37ggdm',
  'did:plc:5o6k7jvowuyaquloafzn3cfw',
  'did:plc:htnh75yxvugm7c54squy5jrr',
  'did:plc:7gk35tkrkugkdcqdteviyara',
  'did:plc:gagt3d5dp2xtw2layuu5uz2s'
];

const SELECTIVE_ACCOUNTS = [
  'did:plc:7q4nnnxawajbfaq7to5dpbsy'
];

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    // This logs the text of every post off the firehose.
    // Just for fun :)
    // Delete before actually using
    // for (const post of ops.posts.creates) {
    //   console.log(post.author);
    // }

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only posts that meet one of our critera
        return SUPER_ACCOUNTS.includes(create.author) || this.filterPostByAuthorAndEmoji(create.record.text, create.author, '📰', SELECTIVE_ACCOUNTS);
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

  private filterPostByAuthorAndEmoji(text: string, author: string, emoji: string, authors: string[]): boolean {
    return authors.includes(author) && text.includes(emoji);
  }
}
