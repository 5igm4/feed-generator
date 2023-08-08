import fetch from 'node-fetch';
import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'

const fetchUsers = process.env.FETCH_USERS?.split(',') || [];
const result: string[] = [];


// const SUPER_ACCOUNTS = [
//   'did:plc:ul3dm3b76hu2cqla3qxb4v6u',
//   'did:plc:nyxviptfyic2lvuorkr5yy3y',
//   'did:plc:k4jt6heuiamymgi46yeuxtpt',
//   'did:plc:uewxgchsjy4kmtu7dcxa77us',
//   'did:plc:eclio37ymobqex2ncko63h4r',
//   'did:plc:k5nskatzhyxersjilvtnz4lh',
//   'did:plc:lia4ywzl2c2kt4dn3kzbywog',
//   'did:plc:k5nskatzhyxersjilvtnz4lh',
//   'did:plc:uzyjut26gy6mcnkf3hfv25sp',
//   'did:plc:ank6pz6hhgvrtmwg7rmmw4fo',
//   'did:plc:yun5yzvbtyt6oge4h35rg2jd',
//   'did:plc:ivseckn32uuldbxyt3qqlsyv',
//   'did:plc:rjtifgmgwnacwa24lqp4tpsy',
//   'did:plc:n2kurkne2vwpccuqwz37ggdm',
//   'did:plc:5o6k7jvowuyaquloafzn3cfw',
//   'did:plc:htnh75yxvugm7c54squy5jrr',
//   'did:plc:7gk35tkrkugkdcqdteviyara',
//   'did:plc:gagt3d5dp2xtw2layuu5uz2s'
// ];

// const SELECTIVE_ACCOUNTS = [
//   'did:plc:7q4nnnxawajbfaq7to5dpbsy'
// ];

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  private SUPER_ACCOUNTS: string[];
  private SELECTIVE_ACCOUNTS: string[];
  private SELECTIVE_EMOJIS: string [];

  async loadConfig(): Promise<void> {
    const fetchUsers = process.env.FETCH_USERS?.split(',') || [];
    const selectiveUsers = process.env.FETCH_SELECTIVE_USERS?.split(',') || [];
    
    this.SELECTIVE_EMOJIS = process.env.FETCH_SELECTIVE_EMOJI?.split(',') || [];
    this.SUPER_ACCOUNTS = await this.loadAccounts(fetchUsers);
    this.SELECTIVE_ACCOUNTS = await this.loadAccounts(selectiveUsers);
  }

  async loadAccounts(fetchUsers: string[]): Promise<string[]> {
    const result: string [] = [];
    const promises = fetchUsers.map((user) => {
      return (async () => {
        const res = await fetch(`https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle=${user}`);
        const did = (await res.json() as any).did;
        result.push(did);
      })();
    });
    await Promise.all(promises);
    return result;
  }

  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return
    const ops = await getOpsByType(evt)

    const postsToDelete = ops.posts.deletes.map((del) => del.uri)
    const postsToCreate = ops.posts.creates
      .filter((create) => {
        // only posts that meet one of our critera
        return this.SUPER_ACCOUNTS.includes(create.author) || this.filterPostByAuthorAndEmojis(create.record.text, create.author, this.SELECTIVE_EMOJIS, this.SELECTIVE_ACCOUNTS);
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

  private filterPostByAuthorAndEmojis(text: string, author: string, emojis: string[], authors: string[]): boolean {
    return authors.includes(author) && this.filterTextForEmojis(text, emojis);
  }

  private filterTextForEmojis(text: string, emojis: string[]): boolean {
    for (const char of text) {
      if (emojis.includes(char)) {
        return true;
      }
    }
    return false;
  }
}
