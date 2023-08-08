import fetch from 'node-fetch'
import dotenv from 'dotenv'

;(async function main(): Promise<void> {
  dotenv.config()
  const fetchUsers = process.env.FETCH_USERS?.split(',') || []
  const result: string[] = []
  for (const user of fetchUsers) {
    const res = await fetch(
      `https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle=${user}`,
    )
    result.push(((await res.json()) as any).did)
  }
  console.log(result)
})()
