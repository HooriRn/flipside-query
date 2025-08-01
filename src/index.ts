import { Flipside } from '@flipsidecrypto/sdk'
import dotenv from 'dotenv'
import sql from './sql'
dotenv.config()

const flipside = new Flipside(
  process.env.FLIPSIDE_API_KEY ?? '',
  'https://api-v2.flipsidecrypto.xyz',
)

async function main() {
  const rest = await flipside.query.run({ sql: sql })
  console.log('Query Results:', rest)
}

main()
