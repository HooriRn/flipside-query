import { Flipside, QueryResultSet } from '@flipsidecrypto/sdk'
import dotenv from 'dotenv'
import sql from './sql'
import fs from 'fs'
import path from 'path'
dotenv.config()

const flipside = new Flipside(
  process.env.FLIPSIDE_API_KEY ?? '',
  'https://api-v2.flipsidecrypto.xyz',
)

function writeToCSV(result: QueryResultSet | undefined) {
  const dataDir = path.join(__dirname, '..', 'data')
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir)
  }

  if (result && result.records && result.records.length > 0) {
    const headers = Object.keys(result.records[0])
    const csvRows = [
      headers.join(','), // header row
      ...result.records.map((row: Record<string, unknown>) =>
        headers.map((h) => JSON.stringify(row[h] ?? '')).join(','),
      ),
    ]
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-')
    const filePath = path.join(dataDir, `result-${timestamp}.csv`)
    fs.writeFileSync(filePath, csvRows.join('\n'))
    console.log(`Saved CSV to ${filePath}`)
  } else {
    console.log('No records found.')
  }
}

async function main() {
  const result = await flipside.query.run({ sql: sql })

  writeToCSV(result)
}

main()
