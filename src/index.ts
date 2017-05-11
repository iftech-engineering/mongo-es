import 'source-map-support/register'
import { parse, format } from 'url'
import { readFile } from 'fs'
import { resolve as resolvePath } from 'path'
import { forEach } from 'lodash'
import { scan, tail } from './extract'
import { document, oplog } from './transform'
import { Config } from './types'
import { init } from './models'

async function readConfig(path: string): Promise<Config> {
  return new Promise<Config>((resolve, reject) => {
    readFile(resolvePath(path), 'utf8', (err, str) => {
      err ? reject(err) : resolve(JSON.parse(str))
    })
  })
}

async function run() {
  const config = await readConfig(process.argv[2])
  await init(config)
  const now = new Date(0)
  forEach(config.tasks, task => {
    scan(task.extract, config.mongo.provisionedReadCapacity).subscribe((doc) => {
      console.log(task.extract.collection, document(task, doc))
    }, console.error, () => {
      tail(task.extract, now, config.mongo.provisionedReadCapacity).subscribe(async (log) => {
        console.log(task.extract.collection, await oplog(task, log))
      }, console.error)
    })
  })
}

run()
