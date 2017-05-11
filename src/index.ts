import 'source-map-support/register'
import { readFile } from 'fs'
import { resolve as resolvePath } from 'path'
import { forEach } from 'lodash'
import { scan, tail } from './extract'
import { document } from './transform'
import { Config } from './types'

async function readConfig(path: string): Promise<Config> {
  return new Promise<Config>((resolve, reject) => {
    readFile(resolvePath(path), 'utf8', (err, str) => {
      err ? reject(err) : resolve(JSON.parse(str))
    })
  })
}

async function run() {
  const config = await readConfig(process.argv[2])
  forEach(config.tasks, task => {
    scan(config.mongo, task.extract).subscribe(async (doc) => {
      console.log(task.extract.collection, await document(task.transform, doc))
    }, console.error)
    // tail(config.mongo, task.extract, new Date()).subscribe(console.log, console.error)
  })
}

run()
