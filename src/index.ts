import 'source-map-support/register'
import { parse, format } from 'url'
import { readFile } from 'fs'
import { resolve as resolvePath } from 'path'
import { forEach, map, compact } from 'lodash'
import { scan, tail } from './extract'
import { document, oplog } from './transform'
import { bulk } from './load'
import { Config, IntermediateRepresentation } from './types'
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
    console.log('scan', 'start', task)
    scan(task.extract, config.mongo.provisionedReadCapacity)
      .bufferWithTimeOrCount(1000, 1000)
      .subscribe(async (docs) => {
        console.log('scan', docs.length)
      }, (err) => {
        console.error('scan', err)
      }, () => {
        console.log('tail', 'start', task)
        tail(task.extract, now, config.mongo.provisionedReadCapacity)
          .bufferWithTimeOrCount(1000, 1000)
          .subscribe((logs) => {
            console.log('scan', logs.length)
          }, (err) => {
            console.error('tail', err)
          }, () => {
            console.error('tail', 'should not complete')
          })
      })
  })
}

run()
