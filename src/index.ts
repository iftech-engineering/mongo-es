import 'source-map-support/register'
import { parse, format } from 'url'
import { readFile } from 'fs'
import { resolve as resolvePath } from 'path'
import { forEach, map } from 'lodash'
import { Observable } from 'rx'
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
    console.log('scan', 'start', `Mongo: ${task.extract.db}.${task.extract.collection}`, '->', `Elasticsearch: ${task.load.index}.${task.load.type}`)
    scan(task.extract, config.limitions.mongoReadCapacity)
      .bufferWithTimeOrCount(1000, config.limitions.elasticsearchBulkSize)
      .subscribe(async (docs) => {
        if (docs.length === 0) {
          return
        }
        try {
          await bulk(task.load, map(docs, doc => document(task, doc)))
          console.log('scan', docs.length)
        } catch (err) {
          console.error('scan', err)
        }
      }, (err) => {
        console.error('scan', err)
      }, () => {
        console.log('tail', 'start', `Mongo: ${task.extract.db}.${task.extract.collection}`, '->', `Elasticsearch: ${task.load.index}.${task.load.type}`)
        tail(task.extract, now, config.limitions.mongoReadCapacity)
          .bufferWithTimeOrCount(1000, 50)
          .flatMap((logs) => {
            return Observable.create<IntermediateRepresentation>((observer) => {
              forEach(logs, async (log) => {
                const doc = await oplog(task, log)
                if (doc) {
                  observer.onNext(doc)
                } else {
                  console.warn('tail', 'oplog not transformed', log)
                }
              })
            })
          })
          .bufferWithTimeOrCount(1000, config.limitions.elasticsearchBulkSize)
          .subscribe(async (docs) => {
            if (docs.length === 0) {
              return
            }
            try {
              await bulk(task.load, docs)
              console.log('tail', docs.length)
            } catch (err) {
              console.error('tail', err)
            }
          }, (err) => {
            console.error('tail', err)
          }, () => {
            console.error('tail', 'should not complete')
          })
      })
  })
}

run()
