import 'source-map-support/register'
import { parse, format } from 'url'
import { readFile } from 'fs'
import { resolve as resolvePath } from 'path'
import { forEach, map, compact } from 'lodash'
import { Observable } from 'rx'
import { scan, tail } from './extract'
import { document, oplog } from './transform'
import { bulk, exists, putMapping, create } from './load'
import { Task, Config, Controls, IntermediateRepresentation } from './types'
import { init } from './models'

async function readConfig(path: string): Promise<Config> {
  return new Promise<Config>((resolve, reject) => {
    readFile(resolvePath(path), 'utf8', (err, str) => {
      err ? reject(err) : resolve(JSON.parse(str))
    })
  })
}

async function scanDocument(controls: Controls, task: Task): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    scan(task.extract, controls.mongoReadCapacity || 10000)
      .bufferWithTimeOrCount(1000, controls.elasticsearchBulkSize || 5000)
      .subscribe(async (docs) => {
        if (docs.length === 0) {
          return
        }
        try {
          await bulk(task.load, compact<any>(map(docs, doc => document(task, doc))))
          console.log('scan', docs.length)
        } catch (err) {
          console.warn('scan', err.message)
        }
      }, reject, resolve)
  })
}

async function tailOpLog(controls: Controls, task: Task, from: Date): Promise<never> {
  return new Promise<never>((resolve, reject) => {
    tail(task.extract, from, controls.mongoReadCapacity || 10000)
      .bufferWithTimeOrCount(1000, 50)
      .flatMap((logs) => {
        return Observable.create<IntermediateRepresentation>((observer) => {
          forEach(logs, async (log) => {
            const doc = await oplog(task, log)
            if (doc) {
              observer.onNext(doc)
            }
          })
        })
      })
      .bufferWithTimeOrCount(1000, controls.elasticsearchBulkSize || 5000)
      .subscribe(async (docs) => {
        if (docs.length === 0) {
          return
        }
        try {
          await bulk(task.load, docs)
          console.log('tail', docs.length)
        } catch (err) {
          console.warn('tail', err.message)
        }
      }, reject, resolve)
  })
}

async function run() {
  try {
    const config = await readConfig(process.argv[2])
    await init(config)
    const from = config.controls.tailFromTime
      ? new Date(config.controls.tailFromTime)
      : new Date()

    if (config.elasticsearch.index && ! await exists(config.elasticsearch.index.index)) {
      await create(config.elasticsearch.index)
      console.log('create index', config.elasticsearch.index.index)
      for (let task of config.tasks) {
        await putMapping(task.load)
        console.log('put mapping', task.load.type)
      }
    }

    forEach(config.tasks, async (task) => {
      if (!config.controls.tailFromTime) {
        try {
          console.log('scan', 'start', `Mongo: ${task.extract.db}.${task.extract.collection}`,
            '->', `Elasticsearch: ${task.load.index}.${task.load.type}`)
          await scanDocument(config.controls, task)
          console.log('scan', 'end')
        } catch (err) {
          console.error('scan', err)
        }
      }
      try {
        console.log('tail', 'start', `Mongo: ${task.extract.db}.${task.extract.collection}`,
          '->', `Elasticsearch: ${task.load.index}.${task.load.type}`, 'from', from)
        await tailOpLog(config.controls, task, from)
        console.error('tail', 'should not end')
      } catch (err) {
        console.error('tail', err)
      }
    })
  } catch (err) {
    console.error('run', err)
  }
}

console.debug = process.env.NODE_ENV === 'dev' ? console.log : () => null

run()
