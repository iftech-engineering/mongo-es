import { forEach, map, compact } from 'lodash'
import { Observable } from 'rx'

import { bulk } from './load'
import { IntermediateRepresentation } from './types'
import { Config, MongoDB, Elasticsearch, Extract, Transform } from './models'
import { Controls, Task } from './models/Config'

async function scanDocument(controls: Controls, task: Task, extract: Extract, transform: Transform): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    extract.scan()
      .bufferWithTimeOrCount(1000, controls.elasticsearchBulkSize)
      .subscribe(async (docs) => {
        if (docs.length === 0) {
          return
        }
        try {
          await bulk(task, compact<any>(map(docs, doc => transform.document(doc))))
          console.log('scan', task.name(), docs.length, docs[0]._id.toHexString())
        } catch (err) {
          console.warn('scan', task.name(), err.message)
        }
      }, reject, resolve)
  })
}

async function tailOpLog(controls: Controls, task: Task, extract: Extract, transform: Transform): Promise<never> {
  return new Promise<never>((resolve) => {
    extract.tail()
      .bufferWithTimeOrCount(1000, 50)
      .flatMap((logs) => {
        return Observable.create<IntermediateRepresentation>(async (observer) => {
          for (let log of logs) {
            const doc = await transform.oplog(log)
            if (doc) {
              observer.onNext(doc)
            }
          }
        })
      })
      .bufferWithTimeOrCount(1000, controls.elasticsearchBulkSize)
      .subscribe(async (docs) => {
        if (docs.length === 0) {
          return
        }
        try {
          await bulk(task, docs)
          console.log('tail', task.name(), docs.length)
        } catch (err) {
          console.warn('tail', task.name(), err.message)
        }
      }, (err) => {
        console.error('tail', task.name(), err)
      }, () => {
        console.error('tail', task.name(), 'should not complete')
        resolve()
      })
  })
}

async function runTask(config: Config, task: Task) {
  const extract = new Extract(task, config.controls.mongodbReadCapacity)
  const transform = new Transform(task)
  if (task.from.phase === 'scan') {
    try {
      console.log('scan', task.name(), 'start', 'from', task.from.id)
      await scanDocument(config.controls, task, extract, transform)
      console.log('scan', task.name(), 'end')
    } catch (err) {
      console.error('scan', err)
    }
  }
  console.log('tail', task.name(), 'start', 'from', task.from.time)
  await tailOpLog(config.controls, task, extract, transform)
}

export async function run(config: Config) {
  console.debug(JSON.stringify(config, null, 2))

  await MongoDB.init(config)
  await Elasticsearch.init(config)
  console.log('run', new Date())

  for (let index of config.elasticsearch.indices) {
    index.index += config.controls.indexNameSuffix
    if (!await Elasticsearch.exists(index)) {
      await Elasticsearch.create(index)
      console.log('create index', index.index)
    }
  }

  for (let index in config.tasks) {
    const task = config.tasks[index]
    task.load.index += config.controls.indexNameSuffix
    await Elasticsearch.putMapping(task.load)
    console.log('put mapping', task.load.index, task.load.type)
  }

  forEach(config.tasks, async (task) => {
    await runTask(config, task)
  })
}

console.debug = process.env.NODE_ENV === 'dev' ? console.log : () => null
