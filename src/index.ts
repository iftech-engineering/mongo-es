#!/usr/bin/env node

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
import { taskName } from './utils'

async function readConfig(path: string): Promise<Config> {
  return new Promise<Config>((resolve, reject) => {
    readFile(resolvePath(path), 'utf8', (err, str) => {
      err ? reject(err) : resolve(JSON.parse(str))
    })
  })
}

async function scanDocument(controls: Controls, task: Task): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    scan(task.extract, controls.mongodbReadCapacity || 10000)
      .bufferWithTimeOrCount(1000, controls.elasticsearchBulkSize || 5000)
      .subscribe(async (docs) => {
        if (docs.length === 0) {
          return
        }
        try {
          await bulk(task.load, compact<any>(map(docs, doc => document(task, doc))))
          console.log('scan', taskName(task), docs.length)
        } catch (err) {
          console.warn('scan', taskName(task), err.message)
        }
      }, reject, resolve)
  })
}

async function tailOpLog(controls: Controls, task: Task, from: Date): Promise<never> {
  return new Promise<never>((resolve) => {
    tail(task.extract, from)
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
          console.log('tail', taskName(task), docs.length)
        } catch (err) {
          console.warn('tail', taskName(task), err.message)
        }
      }, (err) => {
        const oneMinuteAgo = new Date()
        oneMinuteAgo.setMinutes(oneMinuteAgo.getMinutes() - 1)
        console.error('tail', taskName(task), err)
        return tailOpLog(controls, task, oneMinuteAgo)
      }, () => {
        console.error('tail', taskName(task), 'should not complete')
        resolve()
      })
  })
}

async function runTask(config: Config, task: Task, from: Date) {
  if (!config.controls.tailFromTime) {
    try {
      console.log('scan', taskName(task), 'start')
      await scanDocument(config.controls, task)
      console.log('scan', taskName(task), 'end')
    } catch (err) {
      console.error('scan', err)
    }
  }
  console.log('tail', taskName(task), 'start', 'from', from)
  await tailOpLog(config.controls, task, from)
}

(async function run() {
  try {
    const config = await readConfig(process.argv[2])
    console.debug(JSON.stringify(config, null, 2))

    await init(config)
    const from = config.controls.tailFromTime
      ? new Date(config.controls.tailFromTime)
      : new Date()
    console.log('run', from)

    for (let index of config.elasticsearch.indices || []) {
      index.index += config.controls.indexNameSuffix || ''
      if (!await exists(index.index)) {
        await create(index)
        console.log('create index', index.index)
      }
    }

    for (let task of config.tasks) {
      task.load.index += config.controls.indexNameSuffix || ''
      await putMapping(task.load)
      console.log('put mapping', task.load.index, task.load.type)
    }

    forEach(config.tasks, (task) => {
      runTask(config, task, from)
    })
  } catch (err) {
    console.error('run', err)
  }
})()

console.debug = process.env.NODE_ENV === 'dev' ? console.log : () => null
