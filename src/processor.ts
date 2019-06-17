import { Readable } from 'stream'

import { Observable } from 'rx'
import * as _ from 'lodash'
import { Timestamp } from 'mongodb'

import { Task, Controls, CheckPoint } from './config'
import { IR, MongoDoc, ESDoc, OpLog } from './types'
import Elasticsearch from './elasticsearch'
import MongoDB from './mongodb'

export default class Processor {
  static provisionedReadCapacity: number
  static consumedReadCapacity: number
  task: Task
  controls: Controls
  mongodb: MongoDB
  elasticsearch: Elasticsearch
  queue: OpLog[][] = []
  running: boolean = false

  constructor(task: Task, controls: Controls, mongodb: MongoDB, elasticsearch: Elasticsearch) {
    this.task = task
    this.controls = controls
    this.mongodb = mongodb
    this.elasticsearch = elasticsearch
    Processor.provisionedReadCapacity = controls.mongodbReadCapacity
    Processor.consumedReadCapacity = 0
  }

  static controlReadCapacity(stream: Readable): Readable {
    if (!Processor.provisionedReadCapacity) {
      return stream
    }
    const interval = setInterval(() => {
      Processor.consumedReadCapacity = 0
      stream.resume()
    }, 1000)
    stream.addListener('data', () => {
      Processor.consumedReadCapacity++
      if (Processor.consumedReadCapacity >= Processor.provisionedReadCapacity) {
        stream.pause()
      }
    })
    stream.addListener('end', () => {
      clearInterval(interval)
    })
    return stream
  }

  transformer(
    action: 'upsert' | 'delete',
    doc: MongoDoc | ESDoc,
    timestamp?: Timestamp,
    isESDoc: boolean = false,
  ): IR | null {
    if (action === 'delete') {
      return {
        action: 'delete',
        id: doc._id.toString(),
        parent: this.task.transform.parent && _.get(doc, this.task.transform.parent),
        timestamp: timestamp ? timestamp.getHighBits() : 0,
      }
    }

    const data = _.reduce(
      this.task.transform.mapping,
      (obj, value, key) => {
        if (isESDoc) {
          key = value
        }
        if (_.has(doc, key)) {
          _.set(obj, value, _.get(doc, key))
        }
        return obj
      },
      { ...this.task.transform.static } || {},
    )
    if (_.isEmpty(data)) {
      return null
    }
    return {
      action: 'upsert',
      id: doc._id.toString(),
      data,
      parent: this.task.transform.parent && _.get(doc, this.task.transform.parent),
      timestamp: timestamp ? timestamp.getHighBits() : 0,
    }
  }

  applyUpdateMongoDoc(
    doc: MongoDoc,
    set: { [key: string]: any } = {},
    unset: { [key: string]: any } = {},
  ): MongoDoc {
    _.forEach(this.task.transform.mapping, (ignored, key) => {
      if (_.get(unset, key)) {
        _.unset(doc, key)
      }
      if (_.has(set, key)) {
        _.set(doc, key, _.get(set, key))
      }
    })
    return doc
  }

  applyUpdateESDoc(
    doc: ESDoc,
    set: { [key: string]: any } = {},
    unset: { [key: string]: any } = {},
  ): ESDoc {
    _.forEach(this.task.transform.mapping, (value, key) => {
      if (_.get(unset, key)) {
        _.unset(doc, value)
      }
      if (_.has(set, key)) {
        _.set(doc, value, _.get(set, key))
      }
    })
    return doc
  }
  
  hasKeyStartsWith(o, key_name): boolean {
    return _.keys(o).find((key) => {
      return key.split('.', 1)[0] === key_name;
    }) ? true : false;
  }
  
  ignoreUpdate(oplog: OpLog): boolean {
    let ignore = true
    if (oplog.op === 'u') {
      _.forEach(this.task.transform.mapping, (value, key) => {
        ignore =
          ignore && !(this.hasKeyStartsWith(oplog.o, key) || this.hasKeyStartsWith(oplog.o.$set, key) || this.hasKeyStartsWith(oplog.o.$unset, key))
      })
    }
    return ignore
  }

  scan(): Observable<MongoDoc> {
    return Observable.create<MongoDoc>(observer => {
      try {
        const stream = Processor.controlReadCapacity(this.mongodb.getCollection())
        stream.addListener('data', (doc: MongoDoc) => {
          observer.onNext(doc)
        })
        stream.addListener('error', (err: Error) => {
          observer.onError(err)
        })
        stream.addListener('end', () => {
          observer.onCompleted()
        })
      } catch (err) {
        observer.onError(err)
      }
    })
  }

  tail(): Observable<OpLog> {
    return Observable.create<OpLog>(observer => {
      try {
        const cursor = this.mongodb.getOplog()
        cursor.forEach(
          (log: OpLog) => {
            observer.onNext(log)
          },
          () => {
            observer.onCompleted()
          },
        )
      } catch (err) {
        observer.onError(err)
      }
    })
  }

  async oplog(oplog: OpLog): Promise<IR | null> {
    try {
      switch (oplog.op) {
        case 'i': {
          return this.transformer('upsert', oplog.o, oplog.ts)
        }
        case 'u': {
          if (!oplog.o2._id) {
            console.warn('oplog', 'cannot transform', oplog)
            return null
          }
          if (this.ignoreUpdate(oplog)) {
            console.debug('ignoreUpdate', oplog)
            return null
          }
          if (_.keys(oplog.o).find(key => !key.startsWith('$'))) {
            return this.transformer(
              'upsert',
              {
                _id: oplog.o2._id,
                ...oplog.o,
              },
              oplog.ts,
            )
          }
          const old = this.task.transform.parent
            ? await this.elasticsearch.search(oplog.o2._id.toHexString())
            : await this.elasticsearch.retrieve(oplog.o2._id.toHexString())
          const doc = old
            ? this.applyUpdateESDoc(old, oplog.o.$set, oplog.o.$unset)
            : await this.mongodb.retrieve(oplog.o2._id)
          return doc ? this.transformer('upsert', doc, oplog.ts, !!old) : null
        }
        case 'd': {
          if (_.size(oplog.o) !== 1 || !oplog.o._id) {
            console.warn('oplog', 'cannot transform', oplog)
            return null
          }
          const doc = this.task.transform.parent
            ? await this.elasticsearch.search(oplog.o._id.toHexString())
            : oplog.o
          console.debug(doc)
          return doc ? this.transformer('delete', doc, oplog.ts) : null
        }
        default: {
          return null
        }
      }
    } catch (err) {
      console.error('oplog', err)
      return null
    }
  }

  async load(irs: IR[]): Promise<void> {
    if (irs.length === 0) {
      return
    }
    const body: any[] = []
    irs.forEach(ir => {
      switch (ir.action) {
        case 'upsert': {
          body.push({
            index: {
              _index: this.task.load.index,
              _type: this.task.load.type,
              _id: ir.id,
              _parent: ir.parent,
            },
          })
          body.push(ir.data)
          break
        }
        case 'delete': {
          body.push({
            delete: {
              _index: this.task.load.index,
              _type: this.task.load.type,
              _id: ir.id,
              _parent: ir.parent,
            },
          })
          break
        }
      }
    })
    return await this.elasticsearch.bulk({ body })
  }

  mergeOplogs(oplogs: OpLog[]): OpLog[] {
    const store: { [key: string]: OpLog } = {}
    for (let oplog of _.sortBy(oplogs, 'ts')) {
      switch (oplog.op) {
        case 'i': {
          store[oplog.ns + oplog.o._id.toString()] = oplog
          break
        }
        case 'u': {
          const key = oplog.ns + oplog.o2._id.toString()
          const log = store[key]
          if (log && log.op === 'i') {
            log.o = this.applyUpdateMongoDoc(log.o, oplog.o.$set, oplog.o.$unset)
            log.ts = oplog.ts
          } else if (log && log.op === 'u') {
            log.o = _.merge(log.o, oplog.o)
            log.ts = oplog.ts
          } else {
            store[key] = oplog
          }
          break
        }
        case 'd': {
          const key = oplog.ns + oplog.o._id.toString()
          if (store[key] && store[key].op === 'i') {
            delete store[key]
          } else {
            store[key] = oplog
          }
          break
        }
      }
    }
    return _.sortBy(_.map(store, oplog => oplog), 'ts')
  }

  async scanDocument(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.scan()
        .bufferWithTimeOrCount(
          this.controls.elasticsearchBulkInterval,
          this.controls.elasticsearchBulkSize,
        )
        .map(docs => _.compact<IR>(_.map(docs, doc => this.transformer('upsert', doc))))
        .subscribe(
          async irs => {
            if (irs.length === 0) {
              return
            }
            try {
              await this.load(irs)
              await Task.saveCheckpoint(
                this.task.name(),
                new CheckPoint({
                  phase: 'scan',
                  id: irs[0].id,
                }),
              )
              console.log('scan', this.task.name(), irs.length, irs[0].id)
            } catch (err) {
              console.warn('scan', this.task.name(), err.message)
            }
          },
          reject,
          resolve,
        )
    })
  }

  async tailOpLog(): Promise<never> {
    return new Promise<never>((resolve, reject) => {
      this.tail()
        .bufferWithTimeOrCount(
          this.controls.elasticsearchBulkInterval,
          this.controls.elasticsearchBulkSize,
        )
        .subscribe(
          oplogs => {
            this.queue.push(oplogs)
            if (!this.running) {
              this.running = true
              setImmediate(this._processOplog.bind(this))
            }
          },
          err => {
            console.error('tail', this.task.name(), err)
            reject(err)
          },
          () => {
            const err = new Error('should not complete')
            console.error('tail', this.task.name(), err)
            reject(err)
          },
        )
    })
  }

  async _processOplog() {
    if (this.queue.length === 0) {
      this.running = false
      return
    }
    while (this.queue.length > 0) {
      const oplogs = _.flatten(this.queue)
      this.queue = []
      await this._processOplogSafe(oplogs)
    }
    setImmediate(this._processOplog.bind(this))
  }

  async _processOplogSafe(oplogs) {
    try {
      const irs = _.compact(
        await Promise.all(
          this.mergeOplogs(oplogs).map(async oplog => {
            return await this.oplog(oplog)
          }),
        ),
      )
      if (irs.length > 0) {
        await this.load(irs)
        await Task.saveCheckpoint(
          this.task.name(),
          new CheckPoint({
            phase: 'tail',
            time: Date.now() - 1000 * 10,
          }),
        )
        console.log('tail', this.task.name(), irs.length, new Date(irs[0].timestamp * 1000))
      }
    } catch (err) {
      console.warn('tail', this.task.name(), err.message)
    }
  }
}
