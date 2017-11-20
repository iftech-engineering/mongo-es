import { Readable } from 'stream'

import { Observable } from 'rx'
import { forEach, size, get, set, unset, has, keys, compact, map, reduce, isEmpty } from 'lodash'

import { Task, Controls, CheckPoint } from './config'
import Elasticsearch from './elasticsearch'
import MongoDB from './mongodb'
import { IR, Document, OpLog } from './types'

export default class Processor {
  private task: Task
  private controls: Controls
  private static provisionedReadCapacity: number
  private static consumedReadCapacity: number

  constructor(task: Task, controls: Controls) {
    this.task = task
    this.controls = controls
    Processor.provisionedReadCapacity = controls.mongodbReadCapacity
    Processor.consumedReadCapacity = 0
  }

  private static controlReadCapacity(stream: Readable): Readable {
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

  public transformer(action: 'upsert' | 'delete', doc: Document): IR | null {
    if (action === 'delete') {
      return {
        action: 'delete',
        id: doc._id.toHexString(),
        parent: this.task.transform.parent && get(doc, this.task.transform.parent),
      }
    }
    const data = reduce(this.task.transform.mapping, (obj, value, key) => {
      if (has(doc, key)) {
        set(obj, value, get(doc, key))
      }
      return obj
    }, {})
    if (isEmpty(data)) {
      return null
    }
    return {
      action: 'upsert',
      id: doc._id.toHexString(),
      data,
      parent: this.task.transform.parent && get(doc, this.task.transform.parent),
    }
  }

  public applyUpdate(doc: Document, $set: any = {}, $unset: any = {}): Document {
    forEach(this.task.transform.mapping, (value, key) => {
      if (has($unset, key)) {
        unset(doc, value)
      }
      if (has($set, key)) {
        set(doc, value, get($set, key))
      }
    })
    return doc
  }

  public ignoreUpdate(oplog: OpLog): boolean {
    let ignore = true
    if (oplog.op === 'u') {
      forEach(this.task.transform.mapping, (value, key) => {
        ignore = ignore && !(has(oplog.o, key) || has(oplog.o.$set, key) || has(oplog.o.$unset, key))
      })
    }
    return ignore
  }

  public scan(): Observable<Document> {
    return Observable.create<Document>((observer) => {
      try {
        const stream = Processor.controlReadCapacity(MongoDB.getCollection(this.task))
        stream.addListener('data', (doc: Document) => {
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

  public tail(): Observable<OpLog> {
    return Observable.create<OpLog>((observer) => {
      try {
        const cursor = MongoDB.getOplog(this.task)
        cursor.forEach((log: OpLog) => {
          observer.onNext(log)
        }, () => {
          observer.onCompleted()
        })
      } catch (err) {
        observer.onError(err)
      }
    })
  }

  public async oplog(oplog: OpLog): Promise<IR | null> {
    try {
      switch (oplog.op) {
        case 'i': {
          return this.transformer('upsert', oplog.o)
        }
        case 'u': {
          if (size(oplog.o2) !== 1 || !oplog.o2._id) {
            console.warn('oplog', 'cannot transform', oplog)
            return null
          }
          if (this.ignoreUpdate(oplog)) {
            console.debug('ignoreUpdate', oplog)
            return null
          }
          if (keys(oplog.o).filter(key => key.startsWith('$')).length === 0) {
            return this.transformer('upsert', {
              _id: oplog.o2._id,
              ...oplog.o,
            })
          }
          const old = this.task.transform.parent
            ? await Elasticsearch.search(this.task, oplog.o2._id)
            : await Elasticsearch.retrieve(this.task, oplog.o2._id)
          const doc = old
            ? this.applyUpdate(old, oplog.o.$set, oplog.o.$unset)
            : await MongoDB.retrieve(this.task, oplog.o2._id)
          return doc ? this.transformer('upsert', doc) : null
        }
        case 'd': {
          if (size(oplog.o) !== 1 || !oplog.o._id) {
            console.warn('oplog', 'cannot transform', oplog)
            return null
          }
          const doc = this.task.transform.parent
            ? await Elasticsearch.search(this.task, oplog.o._id)
            : oplog.o
          console.debug(doc)
          return doc ? this.transformer('delete', doc) : null
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

  public async load(irs: IR[]): Promise<void> {
    if (irs.length === 0) {
      return
    }
    const body: any[] = []
    irs.forEach((ir) => {
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
            'delete': {
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
    return await Elasticsearch.bulk({ body })
  }

  public async scanDocument(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.scan()
        .bufferWithTimeOrCount(1000, this.controls.elasticsearchBulkSize)
        .map(docs => compact<IR>(map(docs, doc => this.transformer('upsert', doc))))
        .subscribe(async (irs) => {
          if (irs.length === 0) {
            return
          }
          try {
            await this.load(irs)
            await Task.saveCheckpoint(this.task.name(), new CheckPoint({
              phase: 'scan',
              id: irs[0].id,
            }))
            console.log('scan', this.task.name(), irs.length, irs[0].id)
          } catch (err) {
            console.warn('scan', this.task.name(), err.message)
          }
        }, reject, resolve)
    })
  }

  public async tailOpLog(): Promise<never> {
    return new Promise<never>((resolve) => {
      this.tail()
        .bufferWithTimeOrCount(1000, 50)
        .flatMap((logs) => {
          return Observable.create<IR>(async (observer) => {
            for (let log of logs) {
              const ir = await this.oplog(log)
              if (ir) {
                observer.onNext(ir)
              }
            }
          })
        })
        .bufferWithTimeOrCount(1000, this.controls.elasticsearchBulkSize)
        .subscribe(async (irs) => {
          if (irs.length === 0) {
            return
          }
          try {
            await this.load(irs)
            await Task.saveCheckpoint(this.task.name(), new CheckPoint({
              phase: 'tail',
              time: Date.now() - 1000 * 10,
            }))
            console.log('tail', this.task.name(), irs.length)
          } catch (err) {
            console.warn('tail', this.task.name(), err.message)
          }
        }, (err) => {
          console.error('tail', this.task.name(), err)
        }, () => {
          console.error('tail', this.task.name(), 'should not complete')
          resolve()
        })
    })
  }
}
