import { Readable } from 'stream'

import { Observable } from 'rx'
import { forEach, size, get, set, unset, has, keys } from 'lodash'

import { Task, Controls } from './Config'
import { Elasticsearch, MongoDB } from './index'
import { IntermediateRepresentation, Document, OpLog, Timestamp } from '../types'

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

  private static controlReadCapacity(stream: Readable): void {
    if (!Processor.provisionedReadCapacity) {
      return
    }
    setInterval(() => {
      Processor.consumedReadCapacity = 0
      stream.resume()
    }, 1000)
    stream.addListener('data', () => {
      Processor.consumedReadCapacity++
      if (Processor.consumedReadCapacity >= Processor.provisionedReadCapacity) {
        stream.pause()
      }
    })
  }

  public transformer(action: 'create' | 'update' | 'delete', doc: Document): IntermediateRepresentation | null {
    const IR: IntermediateRepresentation = {
      action,
      id: doc._id.toHexString(),
      data: {},
      parent: this.task.transform.parent && get<string>(doc, this.task.transform.parent)
    }
    if (action === 'delete') {
      return IR
    }
    forEach(this.task.transform.mapping, (value, key) => {
      if (has(doc, key)) {
        set(IR.data, value, get(doc, key))
      }
    })
    if (size(IR.data) === 0) {
      return null
    }
    return IR
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
        const stream = MongoDB.getCollection(this.task.extract.db, this.task.extract.collection)
          .find({
            ...this.task.extract.query,
            _id: {
              $lte: this.task.from.id,
            },
          })
          .project(this.task.extract.projection)
          .sort({
            $natural: -1,
          })
          .stream()
        Processor.controlReadCapacity(stream)
        stream.addListener('data', (doc: Document) => {
          observer.onNext(doc)
        })
        stream.addListener('error', (err: Error) => {
          observer.onError(err)
        })
        stream.addListener('end', () => {
          this.task.endScan()
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
        const cursor = MongoDB.getOplog()
          .find({
            ns: `${this.task.extract.db}.${this.task.extract.collection}`,
            ts: {
              $gte: new Timestamp(0, this.task.from.time.getTime() / 1000),
            },
            fromMigrate: {
              $exists: false,
            },
          }, {
            tailable: true,
            oplogReplay: true,
            noCursorTimeout: true,
            awaitData: true,
          })
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


  public async document(doc: Document): Promise<IntermediateRepresentation | null> {
    return await this.transformer('create', doc)
  }

  public async oplog(oplog: OpLog): Promise<IntermediateRepresentation | null> {
    try {
      switch (oplog.op) {
        case 'i': {
          return this.transformer('create', oplog.o)
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
            return this.transformer('update', {
              _id: oplog.o2._id,
              ...oplog.o,
            })
          }
          const old = this.task.transform.parent
            ? await Elasticsearch.search(this.task, oplog.o2._id)
            : await Elasticsearch.retrieve(this.task, oplog.o2._id)
          const doc = old ? this.applyUpdate(old, oplog.o.$set, oplog.o.$unset) : await MongoDB.retrieve(this.task, oplog.o2._id)
          return doc ? this.transformer('update', doc) : null
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

  public async load(IRs: IntermediateRepresentation[]): Promise<void> {
    if (IRs.length === 0) {
      return
    }
    const body: any[] = []
    IRs.forEach((IR) => {
      switch (IR.action) {
        case 'create':
        case 'update': {
          body.push({
            index: {
              _index: this.task.load.index,
              _type: this.task.load.type,
              _id: IR.id,
              _parent: IR.parent,
            },
          })
          body.push(IR.data)
          break
        }
        case 'delete': {
          body.push({
            delete: {
              _index: this.task.load.index,
              _type: this.task.load.type,
              _id: IR.id,
              _parent: IR.parent,
            }
          })
          break
        }
      }
    })
    return await Elasticsearch.bulk({ body })
  }
}
