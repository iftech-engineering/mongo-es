import { Readable } from 'stream'
import { Observable } from 'rx'

import { Document, OpLog, Timestamp } from '../types'
import { MongoDB } from '../models'
import { Task } from './Config'

export default class Extract {
  private task: Task
  private static provisionedReadCapacity: number
  private static consumedReadCapacity: number

  constructor(task: Task, provisionedReadCapacity: number) {
    this.task = task
    Extract.provisionedReadCapacity = provisionedReadCapacity
    Extract.consumedReadCapacity = 0
  }

  private static controlReadCapacity(stream: Readable): void {
    if (!Extract.provisionedReadCapacity) {
      return
    }
    setInterval(() => {
      Extract.consumedReadCapacity = 0
      stream.resume()
    }, 1000)
    stream.addListener('data', () => {
      Extract.consumedReadCapacity++
      if (Extract.consumedReadCapacity >= Extract.provisionedReadCapacity) {
        stream.pause()
      }
    })
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
        Extract.controlReadCapacity(stream)
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
}
