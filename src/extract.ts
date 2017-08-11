import { Readable } from 'stream'
import { Observable } from 'rx'

import { Document, OpLog, Timestamp } from './types'
import { MongoDB } from './models'
import { Task } from './models/Config'

let consumedReadCapacity = 0

function controlReadCapacity(stream: Readable, provisionedReadCapacity: number): void {
  if (!provisionedReadCapacity) {
    return
  }
  const timer = setInterval(() => {
    consumedReadCapacity = 0
    stream.resume()
  }, 1000)
  stream.addListener('data', () => {
    consumedReadCapacity++
    if (consumedReadCapacity >= provisionedReadCapacity) {
      stream.pause()
    }
  })
  stream.addListener('end', () => {
    clearInterval(timer)
  })
}

export function scan(task: Task, provisionedReadCapacity: number): Observable<Document> {
  return Observable.create<Document>((observer) => {
    try {
      const stream = MongoDB.getCollection(task.extract.db, task.extract.collection)
        .find({
          ...task.extract.query,
          _id: {
            $lte: task.from.id,
          },
        })
        .project(task.extract.projection)
        .sort({
          $natural: -1,
        })
        .stream()
      controlReadCapacity(stream, provisionedReadCapacity)
      stream.on('data', (doc: Document) => {
        observer.onNext(doc)
      })
      stream.on('error', (err) => {
        observer.onError(err)
      })
      stream.on('end', () => {
        observer.onCompleted()
      })
    } catch (err) {
      observer.onError(err)
    }
  })
}

export function tail(task: Task): Observable<OpLog> {
  return Observable.create<OpLog>((observer) => {
    try {
      const cursor = MongoDB.getOplog()
        .find({
          ns: `${task.extract.db}.${task.extract.collection}`,
          ts: {
            $gte: new Timestamp(0, task.from.time.getTime() / 1000),
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
      cursor.forEach((doc => {
        observer.onNext(doc)
      }), () => {
        observer.onCompleted()
      })
    } catch (err) {
      observer.onError(err)
    }
  })
}
