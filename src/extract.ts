import { Readable } from 'stream'
import { Timestamp } from 'bson'
import { Observable } from 'rx'
import { mapValues } from 'lodash'
import { ExtractTask } from './types'
import { mongo, elasticsearch } from './models'

let consumedReadCapacity = 0

function controlReadCapacity(stream: Readable, provisionedReadCapacity?: number): void {
  if (!provisionedReadCapacity) {
    return
  }
  const timer = setInterval(() => {
    consumedReadCapacity = 0
    stream.resume()
  }, 1000)
  stream.addListener('data', (doc) => {
    consumedReadCapacity++
    if (consumedReadCapacity >= provisionedReadCapacity) {
      stream.pause()
    }
  })
  stream.addListener('end', () => {
    clearInterval(timer)
  })
}

export function scan(task: ExtractTask, provisionedReadCapacity?: number): Observable<any> {
  return Observable.create(async (observer) => {
    try {
      const stream = mongo()[task.db].collection(task.collection)
        .find(task.query)
        .project(task.projection)
        .sort(task.sort)
        .stream()
      controlReadCapacity(stream, provisionedReadCapacity)
      stream.on('data', (doc) => {
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

export function tail(task: ExtractTask, from: Date, provisionedReadCapacity?: number): Observable<any> {
  return Observable.create(async (observer) => {
    try {
      const stream = mongo()['local'].collection('oplog.rs')
        .find({
          ns: `${task.db}.${task.collection}`,
          ts: {
            $gte: new Timestamp(from.getTime(), 0),
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
        .stream()
      controlReadCapacity(stream, provisionedReadCapacity)
      stream.on('data', (doc) => {
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
