import { parse, format } from 'url'
import { Timestamp } from 'bson'
import { Observable } from 'rx'
import { MongoClient } from 'mongodb'
import { mapValues } from 'lodash'
import { MongoConfig, ExtractTask } from './types'
import { controlReadCapacity } from './utils'

export function scan(config: MongoConfig, task: ExtractTask): Observable<any> {
  return Observable.create(async (observer) => {
    try {
      const url = parse(config.url)
      url.pathname = `/${task.db}`
      const db = await MongoClient.connect(format(url), config.options)
      const collection = db.collection(task.collection)
      const stream = collection
        .find(task.query)
        .project(task.projection)
        .sort(task.sort)
        .stream()
      controlReadCapacity(stream, config.provisionedReadCapacity)
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

export function tail(config: MongoConfig, task: ExtractTask, from: Date): Observable<any> {
  return Observable.create(async (observer) => {
    try {
      const url = parse(config.url)
      url.pathname = '/local'
      const db = await MongoClient.connect(format(url), config.options)
      const oplog = db.collection('oplog.rs')
      const stream = oplog.find({
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
        }).stream()
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
