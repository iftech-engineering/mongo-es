import { Timestamp } from 'bson'
import { Observable } from 'rx'
import { MongoClient } from 'mongodb'
import { mapValues } from 'lodash'
import { Config } from './types'
import { speedLimit } from './utils'

export function scan(config: Config): Observable<any> {
  return Observable.create(async (observer) => {
    try {
      const db = await MongoClient.connect(`${config.mongo.uri}/${config.mongo.db}`, config.mongo.options)
      const collection = db.collection(config.mongo.collection)
      const stream = speedLimit(collection.find(config.mongo.query, config.mongo.fields).sort({
        $natural: -1,
      }).stream(), config.mongo.dps)
      stream.on('data', observer.onNext)
      stream.on('error', observer.onError)
      stream.on('end', observer.onCompleted)
    } catch (err) {
      observer.onError(err)
    }
  })
}

export function tail(config: Config, from: Timestamp): Observable<any> {
  return Observable.create(async (observer) => {
    try {
      const db = await MongoClient.connect(`${config.mongo.uri}/local`, config.mongo.options)
      const oplog = db.collection('oplog.rs')
      const stream = oplog.find({
        ns: `${config.mongo.db}.${config.mongo.collection}`,
        ts: {
          $gte: from.toInt(),
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
      stream.on('data', observer.onNext)
      stream.on('error', observer.onError)
      stream.on('end', observer.onCompleted)
    } catch (err) {
      observer.onError(err)
    }
  })
}
