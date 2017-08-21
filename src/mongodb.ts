import { parse, format } from 'url'
import { Readable } from 'stream'

import { Timestamp, Cursor, Db, MongoClient, ObjectID } from 'mongodb'

import { Document } from './types'
import { Config, Task } from './config'

export default class MongoDB {
  private static dbs: {
    [name: string]: Db
  }

  private constructor() {
  }

  public static async init({ mongodb, tasks }: Config): Promise<void> {
    if (MongoDB.dbs) {
      return
    }
    MongoDB.dbs = {}
    const url = parse(mongodb.url)
    url.pathname = `/local`
    MongoDB.dbs['local'] = await MongoClient.connect(format(url), mongodb.options)
    for (let task of tasks) {
      const url = parse(mongodb.url)
      url.pathname = `/${task.extract.db}`
      MongoDB.dbs[task.extract.db] = await MongoClient.connect(format(url), mongodb.options)
    }
  }

  public static getOplog(task: Task): Cursor {
    return MongoDB.dbs['local'].collection('oplog.rs')
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
  }

  public static getCollection(task: Task): Readable {
    return MongoDB.dbs[task.extract.db].collection(task.extract.collection)
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
  }

  public static async retrieve(task: Task, id: ObjectID): Promise<Document | null> {
    try {
      const doc = await MongoDB.dbs[task.extract.db].collection(task.extract.collection).findOne({
        _id: id,
      })
      console.debug('retrieve from mongodb', doc)
      return doc
    } catch (err) {
      console.warn('retrieve from mongodb', task.name(), id, err)
      return null
    }
  }
}
