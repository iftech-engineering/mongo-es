import { parse, format } from 'url'
import { Readable } from 'stream'

import { Timestamp, Cursor, Db, MongoClient, ObjectID } from 'mongodb'

import { Document } from './types'
import { Config, Task } from './config'

export default class MongoDB {
  private dbs: {
    [name: string]: Db
  }

  private constructor(dbs: {
    [name: string]: Db
  }) {
    this.dbs = dbs
  }

  public static async init({ mongodb, tasks }: Config): Promise<MongoDB> {
    const dbs = {}
    const url = parse(mongodb.url)
    url.pathname = `/local`
    dbs['local'] = await MongoClient.connect(format(url), mongodb.options)
    for (let task of tasks) {
      const url = parse(mongodb.url)
      url.pathname = `/${task.extract.db}`
      dbs[task.extract.db] = await MongoClient.connect(format(url), mongodb.options)
    }
    return new MongoDB(dbs)
  }

  public getOplog(task: Task): Cursor {
    return this.dbs['local'].collection('oplog.rs')
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

  public getCollection(task: Task): Readable {
    return this.dbs[task.extract.db].collection(task.extract.collection)
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

  public async retrieve(task: Task, id: ObjectID): Promise<Document | null> {
    try {
      const doc = await this.dbs[task.extract.db].collection(task.extract.collection).findOne({
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
