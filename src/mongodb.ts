import { parse, format } from 'url'
import { Readable } from 'stream'

import { Timestamp, Cursor, Db, MongoClient, ObjectID, Collection } from 'mongodb'

import { Document } from './types'
import { Config, Task } from './config'

export default class MongoDB {
  dbs: {
    [name: string]: Db
  }
  oplog: Collection

  constructor(dbs: { [name: string]: Db }, oplog: Collection) {
    this.dbs = dbs
    this.oplog = oplog
  }

  static async init({ mongodb, tasks }: Config): Promise<MongoDB> {
    const url = parse(mongodb.url)
    url.pathname = `/local`
    const oplog = (await MongoClient.connect(format(url), mongodb.options)).collection('oplog.rs')
    const dbs = {}
    for (let task of tasks) {
      const url = parse(mongodb.url)
      url.pathname = `/${task.extract.db}`
      dbs[task.extract.db] = await MongoClient.connect(format(url), mongodb.options)
    }
    return new MongoDB(dbs, oplog)
  }

  getOplog(task: Task): Cursor {
    return this.oplog
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

  getCollection(task: Task): Readable {
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

  async retrieve(task: Task, id: ObjectID): Promise<Document | null> {
    try {
      const doc = await this.dbs[task.extract.db].collection(task.extract.collection).findOne({
        _id: id,
      })
      console.debug('retrieve from mongodb', doc)
      return doc
    } catch (err) {
      console.log('retrieve from mongodb', task.name(), id, err)
      return null
    }
  }
}
