import { parse, format } from 'url'
import { Readable } from 'stream'

import { Timestamp, Cursor, MongoClient, ObjectID, Collection } from 'mongodb'

import { Document } from './types'
import { Task, MongoConfig } from './config'

export default class MongoDB {
  collection: Collection
  oplog: Collection
  task: Task

  constructor(collection: Collection, oplog: Collection, task: Task) {
    this.collection = collection
    this.oplog = oplog
    this.task = task
  }

  static async init(mongodb: MongoConfig, task: Task): Promise<MongoDB> {
    const url = parse(mongodb.url)
    url.pathname = `/${task.extract.db}`
    const collection = (await MongoClient.connect(format(url), mongodb.options)).collection(task.extract.collection)
    const oplog = (await MongoClient.connect(format(url), mongodb.options)).collection('oplog.rs')
    return new MongoDB(collection, oplog, task)
  }

  getCollection(): Readable {
    return this.collection
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
  }

  getOplog(): Cursor {
    return this.oplog
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
  }

  async retrieve(id: ObjectID): Promise<Document | null> {
    try {
      const doc = await this.collection.findOne({
        _id: id,
      })
      console.debug('retrieve from mongodb', doc)
      return doc
    } catch (err) {
      console.warn('retrieve from mongodb', this.task.name(), id, err)
      return null
    }
  }
}
