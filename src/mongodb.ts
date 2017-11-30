import { parse, format } from 'url'
import { Readable } from 'stream'
import * as _ from 'lodash'

import { Timestamp, Cursor, MongoClient, ObjectID, Collection } from 'mongodb'

import { Document } from './types'
import { Task, MongoConfig } from './config'

export default class MongoDB {
  static oplog: Collection
  collection: Collection
  task: Task
  retrieveBuffer: { [id: string]: ((doc: Document | null) => void)[] } = {}
  retrieveRunning: boolean = false

  private constructor(collection: Collection, task: Task) {
    this.collection = collection
    this.task = task
  }

  static async init(mongodb: MongoConfig, task: Task): Promise<MongoDB> {
    const url = parse(mongodb.url)
    url.pathname = `/${task.extract.db}`
    const collection = (await MongoClient.connect(format(url), mongodb.options)).collection(task.extract.collection)
    if (!MongoDB.oplog) {
      url.pathname = '/local'
      MongoDB.oplog = (await MongoClient.connect(format(url), mongodb.options)).collection('oplog.rs')
    }
    return new MongoDB(collection, task)
  }

  getCollection(): Readable {
    return this.collection
      .find({
        ...this.task.extract.query,
        _id: {
          $gte: this.task.from.id,
        },
      })
      .project(this.task.extract.projection)
      .stream()
  }

  getOplog(): Cursor {
    return MongoDB.oplog
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
    return new Promise<Document | null>((resolve) => {
      this.retrieveBuffer[id.toHexString()] = this.retrieveBuffer[id.toHexString()] || []
      this.retrieveBuffer[id.toHexString()].push(resolve)
      if (!this.retrieveRunning) {
        this.retrieveRunning = true
        setTimeout(this._retrieve.bind(this), 1000)
      }
    })
  }

  async _retrieve(): Promise<void> {
    const ids = _.take(_.keys(this.retrieveBuffer), 1024)
    if (ids.length === 0) {
      this.retrieveRunning = false
      return
    }
    const docs = await this._retrieveBatchSafe(ids)
    ids.forEach((id) => {
      const cbs = this.retrieveBuffer[id]
      delete this.retrieveBuffer[id]
      cbs.forEach((cb) => {
        cb(docs[id] || null)
      })
    })
    setTimeout(this._retrieve.bind(this), 1000)
  }

  async _retrieveBatchSafe(ids: string[]): Promise<{ [id: string]: Document }> {
    try {
      const docs = await this.collection.find<Document>({
        _id: {
          $in: ids.map(ObjectID.createFromHexString),
        },
      }).toArray()
      console.debug('retrieve from mongodb', docs)
      return _.keyBy(docs, doc => doc._id.toHexString())
    } catch (err) {
      console.warn('retrieve from mongodb', this.task.name(), ids, err)
      return {}
    }
  }
}
