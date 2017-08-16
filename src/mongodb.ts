import { parse, format } from 'url'

import { Collection, Db, MongoClient, ObjectID } from 'mongodb'

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

  public static getOplog(): Collection {
    return MongoDB.dbs['local'].collection('rs.oplog')
  }

  public static getCollection(db: string, collection: string): Collection {
    return MongoDB.dbs[db].collection(collection)
  }

  public static async retrieve(task: Task, id: ObjectID): Promise<Document | null> {
    try {
      const doc = await MongoDB.getCollection(task.extract.db, task.extract.collection).findOne({
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
