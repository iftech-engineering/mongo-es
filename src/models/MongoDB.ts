import { parse, format } from 'url'

import { Collection, Db, MongoClient } from 'mongodb'

import Config from './Config'

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
}
