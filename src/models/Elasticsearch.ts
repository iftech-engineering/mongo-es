import {
  Client,
  IndicesCreateParams,
  IndicesPutMappingParams,
  IndicesExistsParams,
  BulkIndexDocumentsParams,
} from 'elasticsearch'

import { ObjectID, Document } from '../types'
import Config, { Task } from './Config'

export default class Elasticsearch {
  private static client: Client

  private constructor() {
  }

  public static async init({ elasticsearch }: Config): Promise<void> {
    if (Elasticsearch.client) {
      return
    }
    Elasticsearch.client = new Client(elasticsearch.options)
  }

  public static async create(params: IndicesCreateParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      Elasticsearch.client.indices.create(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  public static async putMapping(params: IndicesPutMappingParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      Elasticsearch.client.indices.putMapping(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  public static async exists(params: IndicesExistsParams): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      Elasticsearch.client.indices.exists(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  public static async bulk(params: BulkIndexDocumentsParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      Elasticsearch.client.bulk(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  public static async search(task: Task, id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      Elasticsearch.client.search<Document>({
        index: task.load.index,
        type: task.load.type,
        body: {
          query: {
            term: {
              _id: id.toHexString(),
            },
          },
        },
      }, (err, response: any) => {
        if (err) {
          console.warn('search from elasticsearch', task.name(), id, err.message)
          resolve(null)
          return
        }
        if (response.hits.total === 0) {
          console.warn('search from elasticsearch', task.name(), id, 'not found')
          resolve(null)
          return
        }
        console.debug('search from elasticsearch', response)
        const doc = response.hits.hits[0]._source
        doc._id = new ObjectID(response.hits.hits[0]._id)
        if (task.transform.parent && response.hits.hits[0]._parent) {
          doc[task.transform.parent] = new ObjectID(response.hits.hits[0]._parent)
        }
        resolve(doc)
      })
    })
  }

  public static async retrieve(task: Task, id: ObjectID): Promise<Document | null> {
    return new Promise<Document | null>((resolve) => {
      Elasticsearch.client.get<Document>({
        index: task.load.index as string,
        type: task.load.type,
        id: id.toHexString(),
      }, (err, response) => {
        if (err) {
          console.warn('retrieve from elasticsearch', task.name(), id, err.message)
          resolve(null)
          return
        }
        console.debug('retrieve from elasticsearch', response)
        resolve(response ? {
          ...response._source,
          _id: new ObjectID(response._id),
        } : null)
      })
    })
  }
}
