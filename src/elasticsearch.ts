import { Client, BulkIndexDocumentsParams } from 'elasticsearch'
import * as _ from 'lodash'

import { ESDoc } from './types'
import { ElasticsearchConfig, Task } from './config'

export default class Elasticsearch {
  static client: Client
  task: Task
  searchBuffer: { [id: string]: ((doc: ESDoc | null) => void)[] } = {}
  searchRunning: boolean = false
  retrieveBuffer: { [id: string]: ((doc: ESDoc | null) => void)[] } = {}
  retrieveRunning: boolean = false

  constructor(elasticsearch: ElasticsearchConfig, task: Task) {
    if (!Elasticsearch.client) {
      Elasticsearch.client = new Client({ ...elasticsearch.options })
    }
    this.task = task
  }

  async bulk(params: BulkIndexDocumentsParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      Elasticsearch.client.bulk(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async search(id: string): Promise<ESDoc | null> {
    return new Promise<ESDoc | null>(resolve => {
      this.searchBuffer[id] = this.searchBuffer[id] || []
      this.searchBuffer[id].push(resolve)
      if (!this.searchRunning) {
        this.searchRunning = true
        setTimeout(this._search.bind(this), 1000)
      }
    })
  }

  async _search(): Promise<void> {
    const ids = _.take(_.keys(this.searchBuffer), 1024)
    if (ids.length === 0) {
      this.searchRunning = false
      return
    }
    const docs = await this._searchBatchSafe(ids)
    ids.forEach(id => {
      const cbs = this.searchBuffer[id]
      delete this.searchBuffer[id]
      cbs.forEach(cb => {
        cb(docs[id] || null)
      })
    })
    setTimeout(this._search.bind(this), 1000)
  }

  async _searchBatchSafe(ids: string[]): Promise<{ [id: string]: ESDoc }> {
    return new Promise<{ [id: string]: ESDoc }>(resolve => {
      Elasticsearch.client.search<ESDoc>(
        {
          index: this.task.load.index,
          type: this.task.load.type,
          body: {
            query: {
              terms: {
                _id: ids,
              },
            },
          },
        },
        (err, response) => {
          try {
            if (err) {
              console.warn('search from elasticsearch', this.task.name(), ids, err.message)
              resolve({})
              return
            }
            console.debug('search from elasticsearch', response)
            const docs: ESDoc[] = response.hits.hits.map(this._mapResponse.bind(this))
            resolve(_.keyBy(docs, doc => doc._id))
          } catch (err2) {
            console.error('search from elasticsearch', this.task.name(), ids, err2)
            resolve({})
          }
        },
      )
    })
  }

  async retrieve(id: string): Promise<ESDoc | null> {
    return new Promise<ESDoc | null>(resolve => {
      this.retrieveBuffer[id] = this.retrieveBuffer[id] || []
      this.retrieveBuffer[id].push(resolve)
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
    ids.forEach(id => {
      const cbs = this.retrieveBuffer[id]
      delete this.retrieveBuffer[id]
      cbs.forEach(cb => {
        cb(docs[id] || null)
      })
    })
    setTimeout(this._retrieve.bind(this), 1000)
  }

  async _retrieveBatchSafe(ids: string[]): Promise<{ [id: string]: ESDoc }> {
    return new Promise<{ [id: string]: ESDoc }>(resolve => {
      Elasticsearch.client.mget<ESDoc>(
        {
          index: this.task.load.index as string,
          type: this.task.load.type,
          body: {
            ids,
          },
        },
        (err, response) => {
          try {
            if (err || !response.docs) {
              console.warn('retrieve from elasticsearch', this.task.name(), ids, err.message)
              resolve({})
              return
            }
            console.debug('retrieve from elasticsearch', response)
            const docs: ESDoc[] = response.docs.map(this._mapResponse.bind(this))
            resolve(_.keyBy(docs, doc => doc._id))
          } catch (err2) {
            console.error('retrieve from elasticsearch', this.task.name(), ids, err2)
            resolve({})
          }
        },
      )
    })
  }

  _mapResponse(hit: { _id: string; _parent?: string; _source: ESDoc }): ESDoc {
    const doc = hit._source || {}
    doc._id = hit._id
    if (this.task.transform.parent && hit._parent) {
      _.set(doc, this.task.transform.parent, hit._parent)
    }
    return doc
  }
}
