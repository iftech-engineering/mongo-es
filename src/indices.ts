import {
  Client,
  IndicesCreateParams,
  IndicesPutMappingParams,
  IndicesExistsParams,
} from 'elasticsearch'

import { Config, ElasticsearchConfig, Task } from './config'

export default class Indices {
  static client: Client

  private constructor(elasticsearch: ElasticsearchConfig) {
    if (!Indices.client) {
      Indices.client = new Client({ ...elasticsearch.options })
    }
  }

  static async init(config: Config): Promise<void> {
    const indices = new Indices(config.elasticsearch)
    for (let index of config.elasticsearch.indices) {
      index.index += config.controls.indexNameSuffix
      if (!(await indices.exists(index))) {
        await indices.create(index)
        console.log('create index', index.index)
      }
    }
    for (let task of config.tasks) {
      task.load.index += config.controls.indexNameSuffix
      await indices.putMapping(task.load)
      console.log('put mapping', task.load.index, task.load.type)
    }
  }

  async create(params: IndicesCreateParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      Indices.client.indices.create(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async putMapping(params: IndicesPutMappingParams): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      Indices.client.indices.putMapping(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }

  async exists(params: IndicesExistsParams): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      Indices.client.indices.exists(params, (err, response) => {
        err ? reject(err) : resolve(response)
      })
    })
  }
}
