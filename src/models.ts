import { parse, format } from 'url'
import { MongoClient, Collection } from 'mongodb'
import { Client } from 'elasticsearch'

import { Config, MongoDB, Elasticsearch } from './types'

let _mongodb: MongoDB
let _elasticsearch: Elasticsearch

export async function init(config: Config): Promise<void> {
  _mongodb = {}
  const url = parse(config.mongodb.url)
  url.pathname = `/local`
  _mongodb['local'] = await MongoClient.connect(format(url), config.mongodb.options)
  for (let task of config.tasks) {
    const url = parse(config.mongodb.url)
    url.pathname = `/${task.extract.db}`
    _mongodb[task.extract.db] = await MongoClient.connect(format(url), config.mongodb.options)
  }
  _elasticsearch = new Client(config.elasticsearch.options)
}

export function mongodb() {
  return _mongodb
}

export function elasticsearch() {
  return _elasticsearch
}
