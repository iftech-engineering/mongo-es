import { parse, format } from 'url'
import { MongoClient, Collection } from 'mongodb'
import { Client } from 'elasticsearch'
import { Config, Mongo, Elasticsearch } from './types'

let _mongo: Mongo
let _elasticsearch: Elasticsearch

export async function init(config: Config): Promise<void> {
  _mongo = {}
  const url = parse(config.mongo.url)
  url.pathname = `/local`
  _mongo['local'] = await MongoClient.connect(format(url), config.mongo.options)
  for (let task of config.tasks) {
    const url = parse(config.mongo.url)
    url.pathname = `/${task.extract.db}`
    _mongo[task.extract.db] = await MongoClient.connect(format(url), config.mongo.options)
  }
  _elasticsearch = new Client(config.elasticsearch.options)
}

export function mongo() {
  return _mongo
}

export function elasticsearch() {
  return _elasticsearch
}
