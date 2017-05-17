import { createHash } from 'crypto'
import { parse, format } from 'url'
import { MongoClient, Collection } from 'mongodb'
import { Client } from 'elasticsearch'
import { Config, ExtractTask, MongoDB, Elasticsearch } from './types'

let _mongodb: MongoDB
let _elasticsearch: Elasticsearch

function hash(obj: any): string {
  return createHash('md5').update(JSON.stringify(obj)).digest('hex')
}

export async function init(config: Config): Promise<void> {
  _mongodb = {}
  const url = parse(config.mongodb.url)
  url.pathname = `/local`
  _mongodb[hash('local')] = await MongoClient.connect(format(url), config.mongodb.options)
  for (let task of config.tasks) {
    const url = parse(config.mongodb.url)
    url.pathname = `/${task.extract.db}`
    _mongodb[hash(task.extract)] = await MongoClient.connect(format(url), config.mongodb.options)
  }
  _elasticsearch = new Client(config.elasticsearch.options)
}

export function mongodb(task: ExtractTask | 'local') {
  return _mongodb[hash(task)]
}

export function elasticsearch() {
  return _elasticsearch
}
