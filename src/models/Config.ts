import { ObjectID, MongoClientOptions } from 'mongodb'
import { ConfigOptions, IndicesCreateParams, IndicesPutMappingParams } from 'elasticsearch'

export class MongoConfig {
  url: string
  options?: MongoClientOptions

  constructor({ url, options = {} }) {
    this.url = url
    this.options = options
  }
}

export class ElasticsearchConfig {
  options: ConfigOptions
  indices: IndicesCreateParams[]

  constructor({ options, indices = [] }) {
    this.options = options
    this.indices = indices
  }
}

export class CheckPoint {
  phase: 'scan' | 'tail'
  id: ObjectID
  time: Date

  constructor({ phase, id = 'FFFFFFFFFFFFFFFFFFFFFFFF', time = 0 }) {
    if (phase === 'scan') {
      this.id = new ObjectID(id)
      return
    }
    if (phase === 'tail') {
      this.time = new Date(time)
      return
    }
    throw new Error(`unknown check point phase: ${phase}`)
  }
}

export type ExtractTask = {
  db: string
  collection: string
  query: any
  projection: {
    [key: string]: 1 | 0
  }
}

export type TransformTask = {
  parent?: string
  mapping: {
    [key: string]: string
  }
}

export type LoadTask = IndicesPutMappingParams

export class Task {
  from: CheckPoint
  extract: ExtractTask
  transform: TransformTask
  load: LoadTask

  constructor({ from, extract, transform, load }) {
    this.from = new CheckPoint(from)
    this.extract = extract
    this.transform = transform
    this.load = load
  }

  public name(): string {
    return `${this.extract.db}.${this.extract.collection} -> ${this.load.index}.${this.load.type}`
  }
}

export class Controls {
  mongodbReadCapacity: number
  elasticsearchBulkSize: number
  indexNameSuffix: string

  constructor({ mongodbReadCapacity = Infinity, elasticsearchBulkSize = 5000, indexNameSuffix = '' }) {
    this.mongodbReadCapacity = mongodbReadCapacity
    this.elasticsearchBulkSize = elasticsearchBulkSize
    this.indexNameSuffix = indexNameSuffix
  }
}

export default class Config {
  mongodb: MongoConfig
  elasticsearch: ElasticsearchConfig
  tasks: Task[]
  controls: Controls

  public constructor(str: string) {
    const { mongodb, elasticsearch, tasks, controls } = JSON.parse(str)
    this.mongodb = new MongoConfig(mongodb)
    this.elasticsearch = new ElasticsearchConfig(elasticsearch)
    this.tasks = tasks.map(task => new Task(task))
    this.controls = new Controls(controls)
  }

  public dump(): string {
    return JSON.stringify({
      mongodb: this.mongodb,
      elasticsearch: this.elasticsearch,
      tasks: this.tasks,
      controls: this.controls,
    }, null, 2)
  }
}
