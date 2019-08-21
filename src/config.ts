import { ObjectID, MongoClientOptions } from 'mongodb'
import { ConfigOptions, IndicesCreateParams, IndicesPutMappingParams } from 'elasticsearch'
import { MongoDoc, ESDoc } from './types'

export type MongoConfigOptions = {
  url: string
  options?: MongoClientOptions
}
export class MongoConfig {
  url: string
  options?: MongoClientOptions

  constructor({ url, options = {} }: MongoConfigOptions) {
    this.url = url
    this.options = options
  }
}

export type ElasticsearchConfigOptions = {
  options: ConfigOptions
  indices: IndicesCreateParams[]
}

export class ElasticsearchConfig {
  options: ConfigOptions
  indices: IndicesCreateParams[]

  constructor({ options, indices = [] }: ElasticsearchConfigOptions) {
    this.options = options
    this.indices = indices
  }
}

export type CheckPointOptions = {
  phase: 'scan' | 'tail'
  id?: string
  time?: string | number | Date
}

export class CheckPoint {
  phase: 'scan' | 'tail'
  id: ObjectID
  time: Date

  constructor({ phase, id = '000000000000000000000000', time = Date.now() }: CheckPointOptions) {
    this.phase = phase
    if (phase === 'scan') {
      this.id = new ObjectID(id)
    }
    this.time = new Date(time)
  }
}

export type ExtractTask = {
  db: string
  collection: string
  projection: {
    [key: string]: 1 | 0
  }
}

export type TransformTask = {
  parent?: string
  mapping: ((doc: MongoDoc | ESDoc) => any) | { [key: string]: string }
  static?: {
    [key: string]: string
  }
}

export type LoadTask = IndicesPutMappingParams

export type TaskOptions = {
  from: CheckPointOptions
  extract: ExtractTask
  transform: TransformTask
  load: LoadTask
}

export class Task {
  from: CheckPoint
  extract: ExtractTask
  transform: TransformTask
  load: LoadTask
  static onSaveCallback: (name: string, checkPoint: CheckPoint) => Promise<void>
  static onLoadCallback: (name: string) => Promise<any | null>

  constructor({ from, extract, transform, load }: TaskOptions) {
    this.from = new CheckPoint(from)
    this.extract = extract
    this.transform = transform
    this.load = load
  }

  name(): string {
    return `${this.extract.db}.${this.extract.collection}___${this.load.index}.${this.load.type}`
  }

  async endScan(): Promise<void> {
    this.from.phase = 'tail'
    delete this.from.id
    await Task.saveCheckpoint(this.name(), this.from)
  }

  static onSaveCheckpoint(onSaveCallback: (name: string, checkPoint: CheckPoint) => Promise<void>) {
    Task.onSaveCallback = onSaveCallback
  }

  static onLoadCheckpoint(onLoadCallback: (name: string) => Promise<any | null>) {
    Task.onLoadCallback = onLoadCallback
  }

  static async saveCheckpoint(name: string, checkPoint: CheckPoint): Promise<void> {
    if (Task.onSaveCallback && Task.onSaveCallback instanceof Function) {
      try {
        await Task.onSaveCallback(name, checkPoint)
      } catch (err) {
        console.error('on save checkpoint', name, checkPoint, err)
      }
    }
  }

  static async loadCheckpoint(name: string): Promise<CheckPoint | null> {
    try {
      if (Task.onLoadCallback && Task.onLoadCallback instanceof Function) {
        const obj = await Task.onLoadCallback(name)
        if (obj && obj.phase) {
          return new CheckPoint(obj)
        }
      }
      return null
    } catch (err) {
      console.error('on load checkpoint', name, err)
      return null
    }
  }
}

export type ControlsOptions = {
  mongodbReadCapacity?: number
  elasticsearchBulkInterval?: number
  elasticsearchBulkSize?: number
  indexNameSuffix?: string
}

export class Controls {
  mongodbReadCapacity: number
  elasticsearchBulkInterval: number
  elasticsearchBulkSize: number
  indexNameSuffix: string

  constructor({
    mongodbReadCapacity = Infinity,
    elasticsearchBulkInterval = 5000,
    elasticsearchBulkSize = 5000,
    indexNameSuffix = '',
  }: ControlsOptions) {
    this.mongodbReadCapacity = mongodbReadCapacity
    this.elasticsearchBulkInterval = elasticsearchBulkInterval
    this.elasticsearchBulkSize = elasticsearchBulkSize
    this.indexNameSuffix = indexNameSuffix
  }
}

export type MongoEsConfig = {
  mongodb: MongoConfigOptions
  elasticsearch: ElasticsearchConfigOptions
  controls: ControlsOptions
  tasks: TaskOptions[]
}

export class Config {
  mongodb: MongoConfig
  elasticsearch: ElasticsearchConfig
  tasks: Task[]
  controls: Controls

  constructor(config: string | MongoEsConfig) {
    const { mongodb, elasticsearch, tasks, controls } =
      typeof config === 'string' ? (JSON.parse(config) as MongoEsConfig) : config

    this.mongodb = new MongoConfig(mongodb)
    this.elasticsearch = new ElasticsearchConfig(elasticsearch)
    this.tasks = tasks.map(task => new Task(task))
    this.controls = new Controls(controls)
  }
}
