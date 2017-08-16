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
    this.phase = phase
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
  private static onSaveCallback: (name: string, checkPoint: CheckPoint) => Promise<void>
  private static onLoadCallback: (name: string) => Promise<any | null>

  constructor({ from, extract, transform, load }) {
    this.from = new CheckPoint(from)
    this.extract = extract
    this.transform = transform
    this.load = load
  }

  public name(): string {
    return `${this.extract.db}.${this.extract.collection}___${this.load.index}.${this.load.type}`
  }

  public async endScan(time: Date): Promise<void> {
    this.from.phase = 'tail'
    this.from.time = time
    delete this.from.id
    await Task.saveCheckpoint(this.name(), this.from)
  }

  public static onSaveCheckpoint(onSaveCallback: (name: string, checkPoint: CheckPoint) => Promise<void>) {
    Task.onSaveCallback = onSaveCallback
  }

  public static onLoadCheckpoint(onLoadCallback: (name: string) => Promise<any | null>) {
    Task.onLoadCallback = onLoadCallback
  }

  public static async saveCheckpoint(name: string, checkPoint: CheckPoint): Promise<void> {
    if (Task.onSaveCallback) {
      await Task.onSaveCallback(name, checkPoint)
    }
  }

  public static async loadCheckpoint(name: string): Promise<CheckPoint | null> {
    const obj = await Task.onLoadCallback(name)
    if (Task.onLoadCallback && obj && obj.phase) {
      return new CheckPoint(obj)
    }
    return null
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

export class Config {
  mongodb: MongoConfig
  elasticsearch: ElasticsearchConfig
  tasks: Task[]
  controls: Controls

  constructor(str: string) {
    const { mongodb, elasticsearch, tasks, controls } = JSON.parse(str)
    this.mongodb = new MongoConfig(mongodb)
    this.elasticsearch = new ElasticsearchConfig(elasticsearch)
    this.tasks = tasks.map(task => new Task(task))
    this.controls = new Controls(controls)
  }
}
