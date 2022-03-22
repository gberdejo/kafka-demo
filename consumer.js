const { Kafka } = require('kafkajs')
var mitt = require('mitt')
const express = require('express')
const morgan = require('morgan')
const cors = require('cors')

class App {
  producer
  consumer
  kafka
  constructor(port) {
    this.port = port
    this.app = express()
    this.app.use(cors())
    this.app.use(morgan('dev'))
    this.app.use(express.json())
    this.suscribeEmitter()
    this.suscribeComsumer()
  }
  async suscribeComsumer() {
    console.log('Se suscribio al consumer: test-group')
    this.kafka = new Kafka({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    })
    this.consumer = this.kafka.consumer({ groupId: 'test-group' })
    await this.consumer.connect()
    await this.consumer.subscribe({ topic: 'test-topic', fromBeginning: true })
    await this.consumer.run({eachMessage: this.eachMessage})
  }
  eachMessage = async ({ topic, partition, message }) => {
    console.log(`topic:${topic},partition:${partition}`)
    console.log(message.value.toString())
    const {event,data} = JSON.parse(message.value.toString())
    this.emitter.emit(event,data)
  }
  suscribeEmitter() {
    console.log('Se suscribio al emitter')
    this.emitter = mitt()
    this.emitters()
  }
  emitters() {
    this.emitter.on('@users',({name,lastname})=>console.log(`Mi nombre: ${name}`))
    this.emitter.on('@tasks',({name,description})=>console.log(`Mi Tasks: ${name} and description: ${description}`))
  }
  listen() {
    this.app.listen(this.port, () => {
      console.log('Server Run!: '+this.port)
    })
  }
  
}
const app = new App(4000)
app.listen()

