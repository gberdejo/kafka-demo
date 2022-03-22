const { Kafka } = require('kafkajs')
const express = require('express')
const morgan = require('morgan')
const cors = require('cors')


class App{
  producer
  kafka
  app
  constructor() {
    this.app = express()
    this.app.use(cors())
    this.app.use(morgan('dev'))
    this.app.use(express.json())    
    this.kafka = new Kafka({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
    })
    this.producer = this.kafka.producer()
    this.routes()
  }
  debugUrl = ({url,method}) => {
    console.log(`${url}:${method}`)
  }
  send = async (event,data) => {
    await this.producer.connect()
    await this.producer.send({
      topic: 'test-topic', messages: [{
        value: JSON.stringify({ event, data })
      }]
    })
    await this.producer.disconnect()
  }
  run(port) {
    this.app.listen(port, () => {
      console.log('Server Run!')
    })
  }
  routes() {
    this.app.post('/users', async(req,res) => {
      this.debugUrl(req)
      const { name, lastname } = req.body.data
      await this.send('@users',{name,lastname})
      res.status(200).json(req.body)
    })
    this.app.post('/tasks',async (req,res) => {
      this.debugUrl(req)
      const {name, description} = req.body.data
      await this.send('@tasks',{name,description})
      res.status(200).json(req.body)
    })
    
  }
}
const app = new App()
app.run(3000)


